/*
 * The contents of this file are subject to the AOLserver Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://aolserver.com/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is AOLserver Code and related documentation
 * distributed by AOL.
 * 
 * The Initial Developer of the Original Code is America Online,
 * Inc. Portions created by AOL are Copyright (C) 1999 America Online,
 * Inc. All Rights Reserved.
 *
 * Alternatively, the contents of this file may be used under the terms
 * of the GNU General Public License (the "GPL"), in which case the
 * provisions of GPL are applicable instead of those above.  If you wish
 * to allow use of your version of this file only under the terms of the
 * GPL and not to allow others to use your version of this file under the
 * License, indicate your decision by deleting the provisions above and
 * replace them with the notice and other provisions required by the GPL.
 * If you do not delete the provisions above, a recipient may use your
 * version of this file under either the License or the GPL.
 */


/*
 * init.c --
 *
 *      This file contains routines for creating and accessing
 *      pools of database handles.
 */

#include "dbi.h"

NS_RCSID("@(#) $Header$");


/*
 * Local functions defined in this file
 */

static void
ReturnHandle(Handle * handle)
    NS_GNUC_NONNULL(1);

static int
CloseIfStale(Handle *, time_t now)
    NS_GNUC_NONNULL(1);

static int
Connect(Handle *)
    NS_GNUC_NONNULL(1);

static void
CheckPool(Pool *poolPtr, int stale);

static Ns_Callback     ScheduledPoolCheck;
static Ns_ArgProc      PoolCheckArgProc;

/*
 * Static variables defined in this file
 */

static Tcl_HashTable  serversTable;
static Ns_Cls         handleCls; /* Cache a single handle for current socket conn. */



/*
 *----------------------------------------------------------------------
 *
 * Dbi_RegisterDriver --
 *
 *      Register dbi procs for a driver and create the configured pools.
 *
 * Results:
 *      NS_OK if all goes well, NS_ERROR otherwise.
 *
 * Side effects:
 *      Many...
 *
 *----------------------------------------------------------------------
 */

int
Dbi_RegisterDriver(CONST char *server, CONST char *module, Dbi_Driver *driver)
{
    ServerData    *sdataPtr;
    Pool          *poolPtr;
    Handle        *handlePtr;
    Tcl_HashEntry *hPtr;
    Ns_Set        *pools;
    char          *path, *poolname, *description, *defaultPool;
    int            i, j, new;
    static int     once = 0;

    if (!once) {
        once = 1;
        DbiInitTclObjTypes();
        Ns_ClsAlloc(&handleCls, (Ns_Callback *) Dbi_PutHandle);
        Tcl_InitHashTable(&serversTable, TCL_STRING_KEYS);
        Ns_RegisterProcInfo(ScheduledPoolCheck, "nsdbi:check", PoolCheckArgProc);
    }

    /*
     * Create a server structure to track this virtual server's pools.
     */

    if (server == NULL) {
        Ns_Log(Error, "nsdbi: driver must be loaded into a virtual server");
        return NS_ERROR;
    }

    hPtr = Tcl_CreateHashEntry(&serversTable, server, &new);
    if (new) {
        sdataPtr = ns_calloc(1, sizeof(ServerData));
        sdataPtr->server = server;
        Tcl_InitHashTable(&sdataPtr->poolsTable, TCL_STRING_KEYS);
        Tcl_SetHashValue(hPtr, sdataPtr);
        if (Ns_TclRegisterTrace(server, DbiInitInterp, server,
                                NS_TCL_TRACE_CREATE) != NS_OK) {
            Ns_Log(Error, "nsdbi: error register tcl commands "
                   "for server '%s'", server);
            return NS_ERROR;
        }
    }
    sdataPtr = Tcl_GetHashValue(hPtr);

    /*
     * Configure all the pools for this driver.
     */

    path = Ns_ConfigGetPath(server, module, "pools", NULL);
    if (path == NULL) {
        Ns_Log(Error, "nsdbi: no database pools configured for module '%s', server '%s'",
               module, server);
        return NS_ERROR;
    }
    pools = Ns_ConfigGetSection(path);

    for (i = 0; pools != NULL && i < Ns_SetSize(pools); ++i) {

        poolname = Ns_SetKey(pools, i);
        description = Ns_SetValue(pools, i);
        hPtr = Tcl_CreateHashEntry(&sdataPtr->poolsTable, poolname, &new);
        if (!new) {
            Ns_Log(Error, "nsdbi: duplicate pool '%s' for server '%s'",
                   poolname, server);
            return NS_ERROR;
        }

        /*
         * Configure a single pool.
         */

        poolPtr = ns_calloc(1, sizeof(Pool));
        Ns_MutexInit(&poolPtr->lock);
        Ns_MutexSetName2(&poolPtr->lock, "nsdbi", poolname);
        Ns_CondInit(&poolPtr->getCond);
        Tcl_SetHashValue(hPtr, poolPtr);

        path = Ns_ConfigGetPath(server, module, "pool", poolname, NULL);
        if (path == NULL) {
            Ns_Log(Error, "nsdbi: no configuration for pool '%s', module '%s'",
                   poolname, module);
            return NS_ERROR;
        }

        poolPtr->sdataPtr = sdataPtr;
        poolPtr->driver = driver;
        poolPtr->name = poolname;
        poolPtr->description = description;
        poolPtr->datasource = Ns_ConfigGetValue(path, "datasource");
        poolPtr->user = Ns_ConfigGetValue(path, "user");
        poolPtr->password = Ns_ConfigGetValue(path, "password");
        poolPtr->fVerbose = Ns_ConfigBool(path, "verbose", 0);
        poolPtr->fVerboseError = Ns_ConfigBool(path, "logsqlerrors", 0);
        poolPtr->nhandles = Ns_ConfigIntRange(path, "handles", 2, 1, INT_MAX);
        poolPtr->maxwait = Ns_ConfigIntRange(path, "maxwait", 10, 0, INT_MAX);
        poolPtr->maxidle = Ns_ConfigIntRange(path, "maxidle", 0, 0, INT_MAX);
        poolPtr->maxopen = Ns_ConfigIntRange(path, "maxopen", 0, 0, INT_MAX);
        poolPtr->maxqueries = Ns_ConfigIntRange(path, "maxqueries", 0, 0, INT_MAX);

        if (poolPtr->maxidle || poolPtr->maxopen) {
            Ns_ScheduleProc(ScheduledPoolCheck, poolPtr, 0,
                Ns_ConfigIntRange(path, "checkinterval", 600, 30, INT_MAX));
        }

        /*
         * Fill the pool with handles.
         */

        poolPtr->firstPtr = poolPtr->lastPtr = NULL;
        handlePtr = ns_calloc(poolPtr->nhandles, sizeof(Handle));
        for (j = 0; j < poolPtr->nhandles; j++, handlePtr++) {
            handlePtr->poolPtr = poolPtr;
            Ns_DStringInit(&handlePtr->dsExceptionMsg);
            ReturnHandle(handlePtr);
        }

        /*
         * Is this the default pool for this virtual server?
         */

        path = Ns_ConfigGetPath(server, NULL, "dbi", NULL);
        defaultPool = Ns_ConfigGetValue(path, "defaultpool");
        if (STREQ(poolname, defaultPool)) {
            sdataPtr->defpoolPtr = poolPtr;
        }
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetPool --
 *
 *      Return the Dbi_Pool structure for the given pool name, checking
 *      that the virtual server has been given access to it.
 *
 * Results:
 *      Pointer to Dbi_Pool structure or NULL if pool does not exist or
 *      access not allowed.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

Dbi_Pool *
Dbi_GetPool(CONST char *server, CONST char *poolname)
{
    ServerData *sdataPtr;
    Dbi_Pool   *pool;

    if ((sdataPtr = DbiGetServer(server)) == NULL) {
        Ns_Log(Error, "nsdbi: invalid server '%s' while getting pool '%s'", server, poolname);
        return NULL;
    }
    pool = DbiGetPool(sdataPtr, poolname);
    if (pool == NULL) {
        if (poolname == NULL) {
            Ns_Log(Error, "nsdbi: no default pool for server '%s'", server); 
        } else {
            Ns_Log(Error, "nsdbi: invalid pool '%s' for server '%s'", poolname, server);
        }
    }
    return pool;
}

Dbi_Pool *
DbiGetPool(ServerData *sdataPtr, CONST char *poolname)
{
   Tcl_HashEntry  *hPtr;
   Dbi_Pool       *pool;

   if (poolname == NULL) {
       pool = (Dbi_Pool *) sdataPtr->defpoolPtr;
   } else {
       hPtr = Tcl_FindHashEntry(&sdataPtr->poolsTable, poolname);
       pool = hPtr ? Tcl_GetHashValue(hPtr) : NULL;
   }
   return pool;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DefaultPool --
 *
 *      Return the default pool.
 *
 * Results:
 *      String name of default pool or NULL if no default is defined.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

Dbi_Pool *
Dbi_DefaultPool(CONST char *server)
{
    ServerData *sdataPtr;

    sdataPtr = DbiGetServer(server);
    return (sdataPtr ? (Dbi_Pool *) sdataPtr->defpoolPtr : NULL);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ListPools --
 *
 *      Append the list of all pools available to the virtual server
 *      to the supplied DString.
 *
 * Results:
 *      NS_ERROR if server doesn't exist, NS_OK otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ListPools(Ns_DString *ds, CONST char *server)
{
    ServerData     *sdataPtr;
    Pool           *poolPtr;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;

    if ((sdataPtr = DbiGetServer(server)) == NULL) {
        return NS_ERROR;
    }
    hPtr = Tcl_FirstHashEntry(&sdataPtr->poolsTable, &search);
    while (hPtr != NULL) {
        poolPtr = Tcl_GetHashValue(hPtr);
        Ns_DStringAppendElement(ds, poolPtr->name);
        hPtr = Tcl_NextHashEntry(&search);
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetHandle --
 *
 *      Get a single handle from a pool within the given number of
 *      seconds.
 *
 * Results:
 *      NS_OK/NS_TIMEOUT/NS_ERROR.
 *
 * Side effects:
 *      Database may be opened if needed.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_GetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *pool, Ns_Conn *conn, int wait)
{
    Pool       *poolPtr   = (Pool *) pool;
    Handle     *handlePtr;
    Dbi_Handle *handle;
    Ns_Time     timeout, *timePtr;
    int         status;

    /*
     * Check the conn for a cached handle first.
     */

    if (conn != NULL && (handle = Ns_ClsGet(&handleCls, conn)) != NULL) {
        if (handle->pool == pool) {
            *handlePtrPtr = handle;
            return NS_OK;
        }
        Dbi_PutHandle(handle);
    }

    /*
     * Wait until this thread can be the exclusive thread aquiring
     * handles, watching for timeout.
     */

    Ns_GetTime(&timeout);
    Ns_IncrTime(&timeout, wait >= 0 ? wait : poolPtr->maxwait, 0);
    timePtr = &timeout;

    status = NS_OK;
    Ns_MutexLock(&poolPtr->lock);
    poolPtr->stats.handlegets++;
    while (status == NS_OK && poolPtr->firstPtr == NULL) {
        status = Ns_CondTimedWait(&poolPtr->getCond, &poolPtr->lock, timePtr);
    }
    if (poolPtr->firstPtr != NULL) {
        handlePtr = poolPtr->firstPtr;
        poolPtr->firstPtr = handlePtr->nextPtr;
        handlePtr->nextPtr = NULL;
        if (poolPtr->lastPtr == handlePtr) {
            poolPtr->lastPtr = NULL;
        }
        poolPtr->npresent--;
        handlePtr->n = poolPtr->nhandles - poolPtr->npresent;
        handlePtr->stale_on_close = poolPtr->stale_on_close;
    } else {
        handlePtr = NULL;
        poolPtr->stats.handlemisses++;
    }
    Ns_MutexUnlock(&poolPtr->lock);

    /*
     * If we got a handle, make sure its connected, otherwise return it.
     */

    if (handlePtr != NULL && handlePtr->connected == NS_FALSE) {
        if (Connect(handlePtr) != NS_OK) {
            Ns_MutexLock(&poolPtr->lock);
            ReturnHandle(handlePtr);
            Ns_CondSignal(&poolPtr->getCond);
            Ns_MutexUnlock(&poolPtr->lock);
        }
    }
    if (handlePtr != NULL) {
        *handlePtrPtr = (Dbi_Handle *) handlePtr;
        if (conn != NULL) {
            handlePtr->conn = conn;
            Ns_ClsSet(&handleCls, conn, handlePtr);
        }
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PutHandle --
 *
 *      Cleanup the handle and return it to its pool.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Handle is flushed, reset, and possibly closed as required.
 *
 *----------------------------------------------------------------------
 */

void
Dbi_PutHandle(Dbi_Handle *handle)
{
    Handle  *handlePtr = (Handle *) handle;
    Pool    *poolPtr   = (Pool *) handlePtr->poolPtr;
    time_t   now;

    /*
     * Remove from connection cache.
     */

    if (handlePtr->conn != NULL) {
        Ns_ClsSet(&handleCls, handlePtr->conn, NULL);
        handlePtr->conn = NULL;
    }

    /*
     * Cleanup the handle.
     */

    Dbi_ResetHandle(handle);

    /*
     * Close the handle if it's stale, otherwise update
     * the last access time.
     */

    time(&now);
    if (!CloseIfStale(handlePtr, now)) {
        handlePtr->atime = now;
    }
    Ns_MutexLock(&poolPtr->lock);
    ReturnHandle(handlePtr);
    Ns_CondSignal(&poolPtr->getCond);
    Ns_MutexUnlock(&poolPtr->lock);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ReleaseConnHandles --
 *
 *      Return any cached handles for the given conn back to they're
 *      pools.
 *
 * Results:
 *      Number of cached handles returned.
 *
 * Side effects:
 *      See Dbi_PutHandle.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ReleaseConnHandles(Ns_Conn *conn)
{
    Dbi_Handle *handle;
    int         nhandles = 0;

    handle = Ns_ClsGet(&handleCls, conn);
    if (handle != NULL) {
        Dbi_PutHandle(handle);
        nhandles = 1;
    }
    return nhandles;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_BouncePool --
 *
 *      Close all handles in the pool.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      All handles in the pool are marked stale, but only the unused
 *      handles are disconnected immediately.  Active handles will be
 *      disconnected as they are returned to the pool.
 *
 *----------------------------------------------------------------------
 */

void
Dbi_BouncePool(Dbi_Pool *pool)
{
    Pool *poolPtr = (Pool *) pool;

    CheckPool(poolPtr, 1);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Stats --
 *
 *      Append a list of statistics to the given dstring.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

void
Dbi_Stats(Ns_DString *ds, Dbi_Pool *pool)
{
    Pool *poolPtr = (Pool *) pool;

    Ns_MutexLock(&poolPtr->lock);
    Ns_DStringPrintf(ds, "handlegets %d handlemisses %d "
                     "handleopens %d handlefailures %d queries %d "
                     "agedcloses %d idlecloses %d "
                     "oppscloses %d bounces %d",
                     poolPtr->stats.handlegets,  poolPtr->stats.handlemisses,
                     poolPtr->stats.handleopens, poolPtr->stats.handlefailures,
                     poolPtr->stats.queries,
                     poolPtr->stats.otimecloses, poolPtr->stats.atimecloses,
                     poolPtr->stats.querycloses, poolPtr->stale_on_close);
    Ns_MutexUnlock(&poolPtr->lock);
}


/*
 *----------------------------------------------------------------------
 *
 * DbiLogSql --
 *
 *      Log a SQL statement depending on the verbose state of the
 *      pool.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

void
DbiLogSql(Dbi_Statement *stmt)
{
    Statement *stmtPtr = (Statement *) stmt;

    if (stmt->pool->fVerbose) {
        Ns_Log(Notice, "nsdbi: pool: '%s' sql '%s'",
               stmt->pool->name, Ns_DStringValue(&stmtPtr->dsSql));
    }
}


/*
 *----------------------------------------------------------------------
 *
 * DbiGetServer --
 *
 *      Get per-server data.
 *
 * Results:
 *      Pointer to per-server data structure.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

ServerData *
DbiGetServer(CONST char *server)
{
    Tcl_HashEntry *hPtr;

    hPtr = Tcl_FindHashEntry(&serversTable, server);
    return hPtr ? Tcl_GetHashValue(hPtr) : NULL;
}


/*
 *----------------------------------------------------------------------
 *
 * ReturnHandle --
 *
 *      Return a handle to its pool. Connected handles are pushed on
 *      the front of the list, disconnected handles are appened to
 *      the end.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Note:  The pool lock must be held by the caller and this
 *      function does not signal a thread waiting for handles.
 *
 *----------------------------------------------------------------------
 */

static void
ReturnHandle(Handle *handlePtr)
{
    Pool *poolPtr = (Pool *) handlePtr->poolPtr;

    if (poolPtr->firstPtr == NULL) {
        poolPtr->firstPtr = poolPtr->lastPtr = handlePtr;
        handlePtr->nextPtr = NULL;
    } else if (handlePtr->connected) {
        handlePtr->nextPtr = poolPtr->firstPtr;
        poolPtr->firstPtr = handlePtr;
    } else {
        poolPtr->lastPtr->nextPtr = handlePtr;
        poolPtr->lastPtr = handlePtr;
        handlePtr->nextPtr = NULL;
    }
    poolPtr->npresent++;
}


/*
 *----------------------------------------------------------------------
 *
 * CloseIfStale --
 *
 *      If the handle is stale, close it.
 *
 * Results:
 *      NS_TRUE if connection closed, NS_FALSE otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
CloseIfStale(Handle *handlePtr, time_t now)
{
    Pool *poolPtr = handlePtr->poolPtr;

    char *reason  = NULL;

    if (handlePtr->connected) {
        if (poolPtr->stale_on_close > handlePtr->stale_on_close) {
            reason = "bounced";
        } else if (poolPtr->maxopen && (handlePtr->otime < (now - poolPtr->maxopen))) {
            reason = "aged";
        } else if (poolPtr->maxidle && (handlePtr->atime < (now - poolPtr->maxidle))) {
            reason = "idle";
            poolPtr->stats.atimecloses++;
        } else if (poolPtr->maxqueries && (handlePtr->stats.queries >= poolPtr->maxqueries)) {
            reason = "used";
            poolPtr->stats.querycloses++;
        }
        if (reason) {
            Ns_Log(Notice, "nsdbi: closing %s handle in pool '%s', %d queries",
                   reason, poolPtr->name, handlePtr->stats.queries);
            DbiClose((Dbi_Handle *) handlePtr);
            handlePtr->connected = NS_FALSE;
            handlePtr->arg = NULL;
            handlePtr->atime = handlePtr->otime = 0;
            poolPtr->stats.queries += handlePtr->stats.queries;
            handlePtr->stats.queries = 0;
        }
    }

    return reason ? NS_TRUE : NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * CheckPool --
 *
 *      Verify all handles currently in a pool are not stale.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Stale handles, if any, are disconnected.
 *
 *----------------------------------------------------------------------
 */

static void
CheckPool(Pool *poolPtr, int stale)
{
    Handle       *handlePtr, *nextPtr;
    Handle       *checkedPtr;
    time_t        now;

    time(&now);
    checkedPtr = NULL;

    /*
     * Grab the entire list of handles from the pool.
     */

    Ns_MutexLock(&poolPtr->lock);
    if (stale) {
        poolPtr->stale_on_close++;
    }
    handlePtr = poolPtr->firstPtr;
    poolPtr->firstPtr = poolPtr->lastPtr = NULL;
    Ns_MutexUnlock(&poolPtr->lock);

    /*
     * Run through the list of handles, closing any
     * which have gone stale, and then return them
     * all to the pool.
     */

    if (handlePtr != NULL) {
        while (handlePtr != NULL) {
            poolPtr->npresent--;
            nextPtr = handlePtr->nextPtr;
            CloseIfStale(handlePtr, now);
            handlePtr->nextPtr = checkedPtr;
            checkedPtr = handlePtr;
            handlePtr = nextPtr;
        }

        Ns_MutexLock(&poolPtr->lock);
        handlePtr = checkedPtr;
        while (handlePtr != NULL) {
            nextPtr = handlePtr->nextPtr;
            
            ReturnHandle(handlePtr);
            handlePtr = nextPtr;
        }
        Ns_CondSignal(&poolPtr->getCond);
        Ns_MutexUnlock(&poolPtr->lock);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * ScheduledPoolCheck, PoolCheckArgProc --
 *
 *      Periodically check a pool for stale handles.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
ScheduledPoolCheck(void *arg)
{
    Pool *poolPtr = arg;

    CheckPool(poolPtr, 0);
}

static void
PoolCheckArgProc(Tcl_DString *dsPtr, void *arg)
{
    Pool *poolPtr = arg;

    Tcl_DStringAppendElement(dsPtr, poolPtr->name);
}


/*
 *----------------------------------------------------------------------
 *
 * Connect --
 *
 *      Connect a handle by opening the database.
 *
 * Results:
 *      NS_OK if connect ok, NS_ERROR on failure.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Connect(Handle *handlePtr)
{
    Pool *poolPtr = handlePtr->poolPtr;
    int   status;

    poolPtr->stats.handleopens++;
    status = DbiOpen((Dbi_Handle *) handlePtr);
    if (status != NS_OK) {
        poolPtr->stats.handlefailures++;
        handlePtr->atime = handlePtr->otime = 0;
        Ns_Log(Error, "nsdbi: failed to open connection for handle in pool '%s'",
               poolPtr->name);
    } else {
        handlePtr->connected = NS_TRUE;
        handlePtr->atime = handlePtr->otime = time(NULL);
        Ns_Log(Notice, "nsdbi: opened handle %d/%d in pool '%s'",
               handlePtr->n, poolPtr->nhandles, poolPtr->name);
    }

    return status;
}
