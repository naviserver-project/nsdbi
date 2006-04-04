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
IsStale(Handle *, time_t now)
     NS_GNUC_NONNULL(1);

static int
Connect(Handle *)
     NS_GNUC_NONNULL(1);

static void
Disconnect(Handle *)
     NS_GNUC_NONNULL(1);

static Pool *
CreatePool(char *poolname, char *path, char *drivername)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

static void
CheckPool(Pool *poolPtr, int stale);

static Ns_Callback  ScheduledPoolCheck;
static Ns_Callback  LogStats;
static Ns_ArgProc   PoolCheckArgProc;

/*
 * Static variables defined in this file
 */

static Tcl_HashTable  serversTable;
static Tcl_HashTable  poolsTable;
static CONST char    *reasons[] = {"?", "bounced", "aged", "idle", "used"};

/*
 * Handle disconnection reasons.
 */

#define DBI_CLOSE_STALE 1
#define DBI_CLOSE_OTIME 2
#define DBI_CLOSE_ATIME 3
#define DBI_CLOSE_OPPS  4



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
 * Dbi_PoolDefault --
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
Dbi_PoolDefault(CONST char *server)
{
    ServerData *sdataPtr;

    sdataPtr = DbiGetServer(server);
    return (sdataPtr ? (Dbi_Pool *) sdataPtr->defpoolPtr : NULL);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolDbType --
 *
 *      Return the string DB type of the driver for handles in the pool.
 *
 * Results:
 *      String type or null if handle not available.
 *
 * Side effects:
 *      Database handle is acquired and released.
 *
 *----------------------------------------------------------------------
 */

CONST char *
Dbi_PoolDbType(Dbi_Pool *poolPtr)
{
    Dbi_Handle  *handle;
    CONST char  *dbtype;

    if ((Dbi_PoolGetHandle(&handle, poolPtr)) != NS_OK) {
        return NULL;
    }
    dbtype = Dbi_DriverDbType(handle);
    Dbi_PoolPutHandle(handle);

    return dbtype;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolDriverName --
 *
 *      Return the string name of the driver.
 *
 * Results:
 *      String name or null if handle not available.
 *
 * Side effects:
 *      Database handle is acquired and released.
 *
 *----------------------------------------------------------------------
 */

CONST char *
Dbi_PoolDriverName(Dbi_Pool *poolPtr)
{
    Dbi_Handle  *handle;
    CONST char  *name;

    if ((Dbi_PoolGetHandle(&handle, poolPtr)) != NS_OK) {
        return NULL;
    }
    name = Dbi_DriverName(handle);
    Dbi_PoolPutHandle(handle);

    return name;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolList --
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
Dbi_PoolList(Ns_DString *ds, CONST char *server)
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
 * Dbi_PoolGetHandle --
 *
 *      Get a single handle from a pool, waiting at most maxwait
 *      seconds specified by pool.
 *
 * Results:
 *      NS_OK/NS_TIMEOUT/NS_ERROR.
 *
 * Side effects:
 *      See Dbi_TimedGetHandle.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_PoolGetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *poolPtr)
{
    return Dbi_PoolTimedGetHandle(handlePtrPtr, poolPtr, -1);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolTimedGetHandle --
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
Dbi_PoolTimedGetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *pool, int wait)
{
    Pool    *poolPtr = (Pool *) pool;
    Handle  *handlePtr = NULL;
    Ns_Time  timeout, *timePtr;
    int      status;

    /*
     * Wait until this thread can be the exclusive thread aquiring
     * handles, watching for timeout.
     */

    Ns_GetTime(&timeout);
    Ns_IncrTime(&timeout, wait >= 0 ? wait : poolPtr->maxwait, 0);
    timePtr = &timeout;

    status = NS_OK;
    Ns_MutexLock(&poolPtr->lock);
    poolPtr->stats.attempts++;
    while (status == NS_OK && poolPtr->firstPtr == NULL) {
        poolPtr->stats.attempts++;
        status = Ns_CondTimedWait(&poolPtr->getCond, &poolPtr->lock, timePtr);
    }
    if (poolPtr->firstPtr != NULL) {
        handlePtr = poolPtr->firstPtr;
        poolPtr->firstPtr = handlePtr->nextPtr;
        handlePtr->nextPtr = NULL;
        if (poolPtr->lastPtr == handlePtr) {
            poolPtr->lastPtr = NULL;
        }
        poolPtr->stats.successes++;
        poolPtr->npresent--;
        handlePtr->n = poolPtr->nhandles - poolPtr->npresent;
        handlePtr->stale_on_close = poolPtr->stale_on_close;
    } else {
        poolPtr->stats.misses++;
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
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolPutHandle --
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
Dbi_PoolPutHandle(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;
    Pool   *poolPtr   = (Pool *) handlePtr->poolPtr;
    time_t  now;

    /*
     * Cleanup the handle.
     */

    Dbi_ResetHandle(handle);

    /*
     * Close the handle if it's stale, otherwise update
     * the last access time.
     */

    time(&now);
    if (IsStale(handlePtr, now)) {
        Disconnect(handlePtr);
    } else {
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
 * Dbi_PoolStats --
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
Dbi_PoolStats(Ns_DString *ds, Dbi_Pool *pool)
{
    Pool *poolPtr = (Pool *) pool;

    Ns_MutexLock(&poolPtr->lock);
    Ns_DStringPrintf(ds, "attempts %d successes %d misses %d opps %d "
                     "agedcloses %d idlecloses %d oppscloses %d bounces %d",
                     poolPtr->stats.attempts,    poolPtr->stats.successes,
                     poolPtr->stats.misses,      poolPtr->stats.opps,
                     poolPtr->stats.otimecloses, poolPtr->stats.atimecloses,
                     poolPtr->stats.oppscloses,  poolPtr->stale_on_close);
    Ns_MutexUnlock(&poolPtr->lock);
}


/*
 *----------------------------------------------------------------------
 *
 * DbiInitPools --
 *
 *      Initialize the database pools at startup.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Pools may be created as configured.
 *
 *----------------------------------------------------------------------
 */

void
DbiInitPools(void)
{
    Tcl_HashEntry  *hPtr;
    Pool           *poolPtr;
    Ns_Set         *pools;
    char           *path, *pool, *driver;
    int             new, i;

    /*
     * Attempt to create each database pool.
     */

    Tcl_InitHashTable(&serversTable, TCL_STRING_KEYS);
    Tcl_InitHashTable(&poolsTable, TCL_STRING_KEYS);
    pools = Ns_ConfigGetSection("ns/dbi/pools");
    for (i = 0; pools != NULL && i < Ns_SetSize(pools); ++i) {
        pool = Ns_SetKey(pools, i);
        hPtr = Tcl_CreateHashEntry(&poolsTable, pool, &new);
        if (!new) {
            Ns_Log(Error, "nsdbi: duplicate pool '%s'", pool);
            continue;
        }
        path = Ns_ConfigGetPath(NULL, NULL, "dbi", "pool", pool, NULL);
        driver = Ns_ConfigGetValue(path, "driver");
        poolPtr = CreatePool(pool, path, driver);
        if (poolPtr == NULL) {
            Tcl_DeleteHashEntry(hPtr);
        } else {
            Tcl_SetHashValue(hPtr, poolPtr);
        }
    }
    Ns_RegisterAtShutdown(LogStats, NULL);
    Ns_RegisterProcInfo(LogStats, "nsdbi:shutdown", NULL);
    Ns_RegisterProcInfo(ScheduledPoolCheck, "nsdbi:check", PoolCheckArgProc);
}


/*
 *----------------------------------------------------------------------
 *
 * DbiInitServer --
 *
 *      Initialize a virtual server allowed and default options.
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
DbiInitServer(CONST char *server)
{
    Pool           *poolPtr;
    ServerData     *sdataPtr;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;
    char           *path, *pool, *pools, *p;
    int             new;

    path = Ns_ConfigGetPath(server, NULL, "dbi", NULL);

    /*
     * Verify the default pool exists, if any.
     */

    hPtr = Tcl_CreateHashEntry(&serversTable, server, &new);
    sdataPtr = ns_malloc(sizeof(ServerData));
    sdataPtr->server = server;
    Tcl_SetHashValue(hPtr, sdataPtr);

    pool = Ns_ConfigGetValue(path, "defaultpool");
    if (pool != NULL) {
        hPtr = Tcl_FindHashEntry(&poolsTable, pool);
        if (hPtr == NULL) {
            Ns_Log(Error, "nsdbi: no such pool for default '%s'", pool);
            sdataPtr->defpoolPtr = NULL;
        } else {
            sdataPtr->defpoolPtr = Tcl_GetHashValue(hPtr);
        }
    }

    /*
     * Construct per-server pool list and call the server-specific init.
     */

    Tcl_InitHashTable(&sdataPtr->poolsTable, TCL_STRING_KEYS);
    pools = Ns_ConfigGetValue(path, "pools");

    if (pools != NULL && poolsTable.numEntries > 0) {
        if (STREQ(pools, "*")) {
            hPtr = Tcl_FirstHashEntry(&poolsTable, &search);
            while (hPtr != NULL) {
                poolPtr = Tcl_GetHashValue(hPtr);
                DbiDriverInit(server, poolPtr->driver);
                hPtr = Tcl_CreateHashEntry(&sdataPtr->poolsTable, poolPtr->name, &new);
                Tcl_SetHashValue(hPtr, poolPtr);
                hPtr = Tcl_NextHashEntry(&search);
            }
        } else {
            p = pools;
            while (p != NULL && *p != '\0') {
                p = strchr(pools, ',');
                if (p != NULL) {
                    *p = '\0';
                }
                hPtr = Tcl_FindHashEntry(&poolsTable, pools);
                if (hPtr != NULL) {
                    poolPtr = Tcl_GetHashValue(hPtr);
                    DbiDriverInit(server, poolPtr->driver);
                    hPtr = Tcl_CreateHashEntry(&sdataPtr->poolsTable, poolPtr->name, &new);
                    Tcl_SetHashValue(hPtr, poolPtr);
                }
                if (p != NULL) {
                    *p++ = ',';
                }
                pools = p;
            }
        }
    }
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
 * ReturnHandle --
 *
 *      Return a handle to its pool.  Connected handles are pushed on
 *      the front of the list, disconnected handles are appened to
 *      the end.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Handle is returned to the pool.  Note:  The pool lock must be
 *      held by the caller and this function does not signal a thread
 *      waiting for handles.
 *
 *----------------------------------------------------------------------
 */

static void
ReturnHandle(Handle *handlePtr)
{
    Pool *poolPtr = (Pool *) handlePtr->poolPtr;

    if (!handlePtr->connected) {
        poolPtr->stats.opps += handlePtr->stats.opps;
        handlePtr->stats.opps = 0;
        switch (handlePtr->reason) {
        case DBI_CLOSE_OTIME:
            poolPtr->stats.otimecloses++;
            break;
        case DBI_CLOSE_ATIME:
            poolPtr->stats.atimecloses++;
            break;
        case DBI_CLOSE_OPPS:
            poolPtr->stats.oppscloses++;
            break;
        }
        handlePtr->reason = 0;
    }
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
 * IsStale --
 *
 *      Check to see if a handle is stale.
 *
 * Results:
 *      NS_TRUE if handle stale, NS_FALSE otherwise.
 *
 * Side effects:
 *      Staleness reason is updated.
 *
 *----------------------------------------------------------------------
 */

static int
IsStale(Handle *handlePtr, time_t now)
{
    Pool *poolPtr = handlePtr->poolPtr;

    if (handlePtr->connected) {
        if (poolPtr->stale_on_close > handlePtr->stale_on_close) {
            handlePtr->reason = DBI_CLOSE_STALE;
            return NS_TRUE;
        } else if (poolPtr->maxopen && (handlePtr->otime < (now - poolPtr->maxopen))) {
            handlePtr->reason = DBI_CLOSE_OTIME;
            return NS_TRUE;
        } else if (poolPtr->maxidle && (handlePtr->atime < (now - poolPtr->maxidle))) {
            handlePtr->reason = DBI_CLOSE_ATIME;
            return NS_TRUE;
        } else if (poolPtr->maxopps && (handlePtr->stats.opps >= poolPtr->maxopps)) {
            handlePtr->reason = DBI_CLOSE_OPPS;
            return NS_TRUE;
        }
    }
    return NS_FALSE;
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
            if (IsStale(handlePtr, now)) {
                Disconnect(handlePtr);
            }
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
 * CreatePool --
 *
 *      Create a new pool using the given driver.
 *
 * Results:
 *      Pointer to newly allocated Pool structure.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static Pool  *
CreatePool(char *poolname, char *path, char *drivername)
{
    Pool        *poolPtr;
    Handle      *handlePtr;
    Dbi_Driver  *driver;
    int          i;
    char        *datasource;

    if (drivername == NULL) {
        Ns_Log(Error, "nsdbi: no driver for pool '%s'", poolname);
        return NULL;
    }
    driver = DbiLoadDriver(drivername);
    if (driver == NULL) {
        return NULL;
    }
    datasource = Ns_ConfigGetValue(path, "datasource");
    if (datasource == NULL) {
        Ns_Log(Error, "nsdbi: missing datasource for pool '%s'", poolname);
        return NULL;
    }
    poolPtr = ns_calloc((size_t) 1, sizeof(Pool));
    poolPtr->name        = poolname;
    poolPtr->driver      = driver;
    poolPtr->datasource  = datasource;
    poolPtr->user        = Ns_ConfigGetValue(path, "user");
    poolPtr->password    = Ns_ConfigGetValue(path, "password");
    poolPtr->description = Ns_ConfigGetValue("ns/dbi/pools", poolname);
    Ns_MutexInit(&poolPtr->lock);
    Ns_MutexSetName2(&poolPtr->lock, "nsdbi", poolname);
    Ns_CondInit(&poolPtr->getCond);

    if (!Ns_ConfigGetBool(path, "cachehandles", &poolPtr->cache_handles)) {
        poolPtr->cache_handles = 1;
    }
    Ns_ConfigGetBool(path, "verbose", &poolPtr->fVerbose);
    Ns_ConfigGetBool(path, "logsqlerrors", &poolPtr->fVerboseError);
    if (!Ns_ConfigGetInt(path, "connections", &poolPtr->nhandles)
        || poolPtr->nhandles <= 0) {
        Ns_Log(Notice, "nsdbi: setting connections for '%s' to %d", poolname, 2);
        poolPtr->nhandles = 2;
    }
    if (!Ns_ConfigGetInt(path, "maxwait", (int *) &poolPtr->maxwait)
        || poolPtr->maxwait < 0) {
        poolPtr->maxwait = 10;
    }
    if (!Ns_ConfigGetInt(path, "maxidle", (int *) &poolPtr->maxidle)
        || poolPtr->maxidle < 0) {
        poolPtr->maxidle = 0;
    }
    if (!Ns_ConfigGetInt(path, "maxopen", (int *) &poolPtr->maxopen)
        || poolPtr->maxopen < 0) {
        poolPtr->maxopen = 0;
    }
    if (!Ns_ConfigGetInt(path, "maxopps", (int *) &poolPtr->maxopps)
        || poolPtr->maxopps < 0) {
        poolPtr->maxopps = 0;
    }
    if (poolPtr->maxidle || poolPtr->maxopen) {
        if (!Ns_ConfigGetInt(path, "checkinterval", &i) || i < 0) {
            i = 600;        /* 10 minutes. */
        }
        Ns_Log(Notice, "nsdbi: checking pool '%s' every %d seconds", poolname, i);
        Ns_ScheduleProc(ScheduledPoolCheck, poolPtr, 0, i);
    }
    poolPtr->firstPtr = poolPtr->lastPtr = NULL;
    handlePtr = ns_calloc((size_t) poolPtr->nhandles, sizeof(Handle));
    for (i = 0; i < poolPtr->nhandles; i++, handlePtr++) {
        handlePtr->poolPtr = poolPtr;
        Ns_DStringInit(&handlePtr->dsExceptionMsg);
        ReturnHandle(handlePtr);
    }

    return poolPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * Connect --
 *
 *      Connect a handle by opening the database.
 *
 * Results:
 *      NS_OK if connect ok, NS_ERROR otherwise.
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

    status = DbiOpen((Dbi_Handle *) handlePtr);
    if (status != NS_OK) {
        handlePtr->connected = NS_FALSE;
        handlePtr->atime = handlePtr->otime = 0;
    } else {
        handlePtr->connected = NS_TRUE;
        handlePtr->atime = handlePtr->otime = time(NULL);
        Ns_Log(Notice, "nsdbi: opened handle %d/%d in pool '%s'",
               handlePtr->n, poolPtr->nhandles, poolPtr->name);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Disconnect --
 *
 *      Disconnect a handle by closing the database if needed.
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
Disconnect(Handle *handlePtr)
{
    Pool *poolPtr = handlePtr->poolPtr;

    Ns_Log(Notice, "nsdbi: closing %s handle in pool '%s', %d opperations",
           reasons[handlePtr->reason], poolPtr->name, handlePtr->stats.opps);
    DbiClose((Dbi_Handle *) handlePtr);
    handlePtr->arg = NULL;
    handlePtr->atime = handlePtr->otime = 0;
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
 * LogStats --
 *
 *      Log the accumulated stats for each pool at server shutdown.
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
LogStats(void *arg)
{
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;
    Pool           *poolPtr;
    Ns_DString      ds;

    Ns_DStringInit(&ds);

    hPtr = Tcl_FirstHashEntry(&poolsTable, &search);
    while (hPtr != NULL) {
        poolPtr = Tcl_GetHashValue(hPtr);
        Dbi_PoolStats(&ds, (Dbi_Pool *) poolPtr);
        Ns_Log(Notice, "nsdbi: stats for pool '%s': %s",
               poolPtr->name, Ns_DStringValue(&ds));
        Ns_DStringTrunc(&ds, 0);
        hPtr = Tcl_NextHashEntry(&search);
    }
    Ns_DStringFree(&ds);
}
