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


/*
 * The following structure maintains per-server data.
 */

typedef struct ServData {
    Pool          *defpoolPtr;
    Tcl_HashTable  allowedTable;
} ServData;

/*
 * Local functions defined in this file
 */

static void         ReturnHandle(Handle * handle);
static int          IsStale(Handle *, time_t now);
static int          Connect(Handle *);
static Pool        *CreatePool(char *pool, char *path, char *driver);
static ServData    *GetServer(char *server);
static Ns_Callback  CheckPool;
static Ns_ArgProc   CheckArgProc;

/*
 * Static variables defined in this file
 */

static Tcl_HashTable serversTable;
static Tcl_HashTable poolsTable;



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
Dbi_GetPool(char *server, char *pool)
{
    Tcl_HashEntry *hPtr;
    Dbi_Pool      *poolPtr;
    ServData      *sdataPtr;

    hPtr = Tcl_FindHashEntry(&poolsTable, pool);
    if (hPtr == NULL) {
        return NULL;
    }
    poolPtr = Tcl_GetHashValue(hPtr);
    sdataPtr = GetServer(server);
    if (sdataPtr == NULL) {
        Ns_Log(Error, "nsdbi: invalid server '%s' while getting pool '%s'",
               server, pool);
        return NULL;
    }
    if (Tcl_FindHashEntry(&sdataPtr->allowedTable, (char *) poolPtr) == NULL) {
        Ns_Log(Warning, "nsdbi: pool '%s' not available to server '%s'",
               pool, server);
        return NULL;
    }

    return poolPtr;
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
Dbi_PoolDefault(char *server)
{
    ServData *sdataPtr = GetServer(server);

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

char *
Dbi_PoolDbType(Dbi_Pool *poolPtr)
{
    Dbi_Handle *handle;
    char       *dbtype;

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

char *
Dbi_PoolDriverName(Dbi_Pool *poolPtr)
{
    Dbi_Handle *handle;
    char       *name;

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
 *      NS_ERROR if server doesn't exist.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_PoolList(Ns_DString *ds, char *server)
{
    ServData       *sdataPtr = GetServer(server);
    Pool           *poolPtr;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;

    if (sdataPtr == NULL) {
        return NS_ERROR;
    }
    hPtr = Tcl_FirstHashEntry(&sdataPtr->allowedTable, &search);
    while (hPtr != NULL) {
        poolPtr = (Pool *) Tcl_GetHashKey(&sdataPtr->allowedTable, hPtr);
        Ns_DStringAppendElement(ds, poolPtr->name);
        hPtr = Tcl_NextHashEntry(&search);
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolPutHandle --
 *
 *      Cleanup and then return a handle to its pool.
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
    Pool   *poolPtr = (Pool *) handlePtr->poolPtr;
    time_t  now;

    /*
     * Cleanup the handle.
     */

    Dbi_Flush(handle);
    Dbi_ResetHandle(handle);

    Ns_DStringFree(&handlePtr->dsExceptionMsg);
    handlePtr->cExceptionCode[0] = '\0';

    /*
     * Close the handle if it's stale, otherwise update
     * the last access time.
     */

    time(&now);
    if (IsStale(handlePtr, now)) {
        DbiDisconnect(handle);
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
 * Dbi_PoolTimedGetHandle --
 *
 *      Return a single handle from a pool within the given number of
 *      seconds.
 *
 * Results:
 *      Pointer to Dbi_Handle or NULL on error or timeout.
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
     
    if (wait < 0) {
        timePtr = NULL;
    } else {
        Ns_GetTime(&timeout);
        Ns_IncrTime(&timeout, wait, 0);
        timePtr = &timeout;
    }
    status = NS_OK;
    Ns_MutexLock(&poolPtr->lock);
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
    }
    Ns_MutexUnlock(&poolPtr->lock);

    /*
     * If we got a handle, make sure its connected, otherwise return it.
     */

    if (handlePtr != NULL && handlePtr->connected == NS_FALSE) {
        status = Connect(handlePtr);
    }
    if (handlePtr != NULL && status != NS_OK) {
        Ns_MutexLock(&poolPtr->lock);
        ReturnHandle(handlePtr);
        Ns_CondSignal(&poolPtr->getCond);
        Ns_MutexUnlock(&poolPtr->lock);
    } else {
        *handlePtrPtr = (Dbi_Handle *) handlePtr;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolGetHandle --
 *
 *      Return a single handle from a pool.
 *
 * Results:
 *      Pointer to Dbi_Handle or NULL on error.
 *
 * Side effects:
 *      Database may be opened if needed.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_PoolGetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *poolPtr)
{
    return Dbi_PoolTimedGetHandle(handlePtrPtr, poolPtr, 0);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_BouncePool --
 *
 *      Close all handles in the pool.
 *
 * Results:
 *      NS_OK if pool was bounce, NS_ERROR otherwise.
 *
 * Side effects:
 *      Handles are all marked stale and then closed by CheckPool.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_BouncePool(Dbi_Pool *pool)
{
    Pool    *poolPtr = (Pool *) pool;
    Handle  *handlePtr;

    if (poolPtr == NULL) {
        return NS_ERROR;
    }
    Ns_MutexLock(&poolPtr->lock);
    poolPtr->stale_on_close++;
    handlePtr = poolPtr->firstPtr;
    while (handlePtr != NULL) {
        if (handlePtr->connected) {
            handlePtr->stale = 1;
        }
        handlePtr->stale_on_close = poolPtr->stale_on_close;
        handlePtr = handlePtr->nextPtr;
    }
    Ns_MutexUnlock(&poolPtr->lock);
    CheckPool(poolPtr);

    return NS_OK;
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
    Ns_RegisterProcInfo(CheckPool, "nsdbi:check", CheckArgProc);
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
DbiInitServer(char *server)
{
    Pool           *poolPtr;
    ServData       *sdataPtr;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;
    char           *path, *pool, *pools, *p;
    int             new;

    path = Ns_ConfigGetPath(server, NULL, "dbi", NULL);

    /*
     * Verify the default pool exists, if any.
     */

    sdataPtr = ns_malloc(sizeof(ServData));
    hPtr = Tcl_CreateHashEntry(&serversTable, server, &new);
    Tcl_SetHashValue(hPtr, sdataPtr);

    pool = Ns_ConfigGetValue(path, "defaultpool");
    if (pool != NULL) {
        hPtr = Tcl_FindHashEntry(&poolsTable, pool);
        if (hPtr == NULL) {
            Ns_Log(Error, "nsdbi: no such default pool '%s'", pool);
            sdataPtr->defpoolPtr = NULL;
        } else {
            sdataPtr->defpoolPtr = Tcl_GetHashValue(hPtr);
        }
    }

    /*
     * Construct the allowed list and call the server-specific init.
     */

    Tcl_InitHashTable(&sdataPtr->allowedTable, TCL_ONE_WORD_KEYS);
    pools = Ns_ConfigGetValue(path, "pools");

    if (pools != NULL && poolsTable.numEntries > 0) {
        if (STREQ(pools, "*")) {
            hPtr = Tcl_FirstHashEntry(&poolsTable, &search);
            while (hPtr != NULL) {
                poolPtr = Tcl_GetHashValue(hPtr);
                DbiDriverInit(server, poolPtr->driverPtr);
                Tcl_CreateHashEntry(&sdataPtr->allowedTable, (char *) poolPtr, &new);
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
                    DbiDriverInit(server, poolPtr->driverPtr);
                    Tcl_CreateHashEntry(&sdataPtr->allowedTable, (char *) poolPtr, &new);
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
 * DbiDisconnect --
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
DbiDisconnect(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    DbiClose(handle);
    handlePtr->connected = NS_FALSE;
    handlePtr->arg = NULL;
    handlePtr->atime = handlePtr->otime = 0;
    handlePtr->stale = NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiLogSql --
 *
 *      Log a SQL statement depending on the verbose state of the
 *      handle.
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
DbiLogSql(Dbi_Handle *handle, char *sql)
{
    Handle *handlePtr = (Handle *) handle;
    Pool *poolPtr = (Pool *) handlePtr->poolPtr;

    if (Ns_DStringLength(&handlePtr->dsExceptionMsg) > 0 && poolPtr->fVerboseError) {
        Ns_Log(Error, "nsdbi: error(%s,%s): '%s'",
               poolPtr->datasource, Ns_DStringValue(&handlePtr->dsExceptionMsg), sql);
    } else if (poolPtr->fVerbose) {
        Ns_Log(Notice, "nsdbi: sql(%s): '%s'", poolPtr->datasource, sql);
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
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
IsStale(Handle *handlePtr, time_t now)
{
    Pool   *poolPtr = (Pool *) handlePtr->poolPtr;
    time_t  minAccess, minOpen;


    if (handlePtr->connected) {
        minAccess = now - poolPtr->maxidle;
        minOpen = now - poolPtr->maxopen;
        if ((poolPtr->maxidle && (handlePtr->atime < minAccess))
            || (poolPtr->maxopen && (handlePtr->otime < minOpen))
            || (handlePtr->stale == NS_TRUE)
            || (poolPtr->stale_on_close > handlePtr->stale_on_close)) {

            if (poolPtr->fVerbose) {
                Ns_Log(Notice, "dbiinit: closing %s handle in pool '%s'",
                       handlePtr->atime < minAccess ? "idle" : "old",
                       poolPtr->name);
            }
            return NS_TRUE;
        }
    }

    return NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * CheckArgProc --
 *
 *      Ns_ArgProc callback for the pool checker.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Copies name of pool to given dstring.
 *
 *----------------------------------------------------------------------
 */

static void
CheckArgProc(Tcl_DString *dsPtr, void *arg)
{
    Pool *poolPtr = arg;

    Tcl_DStringAppendElement(dsPtr, poolPtr->name);
}


/*
 *----------------------------------------------------------------------
 *
 * CheckPool --
 *
 *      Verify all handles in a pool are not stale.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Stale handles, if any, are closed.
 *
 *----------------------------------------------------------------------
 */

static void
CheckPool(void *arg)
{
    Pool         *poolPtr = arg;
    Handle       *handlePtr, *nextPtr;
    Handle       *checkedPtr;
    time_t        now;

    time(&now);
    checkedPtr = NULL;

    /*
     * Grab the entire list of handles from the pool.
     */

    Ns_MutexLock(&poolPtr->lock);
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
            nextPtr = handlePtr->nextPtr;
            if (IsStale(handlePtr, now)) {
                DbiDisconnect((Dbi_Handle *) handlePtr);
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
CreatePool(char *pool, char *path, char *driver)
{
    Pool        *poolPtr;
    Handle      *handlePtr;
    Dbi_Driver  *driverPtr;
    int          i;
    char        *datasource;

    if (driver == NULL) {
        Ns_Log(Error, "nsdbi: no driver for pool '%s'", pool);
        return NULL;
    }
    driverPtr = DbiLoadDriver(driver);
    if (driverPtr == NULL) {
        return NULL;
    }
    datasource = Ns_ConfigGetValue(path, "datasource");
    if (datasource == NULL) {
        Ns_Log(Error, "nsdbi: missing datasource for pool '%s'", pool);
        return NULL;
    }
    poolPtr = ns_calloc((size_t) 1, sizeof(Pool));
    poolPtr->name        = pool;
    poolPtr->driverPtr   = driverPtr;
    poolPtr->datasource  = datasource;
    poolPtr->user        = Ns_ConfigGetValue(path, "user");
    poolPtr->password    = Ns_ConfigGetValue(path, "password");
    poolPtr->description = Ns_ConfigGetValue("ns/dbi/pools", pool);
    Ns_MutexInit(&poolPtr->lock);
    Ns_MutexSetName2(&poolPtr->lock, "nsdbi", pool);
    Ns_CondInit(&poolPtr->getCond);

    if (!Ns_ConfigGetBool(path, "cachehandles", &poolPtr->cache_handles)) {
        poolPtr->cache_handles = 1;
    }
    Ns_ConfigGetBool(path, "verbose", &poolPtr->fVerbose);
    Ns_ConfigGetBool(path, "logsqlerrors", &poolPtr->fVerboseError);
    if (!Ns_ConfigGetInt(path, "connections", &poolPtr->nhandles)
        || poolPtr->nhandles <= 0) {
        Ns_Log(Notice, "nsdbi: setting connections for '%s' to %d", pool, 2);
        poolPtr->nhandles = 2;
    }
    if (!Ns_ConfigGetInt(path, "maxidle", (int *) &poolPtr->maxidle)
        || poolPtr->maxidle < 0) {
        poolPtr->maxidle = 0;
    }
    if (!Ns_ConfigGetInt(path, "maxopen", (int *) &poolPtr->maxopen)
        || poolPtr->maxopen < 0) {
        poolPtr->maxopen = 0;
    }
    if (poolPtr->maxidle || poolPtr->maxopen) {
        if (!Ns_ConfigGetInt(path, "checkinterval", &i) || i < 0) {
            i = 600;        /* 10 minutes. */
        }
        Ns_Log(Notice, "nsdbi: checking pool '%s' every %d seconds", pool, i);
        Ns_ScheduleProc(CheckPool, poolPtr, 0, i);
    }
    poolPtr->firstPtr = poolPtr->lastPtr = NULL;
    handlePtr = ns_calloc((size_t) poolPtr->nhandles, sizeof(Handle));
    for (i = 0; i < poolPtr->nhandles; i++, handlePtr++) {
        handlePtr->poolPtr = (Dbi_Pool *) poolPtr;
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
    int status;

    status = DbiOpen((Dbi_Handle *) handlePtr);
    if (status != NS_OK) {
        handlePtr->connected = NS_FALSE;
        handlePtr->atime = handlePtr->otime = 0;
        handlePtr->stale = NS_FALSE;
    } else {
        handlePtr->connected = NS_TRUE;
        handlePtr->atime = handlePtr->otime = time(NULL);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * GetServer --
 *
 *      Get per-server data.
 *
 * Results:
 *      Pointer to per-server data.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static ServData *
GetServer(char *server)
{
    Tcl_HashEntry *hPtr;

    hPtr = Tcl_FindHashEntry(&serversTable, server);
    if (hPtr != NULL) {
        return Tcl_GetHashValue(hPtr);
    }
    return NULL;
}
