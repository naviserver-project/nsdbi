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

#include "nsdbi.h"

NS_RCSID("@(#) $Header$");


extern Ns_TclInterpInitProc DbiInitInterp;


/*
 * The following structure tracks which pools are
 * available to a virtual server.
 */

typedef struct ServerData {
    CONST char        *server;
    Dbi_Pool          *defpoolPtr;
    Tcl_HashTable      poolsTable;
} ServerData;

/*
 * The following structure defines a pool of database handles.
 */

typedef struct Pool {

    Ns_Mutex           lock;
    Ns_Cond            getCond;

    struct Handle     *firstPtr;
    struct Handle     *lastPtr;

    char              *module;
    Dbi_Driver        *driver;

    int                nhandles;
    int                npresent;
    int                maxwait;
    time_t             maxidle;
    time_t             maxopen;
    int                maxqueries;
    int                cachesize;

    int                stale_on_close;
    int                stopping;

    struct {
        unsigned int   handlegets;
        unsigned int   handlemisses;
        unsigned int   handleopens;
        unsigned int   handlefailures;
        unsigned int   queries;
        unsigned int   otimecloses;
        unsigned int   atimecloses;
        unsigned int   querycloses;
    } stats;

} Pool;

/*
 * The following structure defines a handle in a database pool.
 */

typedef struct Handle {

    /* Publicly visible in a Dbi_Handle. */

    struct Pool      *poolPtr;      /* The pool this handle belongs to. */
    void             *arg;          /* Driver private handle context. */

    /* Private to a Handle. */

    struct Handle    *nextPtr;      /* Next handle in the pool. */
    Ns_Conn          *conn;         /* Conn that handle is cached for, if any. */
    char              cExceptionCode[6];
    Ns_DString        dsExceptionMsg;
    time_t            otime;        /* Time when handle was connected to db. */
    time_t            atime;        /* Time when handle was last used. */
    int               n;            /* Handle n of nhandles when acquired. */
    int               stale_on_close;
    int               reason;       /* Why the handle is being disconnected. */

    /* Result status. */

    int               fetchingRows; /* Is there a pending result set? */
    int               numCols;      /* Number of columns in penfing result. */
    int               numRows;      /* Number of rows in pending result. */
    int               currentCol;   /* The current column index. */
    int               currentRow;   /* The current row index. */

    unsigned int      stmtid;       /* Unique ID for statements. */
    Ns_Cache         *cache;        /* Cache of prepared Statement structs. */

    struct {
        unsigned int  queries;      /* Total queries via current connection. */
    } stats;

} Handle;

/*
 * The following structure defines a prepared statement kept
 * in a per-handle cache.
 */

typedef struct Statement {
    unsigned int      id;           /* Unique (per handle) statement ID. */
    unsigned int      nqueries;     /* Total queries for this statement. */
    void             *arg;          /* Statement context for driver. */

    Handle           *handlePtr;    /* Handle this Statement belongs to. */
    Tcl_HashEntry    *hPtr;         /* Entry in the id table. */

    Tcl_HashTable     bindTable;    /* Bind variables by name. */
    int               nbind;        /* Number of bind variables. */
    struct {
        CONST char   *name;         /* (Hash table key) */
    } vars[DBI_MAX_BIND];           /* Bind variables by index. */

    int               length;       /* Length of sql. */
    char              sql[1];  /* Driver specific SQL. */
} Statement;


/*
 * Local functions defined in this file
 */

#define DbiLog(handle,level,msg,...)                            \
    Ns_Log(level, "dbi[%s:%s]: " msg,                           \
           ((Handle *) handle)->poolPtr->driver->name,          \
           ((Handle *) handle)->poolPtr->module, __VA_ARGS__)

static ServerData *GetServer(CONST char *server);
static void ReturnHandle(Handle * handle) NS_GNUC_NONNULL(1);
static int CloseIfStale(Handle *, time_t now) NS_GNUC_NONNULL(1);
static int Connect(Handle *) NS_GNUC_NONNULL(1);
static int Connected(Handle *handlePtr) NS_GNUC_NONNULL(1);
static void CheckPool(Pool *poolPtr, int stale) NS_GNUC_NONNULL(1);
static Statement *ParseBindVars(Handle *handlePtr, CONST char *sql, int length);
static int DefineBindVar(Statement *stmtPtr, CONST char *name, Ns_DString *dsPtr);

static Ns_Callback FreeStatement;
static void CloseStatements(Ns_Cache *);

static Ns_Callback     ScheduledPoolCheck;
static Ns_ArgProc      PoolCheckArgProc;
static Ns_ShutdownProc AtShutdown;

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
Dbi_RegisterDriver(CONST char *server, CONST char *module,
                   Dbi_Driver *driver, int size)
{
    ServerData      *sdataPtr;
    Pool            *poolPtr;
    Handle          *handlePtr;
    Tcl_HashEntry   *hPtr;
    Tcl_HashSearch   search;
    Ns_Set          *set;
    char            *path;
    int              i, new, isdefault;
    static int       once = 0;

    /*
     * Ensure all callbacks are present.
     */

    if (sizeof(Dbi_Driver) < size || size <= 0) {
        Ns_Log(Error, "dbi: Dbi_RegisterDriver: driver structure mismatch");
        return NS_ERROR;
    }
    for (i = sizeof(Dbi_Driver) -1; i > 1; i--) {
        if (driver + i == NULL) {
            Ns_Log(Error, "dbi: Dbi_RegisterDriver: driver function missing");
            return NS_ERROR;
        }
    }

    /*
     * Initialise the nsdbi library. Gather list of servers now for
     * the benfit of global modules.
     */

    if (!once) {
        once = 1;

        Ns_ClsAlloc(&handleCls, (Ns_Callback *) Dbi_PutHandle);
        Ns_RegisterProcInfo(ScheduledPoolCheck, "dbi:check", PoolCheckArgProc);

        Tcl_InitHashTable(&serversTable, TCL_STRING_KEYS);

        set = Ns_ConfigGetSection("ns/servers");
        for (i = 0; i < Ns_SetSize(set); i++) {
            sdataPtr = ns_calloc(1, sizeof(ServerData));
            sdataPtr->server = Ns_SetKey(set, i);
            Tcl_InitHashTable(&sdataPtr->poolsTable, TCL_STRING_KEYS);
            hPtr = Tcl_CreateHashEntry(&serversTable, sdataPtr->server, &new);
            Tcl_SetHashValue(hPtr, sdataPtr);
            if (Ns_TclRegisterTrace(sdataPtr->server, DbiInitInterp, sdataPtr->server,
                                    NS_TCL_TRACE_CREATE) != NS_OK) {
                Ns_Log(Error, "dbi: error register tcl commands for server '%s'",
                       sdataPtr->server);
                return NS_ERROR;
            }
        }
    }

    /*
     * Configure this pool.
     */

    path = Ns_ConfigGetPath(server, module, NULL);
    if (path == NULL) {
        Ns_Log(Error, "dbi[%s]: no configuration for pool", module);
        return NS_ERROR;
    }

    poolPtr = ns_calloc(1, sizeof(Pool));
    Ns_MutexSetName2(&poolPtr->lock, "dbi", module);
    Ns_CondInit(&poolPtr->getCond);
    poolPtr->driver = driver;
    poolPtr->module = ns_strdup(module);
    poolPtr->nhandles   = Ns_ConfigIntRange(path, "handles",    2,          1, INT_MAX);
    poolPtr->maxwait    = Ns_ConfigIntRange(path, "maxwait",    10,         0, INT_MAX);
    poolPtr->maxidle    = Ns_ConfigIntRange(path, "maxidle",    0,          0, INT_MAX);
    poolPtr->maxopen    = Ns_ConfigIntRange(path, "maxopen",    0,          0, INT_MAX);
    poolPtr->maxqueries = Ns_ConfigIntRange(path, "maxqueries", 0,          0, INT_MAX);
    poolPtr->cachesize  = Ns_ConfigIntRange(path, "cachesize",  1024*1024,  0, INT_MAX);
    if (poolPtr->maxidle || poolPtr->maxopen) {
        Ns_ScheduleProc(ScheduledPoolCheck, poolPtr, 0,
                        Ns_ConfigIntRange(path, "checkinterval", 600, 30, INT_MAX));
    }
    Ns_RegisterAtShutdown(AtShutdown, poolPtr);

    /*
     * Fill the pool with handles.
     */

    handlePtr = ns_calloc(poolPtr->nhandles, sizeof(Handle));
    for (i = 0; i < poolPtr->nhandles; i++, handlePtr++) {
        handlePtr->poolPtr = poolPtr;
        Ns_DStringInit(&handlePtr->dsExceptionMsg);
        handlePtr->cache = Ns_CacheCreateSz(module, TCL_STRING_KEYS,
                                            poolPtr->cachesize, FreeStatement);
        ReturnHandle(handlePtr);
    }

    /*
     * Map pool to all virtual servers if module is global.
     */

    isdefault = Ns_ConfigBool(path, "default", 0);

    if (server != NULL) {
        hPtr = Tcl_FindHashEntry(&serversTable, server);
        assert(hPtr != NULL);
        sdataPtr = Tcl_GetHashValue(hPtr);
        hPtr = Tcl_CreateHashEntry(&sdataPtr->poolsTable, module, &new);
        Tcl_SetHashValue(hPtr, poolPtr);
        if (isdefault) {
            sdataPtr->defpoolPtr = (Dbi_Pool *) poolPtr;
        }
    } else {
        hPtr = Tcl_FirstHashEntry(&serversTable, &search);
        while (hPtr != NULL) {
            sdataPtr = Tcl_GetHashValue(hPtr);
            hPtr = Tcl_CreateHashEntry(&sdataPtr->poolsTable, module, &new);
            Tcl_SetHashValue(hPtr, poolPtr);
            if (isdefault) {
                sdataPtr->defpoolPtr = (Dbi_Pool *) poolPtr;
            }
            hPtr = Tcl_NextHashEntry(&search);
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
    ServerData     *sdataPtr;
    Tcl_HashEntry  *hPtr;
    Dbi_Pool       *pool;

    if ((sdataPtr = GetServer(server)) == NULL) {
        Ns_Log(Error, "dbi: invalid server '%s' while getting pool '%s'", server, poolname);
        return NULL;
    }
    if (poolname == NULL) {
        pool = sdataPtr->defpoolPtr;
    } else {
        hPtr = Tcl_FindHashEntry(&sdataPtr->poolsTable, poolname);
        pool = hPtr ? Tcl_GetHashValue(hPtr) : NULL;
    }
    if (pool == NULL) {
        if (poolname == NULL) {
            Ns_Log(Error, "dbi: no default pool for server '%s'", server); 
        } else {
            Ns_Log(Error, "dbi: invalid pool '%s' for server '%s'", poolname, server);
        }
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

    sdataPtr = GetServer(server);
    return (sdataPtr ? sdataPtr->defpoolPtr : NULL);
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

    if ((sdataPtr = GetServer(server)) == NULL) {
        return NS_ERROR;
    }
    hPtr = Tcl_FirstHashEntry(&sdataPtr->poolsTable, &search);
    while (hPtr != NULL) {
        poolPtr = Tcl_GetHashValue(hPtr);
        Ns_DStringAppendElement(ds, poolPtr->module);
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
 *      NS_OK / NS_TIMEOUT / NS_ERROR.
 *
 * Side effects:
 *      Database may be opened if needed.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_GetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *pool, Ns_Conn *conn,
              Ns_Time *timeoutPtr)
{
    Pool       *poolPtr = (Pool *) pool;
    Handle     *handlePtr;
    Dbi_Handle *handle;
    Ns_Time     time;
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

    if (timeoutPtr == NULL) {
        Ns_GetTime(&time);
        Ns_IncrTime(&time, poolPtr->maxwait, 0);
        timeoutPtr = &time;
    }

    status = NS_OK;
    Ns_MutexLock(&poolPtr->lock);
    poolPtr->stats.handlegets++;
    while (status == NS_OK
           && !poolPtr->stopping
           && poolPtr->firstPtr == NULL) {
        status = Ns_CondTimedWait(&poolPtr->getCond, &poolPtr->lock, timeoutPtr);
    }
    if (poolPtr->stopping) {
        status = NS_ERROR;
        handlePtr = NULL;
    } else if (poolPtr->firstPtr == NULL) {
        handlePtr = NULL;
        poolPtr->stats.handlemisses++;
    } else {
        handlePtr = poolPtr->firstPtr;
        poolPtr->firstPtr = handlePtr->nextPtr;
        handlePtr->nextPtr = NULL;
        if (poolPtr->lastPtr == handlePtr) {
            poolPtr->lastPtr = NULL;
        }
        poolPtr->npresent--;
        handlePtr->n = poolPtr->nhandles - poolPtr->npresent;
        handlePtr->stale_on_close = poolPtr->stale_on_close;
    }
    Ns_MutexUnlock(&poolPtr->lock);

    /*
     * If we got a handle, make sure its connected, otherwise return it.
     */

    if (handlePtr != NULL && !Connected(handlePtr)) {
        if (Connect(handlePtr) != NS_OK) {
            Ns_MutexLock(&poolPtr->lock);
            ReturnHandle(handlePtr);
            Ns_CondSignal(&poolPtr->getCond);
            Ns_MutexUnlock(&poolPtr->lock);
            status = NS_ERROR;
        }
    }

    /*
     * Return the handle and cache for the current connection.
     */

    if (status == NS_OK) {
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
 *      Handle is reset and possibly closed as required.
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

    Dbi_Reset(handle);

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
 *      Return any cached handles for the given conn back to their
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
 * Dbi_Prepare --
 *
 *      Parse the sql for bind variables.
 *
 * Results:
 *      NS_ERROR if max bind variables exceeded.
 *
 * Side effects:
 *      Statement is parsed by driver callback and any bind variables
 *      are converted to driver specific notation.
 *
 *      Statement is cached for handle.
 *
 *----------------------------------------------------------------------
 */

Dbi_Statement *
Dbi_Prepare(Dbi_Handle *handle, CONST char *sql, int length)
{
    Handle          *handlePtr = (Handle *) handle;
    Dbi_Driver      *driver    = handlePtr->poolPtr->driver;
    Statement       *stmtPtr;
    Ns_Entry        *entry;
    int              new;

    /*
     * Find the statement in the handle cache.
     */

    entry = Ns_CacheCreateEntry(handlePtr->cache, sql, &new);
    if (new) {
        if ((stmtPtr = ParseBindVars(handlePtr, sql, length)) == NULL) {
            Ns_CacheFlushEntry(entry);
            return NULL;
        }
        Ns_CacheSetValueSz(entry, stmtPtr, sizeof(Statement) + stmtPtr->length);
    } else {
        stmtPtr = Ns_CacheGetValue(entry);
    }

    /*
     * Prepare the query.
     */

    if (stmtPtr->arg == NULL) {

        DbiLog(handle, Debug, "Dbi_Prepare: calling Dbi_PrepareProc: id: %u nqueries: %d",
               stmtPtr->id, stmtPtr->nqueries);

        if ((*driver->prepareProc)(handle, stmtPtr->sql, stmtPtr->length,
                                   stmtPtr->id, stmtPtr->nqueries,
                                   &stmtPtr->arg, driver->arg) != NS_OK) {
            return NULL;
        }
    }

    return (Dbi_Statement *) stmtPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetNumVariables --
 *
 *      Return the number of bind variables found in the prepared
 *      statement.
 *
 * Results:
 *      Zero or more variables.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_GetNumVariables(Dbi_Statement *stmt)
{
    return ((Statement *) stmt)->nbind;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetBindVariable --
 *
 *      Get the name of the bind variable at the given index.
 *
 *      Called by user code after a Dbi_Prepare to identify variables
 *      which need bound.
 *
 * Results:
 *      NS_OK if exists, NS_ERROR otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_GetBindVariable(Dbi_Statement *stmt, int idx, CONST char **namePtr)
{
    Statement *stmtPtr = (Statement *) stmt;

    if (idx < 0 || idx >= stmtPtr->nbind) {
        return NS_ERROR;
    }
    *namePtr = stmtPtr->vars[idx].name;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Exec --
 *
 *      Execute an SQL statement.
 *
 * Results:
 *      DBI_EXEC_DML, DBI_EXEC_ROWS, or DBI_EXEC_ERROR.
 *      nrowsPtr updated with rows affected for a DBI_EXEC_DML result.
 *      nrowsPtr and ncolsPtr updated for a DBI_EXEC_ROWS result.
 *
 * Side effects:
 *      SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

DBI_EXEC_STATUS
Dbi_Exec(Dbi_Handle *handle, Dbi_Statement *stmt,
         CONST char **values, unsigned int *lengths)
{
    Statement       *stmtPtr   = (Statement *) stmt;
    Handle          *handlePtr = (Handle *) handle;
    Dbi_Driver      *driver    = handlePtr->poolPtr->driver;
    DBI_EXEC_STATUS  status;

    DbiLog(handle, Debug, "Dbi_Exec: calling Dbi_ExecProc: bound: %d sql: %s",
           stmtPtr->nbind, stmtPtr->sql);

    if (stmtPtr->nbind > 0
        && (values == NULL || lengths == NULL)) {
        Dbi_SetException(handle, "DBI",
            "bug: bind variables not bound with values");
    }

    status = (*driver->execProc)(handle, stmtPtr->sql, stmtPtr->length,
                                 values, lengths, stmtPtr->nbind,
                                 &handlePtr->numCols, &handlePtr->numRows,
                                 stmtPtr->arg, driver->arg);
    handlePtr->stats.queries++;
    stmtPtr->nqueries++;

    if (status == DBI_EXEC_ROWS) {
        handlePtr->fetchingRows = NS_TRUE;
        if (handlePtr->numCols < 1) {
            Dbi_SetException(handle, "DBI",
                "bug: driver returned rows but failed to set number of columns");
            return DBI_EXEC_ERROR;
        }
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_NumColumns, Dbi_NumRows --
 *
 *      Return the number of columns or rows.
 *
 * Results:
 *      Number of columns or rows. 0 if none.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_NumColumns(Dbi_Handle *handle)
{
    return ((Handle *) handle)->numCols;
}

int
Dbi_NumRows(Dbi_Handle *handle)
{
    return ((Handle *) handle)->numRows;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_NextValue --
 *
 *      Fetch the result from the next column index of the next row.
 *      If columnPtr is not null, set the column name also.
 *      This routine is normally called repeatedly after a Dbi_Exec.
 *
 * Results:
 *      DBI_VALUE:       the next result value was successfully retrieved
 *      DBI_END_COL:     the result of the last column in current row
 *      DBI_END_ROWS:    the result of the last column in the last row
 *      DBI_VALUE_ERROR: an error occurred retrieving the result
 *
 * Side effects:
 *      The current column/row counter is advanced.
 *
 *----------------------------------------------------------------------
 */

DBI_VALUE_STATUS
Dbi_NextValue(Dbi_Handle *handle, CONST char **valuePtr, int *vlengthPtr,
              CONST char **columnPtr, int *clengthPtr)
{
    Handle     *handlePtr = (Handle *) handle;
    Dbi_Driver *driver    = handlePtr->poolPtr->driver;
    int         status;

    if (handlePtr->fetchingRows == NS_FALSE) {
        Ns_Log(Bug, "dbi: Dbi_NextValue: no pending rows");
        return DBI_VALUE_ERROR;
    }

    DbiLog(handlePtr, Debug, "Dbi_NextValue: calling Dbi_ValueProc: "
           "ncols: %d nrows: %d column index: %d row index: %d",
           handlePtr->numCols, handlePtr->numRows,
           handlePtr->currentCol, handlePtr->currentRow);

    status = (*driver->valueProc)(handle, handlePtr->currentCol, handlePtr->currentRow,
                                  valuePtr, vlengthPtr, driver->arg);
    if (status == NS_ERROR) {
        return DBI_VALUE_ERROR;
    }

    if (columnPtr != NULL && clengthPtr != NULL) {
        DbiLog(handlePtr, Debug, "Dbi_NextValue: calling Dbi_ColumnProc: "
               "column index: %d row index: %d",
               handlePtr->currentCol, handlePtr->currentRow);
        status = (*driver->columnProc)(handle, handlePtr->currentCol,
                                       columnPtr, clengthPtr, driver->arg);
        if (status == NS_ERROR) {
            return DBI_VALUE_ERROR;
        }
    }

    if (handlePtr->currentCol++ == handlePtr->numCols - 1) {
        handlePtr->currentCol = 0; /* Reset for next row. */

        if (handlePtr->currentRow == handlePtr->numRows - 1) {
            handlePtr->fetchingRows = NS_FALSE;
            status = DBI_END_ROWS;
        } else {
            handlePtr->currentRow++;
            status = DBI_END_COL;
        }
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Flush --
 *
 *      Flush any pending rows.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Rows waiting in the result set are dumped, perhaps by simply
 *      fetching them over one by one -- depends on driver.
 *
 *----------------------------------------------------------------------
 */

void
Dbi_Flush(Dbi_Handle  *handle)
{
    Handle     *handlePtr = (Handle *) handle;
    Dbi_Driver *driver    = handlePtr->poolPtr->driver;

    DbiLog(handlePtr, Debug, "Dbi_Flush: calling Dbi_FlushProc:"
           "fetching rows: %d cols: %d rows: %d currentCol: %d currentRow: %d",
           handlePtr->fetchingRows, handlePtr->numCols, handlePtr->numRows,
           handlePtr->currentCol, handlePtr->currentRow);

    (*driver->flushProc)(handle, driver->arg);

    handlePtr->numCols = handlePtr->numRows = 0;
    handlePtr->currentCol = handlePtr->currentRow = 0;
    handlePtr->fetchingRows = NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Reset --
 *
 *      Reset a handle.
 *
 * Results:
 *      NS_OK if handle reset, NS_ERROR if reset failed and handle
 *      should be returned to pool.
 *
 * Side effects:
 *      Depends on driver.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Reset(Dbi_Handle *handle)
{
    Dbi_Driver *driver = ((Handle *) handle)->poolPtr->driver;
    int         status;

    DbiLog(handle, Debug, "%s", "Dbi_Reset: calling Dbi_ResetProc");

    status = (*driver->resetProc)(handle, driver->arg);

    if (Dbi_ExceptionPending(handle)) {
        DbiLog(handle, Error, "reset: %s: %s",
               Dbi_ExceptionCode(handle), Dbi_ExceptionMsg(handle));
        Dbi_ResetException(handle);
    }

    return status;
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
 *      Pointer to dest.string.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_Stats(Ns_DString *dest, Dbi_Pool *pool)
{
    Pool *poolPtr = (Pool *) pool;

    Ns_MutexLock(&poolPtr->lock);
    Ns_DStringPrintf(dest, "handlegets %d handlemisses %d "
                     "handleopens %d handlefailures %d queries %d "
                     "agedcloses %d idlecloses %d "
                     "oppscloses %d bounces %d",
                     poolPtr->stats.handlegets,  poolPtr->stats.handlemisses,
                     poolPtr->stats.handleopens, poolPtr->stats.handlefailures,
                     poolPtr->stats.queries,
                     poolPtr->stats.otimecloses, poolPtr->stats.atimecloses,
                     poolPtr->stats.querycloses, poolPtr->stale_on_close);
    Ns_MutexUnlock(&poolPtr->lock);

    return Ns_DStringValue(dest);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_PoolName, Dbi_DriverName, Dbi_DatabaseName --
 *
 *      Return the string name of the pool, the driver or the database.
 *
 * Results:
 *      String name.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

CONST char *
Dbi_PoolName(Dbi_Pool *pool)
{
    return ((Pool *) pool)->module;
}

CONST char *
Dbi_DriverName(Dbi_Pool *pool)
{
    return ((Pool *) pool)->driver->name;
}

CONST char *
Dbi_DatabaseName(Dbi_Pool *pool)
{
    return ((Pool *) pool)->driver->database;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SetException --
 *
 *      Set SQL exception state and message for the given handle.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Status code and message may be updated.
*
 *----------------------------------------------------------------------
 */

void
Dbi_SetException(Dbi_Handle *handle, CONST char *sqlstate, CONST char *fmt, ...)
{
    Handle      *handlePtr = (Handle *) handle;
    Ns_DString  *ds = &handlePtr->dsExceptionMsg;
    va_list      ap;
    int          len;

    strncpy(handlePtr->cExceptionCode, sqlstate, 6);
    handlePtr->cExceptionCode[5] = '\0';

    if (fmt != NULL) {
        Ns_DStringTrunc(ds, 0);
        va_start(ap, fmt);
        Ns_DStringVPrintf(ds, (char *) fmt, ap);
        va_end(ap);
        len = Ns_DStringLength(ds);
        while (ds->string[len - 1] == '\n') {
            Ns_DStringTrunc(ds, len - 1);
        }
    }
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ResetException --
 *
 *      Clear any stored SQL exception state and message for the
 *      given handle.
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
Dbi_ResetException(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    handlePtr->cExceptionCode[0] = '\0';
    Ns_DStringTrunc(&handlePtr->dsExceptionMsg, 0);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ExceptionCode --
 *
 *      The current 5 character exception code for the given handle.
 *
 * Results:
 *      cstring.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_ExceptionCode(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    return handlePtr->cExceptionCode;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ExceptionMsg --
 *
 *      The current exception message for the given handle.
 *
 * Results:
 *      Pointer to cstring or NULL if there is no message.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_ExceptionMsg(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    if (Ns_DStringLength(&handlePtr->dsExceptionMsg)) {
        return Ns_DStringValue(&handlePtr->dsExceptionMsg);
    }
    return NULL;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ExceptionPending --
 *
 *      Is an exception currently set on the given handle?
 *
 * Results:
 *      NS_TRUE or NS_FALSE.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ExceptionPending(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    if (handlePtr->cExceptionCode[0] != '\0'
        || Ns_DStringLength(&handlePtr->dsExceptionMsg)) {
        return NS_TRUE;
    }
    return NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * GetServer --
 *
 *      Get per-server data.
 *
 * Results:
 *      Pointer to per-server data structure or NULL if doesn't exist.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

ServerData *
GetServer(CONST char *server)
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
    } else if (Connected(handlePtr)) {
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
    Dbi_Handle *handle  = (Dbi_Handle *) handlePtr;
    Pool       *poolPtr = handlePtr->poolPtr;
    Dbi_Driver *driver  = poolPtr->driver;
    char       *reason  = NULL;

    if (Connected(handlePtr)) {
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

            DbiLog(handle, Notice, "closing %s handle, %d queries",
                   reason, handlePtr->stats.queries);

            CloseStatements(handlePtr->cache);

            (*driver->closeProc)(handle, driver->arg);
            handle->arg = NULL;
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

    Tcl_DStringAppendElement(dsPtr, poolPtr->module);
}


/*
 *----------------------------------------------------------------------
 *
 * AtShutdown --
 *
 *      Shutdown callback to close handles in a pool.
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
AtShutdown(Ns_Time *toPtr, void *arg)
{
    Pool       *poolPtr = arg;
    Ns_DString  ds;

    if (toPtr == NULL) {
        Ns_MutexLock(&poolPtr->lock);
        poolPtr->stopping = 1;
        Ns_CondBroadcast(&poolPtr->getCond);
        Ns_MutexUnlock(&poolPtr->lock);
    } else {
        Ns_DStringInit(&ds);
        Ns_Log(Notice, "dbi[%s:%s]: %s", poolPtr->driver->name, poolPtr->module,
               Dbi_Stats(&ds, (Dbi_Pool *) poolPtr));
        Ns_DStringFree(&ds);
        CheckPool(poolPtr, 1);
    }
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
    Dbi_Handle *handle  = (Dbi_Handle *) handlePtr;
    Pool       *poolPtr = handlePtr->poolPtr;
    Dbi_Driver *driver  = poolPtr->driver;
    char       *msg;
    int         status  = NS_ERROR;

    if (!poolPtr->stopping) {
        DbiLog(handle, Debug, "%s", "Connect: calling Dbi_OpenProc");
        status = (*driver->openProc)(handle, driver->arg);
        poolPtr->stats.handleopens++;
        if (status != NS_OK) {
            poolPtr->stats.handlefailures++;
            DbiLog(handle, Error, "handle connection failed (%d): "
                   "code: '%s' msg: '%s'",
                   poolPtr->stats.handlefailures,
                   Dbi_ExceptionCode(handle), Dbi_ExceptionMsg(handle));
        } else {
            handlePtr->atime = handlePtr->otime = time(NULL);
            msg = Dbi_ExceptionMsg(handle);
            DbiLog(handle, Notice, "opened handle %d/%d%s%s",
                   handlePtr->n, poolPtr->nhandles,
                   msg ? ": " : "", msg ? msg : "");
            Dbi_ResetException(handle);
        }
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Connected --
 *
 *      Is the given database handle currently connected?
 *
 * Results:
 *      NS_TRUE or NS_FALSE.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Connected(Handle *handlePtr)
{
    Dbi_Handle *handle = (Dbi_Handle *) handlePtr;
    Dbi_Driver *driver = handlePtr->poolPtr->driver;

    return (*driver->connectedProc)(handle, driver->arg);
}


/*
 *----------------------------------------------------------------------
 *
 * FreeStatement --
 *
 *      Cache callback to free a prepared statement.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Calls the driver's Dbi_PrepareCloseProc.
 *
 *----------------------------------------------------------------------
 */

static void
FreeStatement(void *arg)
{
    Statement  *stmtPtr = arg;
    Dbi_Driver *driver;

    Tcl_DeleteHashTable(&stmtPtr->bindTable);
    if (stmtPtr->arg != NULL) {
        DbiLog(stmtPtr->handlePtr, Debug,
               "FreeStatement: calling Dbi_PrepareCloseProc: id: %u nqueries: %u",
               stmtPtr->id, stmtPtr->nqueries);
        driver = stmtPtr->handlePtr->poolPtr->driver;
        (*driver->prepareCloseProc)(stmtPtr->arg, driver->arg);
    }
    ns_free(stmtPtr);
}


/*
 *----------------------------------------------------------------------
 *
 * CloseStatements --
 *
 *      Call the driver proc for each prepared statement in the cache,
 *      but don't flush the cache entries.
 *
 *      This is called just before a handle is closed.
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
CloseStatements(Ns_Cache *cache)
{
    Ns_CacheSearch  search;
    Ns_Entry       *entry;
    Statement      *stmtPtr;
    Dbi_Driver     *driver;

    entry = Ns_CacheFirstEntry(cache, &search);
    while (entry != NULL) {
        stmtPtr = Ns_CacheGetValue(entry);
        if (stmtPtr->arg != NULL) {
            DbiLog(stmtPtr->handlePtr, Debug,
                   "CloseStatements: calling Dbi_PrepareCloseProc: id: %u nqueries: %u",
                   stmtPtr->id, stmtPtr->nqueries);
            driver = stmtPtr->handlePtr->poolPtr->driver;
            (*driver->prepareCloseProc)(stmtPtr->arg, driver->arg);
        }
        entry = Ns_CacheNextEntry(&search);
    }
}



/*
 *----------------------------------------------------------------------
 *
 * ParseBindVars --
 *
 *      Parse the given SQL string for bind variables of the form :name
 *      and call the driver for a replacement string.  Store the
 *      identified bind variables in a hash table of keys.
 *
 * Results:
 *      New Statement pointer or NULL if max bind variables exceeded.
 *
 * Side effects:
 *      Memory for Statement is allocated.
 *
 *----------------------------------------------------------------------
 */

static Statement *
ParseBindVars(Handle *handlePtr, CONST char *origSql, int origLength)
{
    Statement  *stmtPtr = NULL;
    Ns_DString  ds, origDs;
    char        save, *sql, *p, *chunk, *bind;
    int         len, isQuoted, status = NS_OK;

#define preveq(c) (p != sql && *(p-1) == (c))
#define nexteq(c) (*(p+1) == (c))

    /*
     * Allocate a new Statement. Allocate a little extra memory
     * for driver notation which may be (but is unlikely to be)
     * larger than the original. Check for overrun at the end.
     */

    stmtPtr = ns_calloc(1, sizeof(Statement) + origLength + 32);
    stmtPtr->handlePtr = handlePtr;
    Tcl_InitHashTable(&stmtPtr->bindTable, TCL_STRING_KEYS);

    /*
     * Save a copy of the orginal sql to chop up.
     */

    Ns_DStringInit(&ds);
    Ns_DStringInit(&origDs);
    Ns_DStringNAppend(&origDs, origSql, origLength);

    sql = Ns_DStringValue(&origDs);
    len = Ns_DStringLength(&origDs);

    p = sql;
    chunk = sql;
    bind = NULL;
    isQuoted = 0;

    while (len > 0) {

        if (*p == ':' && !isQuoted && !nexteq(':') && !preveq(':') && !preveq('\\')) {
            /* found the start of what looks like a bind variable */
            bind = p;
        } else if (*p == '\'' && bind == NULL) {
            if (p == sql || !preveq('\\')) {
                isQuoted = !isQuoted;
            }
        } else if (bind != NULL) {
            if (!(isalnum((int)*p) || *p == '_') && p > bind) {
                /* End of bind var. Append the preceding chunk. */
                *bind = '\0';
                Ns_DStringNAppend(&ds, chunk, bind - chunk);
                chunk = p;
                /* Now substitute the bind var. */
                ++bind;     /* beginning of bind var */
                save = *p;
                *p = '\0';  /* end of bind var */
                if ((status = DefineBindVar(stmtPtr, bind, &ds))
                    != NS_OK) {
                    goto done;
                }
                *p = save;
                bind = NULL;
            }
        }
        ++p;
        --len;
    }
    /* append remaining chunk */
    Ns_DStringNAppend(&ds, chunk, bind ? bind - chunk : p - chunk);
    /* check for trailing bindvar */
    if (bind != NULL && p > bind) {
        if ((status = DefineBindVar(stmtPtr, ++bind, &ds))
            != NS_OK) {
            goto done;
        }
    }

    /*
     * Check for overrun.
     */

    if (Ns_DStringLength(&ds) > origLength + 32) {
        Dbi_SetException((Dbi_Handle *) handlePtr, "HY000",
                         "bug: not enough memory for bound sql");
        status = NS_ERROR;
        goto done;
    }

 done:
    if (status == NS_OK) {
        stmtPtr->id = ++handlePtr->stmtid;
        strncpy(stmtPtr->sql, Ns_DStringValue(&ds), Ns_DStringLength(&ds));
        stmtPtr->length = Ns_DStringLength(&ds);
    } else {
        Tcl_DeleteHashTable(&stmtPtr->bindTable);
        ns_free(stmtPtr);
        stmtPtr = NULL;
    }
    Ns_DStringFree(&ds);
    Ns_DStringFree(&origDs);

    return stmtPtr;
}

static int
DefineBindVar(Statement *stmtPtr, CONST char *name, Ns_DString *dsPtr)
{
    Dbi_Driver    *driver = stmtPtr->handlePtr->poolPtr->driver;
    Tcl_HashEntry *hPtr;
    int            new, index;

    index = stmtPtr->nbind;
    if (index >= DBI_MAX_BIND) {
        Dbi_SetException((Dbi_Handle *) stmtPtr->handlePtr,
                         "HY000", "max bind variables exceeded: %d",
                         DBI_MAX_BIND);
        return NS_ERROR;
    }

    /*
     * Double count duplicate bind variables as some drivers
     * only support '?' notation, and they have no way to figure
     * out that a bind variable is reused within a statement.
     */

    hPtr = Tcl_CreateHashEntry(&stmtPtr->bindTable, name, &new);
    if (new) {
        Tcl_SetHashValue(hPtr, (void *) index);
    }
    stmtPtr->vars[index].name = Tcl_GetHashKey(&stmtPtr->bindTable, hPtr);
    stmtPtr->nbind++;

    (*driver->bindVarProc)(dsPtr, name, index, driver->arg);

    return NS_OK;
}
