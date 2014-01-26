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
#include "nsdbidrv.h"

extern Ns_TclInterpInitProc DbiInitInterp;


/*
 * The following structure tracks which pools are
 * available to a virtual server.
 */

typedef struct ServerData {
    CONST char        *server;
    Dbi_Pool          *defpoolPtr;  /* The default pool. */
    Tcl_HashTable      poolsTable;  /* All available pools. */
} ServerData;


/*
 * The following structure defines a pool of database handles.
 */

typedef struct Pool {

    char                 *module;          /* Pool name.  */

    Ns_Mutex              lock;
    Ns_Cond               cond;

    struct Handle        *firstPtr;
    struct Handle        *lastPtr;

    int                   maxhandles;      /* Max handles to create for pool. */
    int                   nhandles;        /* Current number of handles created. */
    int                   idlehandles;     /* Number of unused handles in pool. */

    int                   cachesize;       /* Size of prepared statement cache. */

    int                   maxRows;         /* Default max rows a query may return. */
    time_t                maxidle;         /* Seconds before unused handle is closed.  */
    time_t                maxopen;         /* Seconds before active handle is closed. */
    int                   maxqueries;      /* Close active handle after maxqueries. */
    int                   timeout;         /* Default seconds to wait for handle. */

    int                   epoch;           /* Epoch for bouncing handles. */
    int                   stopping;        /* Server is shutting down. */

    struct {
        unsigned int      handlegets;      /* Total No. requests for a handle. */
        unsigned int      handlemisses;    /* Handle requests which timed out. */
        unsigned int      handleopens;     /* Number of times connected to db. */
        unsigned int      handlefailures;  /* Handle open attempts which failed. */
        unsigned int      queries;         /* Total queries by all handles. */
        unsigned int      otimecloses;     /* Handle closes due to maxopen. */
        unsigned int      atimecloses;     /* Handle closed due to maxidle. */
        unsigned int      querycloses;     /* Handle closes due to maxqueries. */
    } stats;


    /*
     * Registered driver callbacks and data.
     */

    ClientData           configData;    /* Driver private config context. */

    CONST char           *drivername;   /* Driver identifier. */
    CONST char           *database;     /* Database identifier. */

    Dbi_OpenProc         *openProc;
    Dbi_CloseProc        *closeProc;
    Dbi_ConnectedProc    *connectedProc;
    Dbi_BindVarProc      *bindVarProc;
    Dbi_PrepareProc      *prepareProc;
    Dbi_PrepareCloseProc *prepareCloseProc;
    Dbi_ExecProc         *execProc;
    Dbi_NextRowProc      *nextRowProc;
    Dbi_ColumnLengthProc *columnLengthProc;
    Dbi_ColumnValueProc  *columnValueProc;
    Dbi_ColumnNameProc   *columnNameProc;
    Dbi_TransactionProc  *transProc;
    Dbi_FlushProc        *flushProc;
    Dbi_ResetProc        *resetProc;

} Pool;

/*
 * The following structure defines a handle in a database pool.
 */

typedef struct Handle {

    /*
     * Publicly visible in a Dbi_Handle.
     */

    struct Pool       *poolPtr;      /* The pool this handle belongs to. */
    unsigned int       rowIdx;       /* The current row of the result set. */
    ClientData         driverData;   /* Driver private handle context. */

    /*
     * Private to a Handle.
     */

    struct Handle     *nextPtr;      /* Next handle in pool or thread cache. */

    Dbi_Isolation      isolation;    /* Isolation level of transactions. */
    int                transDepth;   /* Nesting depth of transactions.*/

    char               cExceptionCode[6];
    Ns_DString         dsExceptionMsg;

    time_t             otime;        /* Time when handle was connected to db. */
    time_t             atime;        /* Time when handle was last used. */
    int                n;            /* Handle n of maxhandles when acquired. */
    int                epoch;

    /* Result status. */

    struct Statement  *stmtPtr;      /* A statement being executed. */

    int                fetchingRows; /* Is there a pending result set? */
    unsigned int       nextRow;      /* Counts the calls to NextRow. */
    int                maxRows;      /* Max rows returned by query, default from pool.. */

    Ns_Cache          *cache;        /* Cache of statements and driver data. */
    unsigned int       stmtid;       /* Unique ID counter for cached statements. */

    struct {
        unsigned int   queries;      /* Total queries via current connection. */
    } stats;

} Handle;


/*
 * The following structure defines a prepared statement kept
 * in a per-handle cache.
 */

typedef struct Statement {

    /*
     * Public in a Dbi_Statement.
     */

    char             *sql;          /* Driver specific SQL: &driverSql */
    int               length;       /* Length of sql. */
    unsigned int      id;           /* Unique (per handle) statement ID. */
    unsigned int      nqueries;     /* Total queries for this statement. */
    ClientData        driverData;   /* Statement context for driver. */

    /*
     * Private to a Statement.
     */

    Handle           *handlePtr;    /* Handle this Statement belongs to. */
    unsigned int      numCols;      /* Number of columns in a result. */
    unsigned int      numVars;      /* Number of bind variables. */

    Tcl_HashEntry    *hPtr;         /* Entry in the per-handle statement table. */

    Tcl_HashTable     bindTable;    /* Bind variables by name. */
    struct {
        CONST char   *name;         /* (Hash table key) */
    } vars[DBI_MAX_BIND];           /* Bind variables by index. */

    char              driverSql[1]; /* Driver specific SQL. */

} Statement;


/*
 * Local functions defined in this file
 */

#define Log(handle,level,msg,...)                               \
    Ns_Log(level, "dbi[%s]: " msg,                              \
           ((Handle *) handle)->poolPtr->module, __VA_ARGS__)


static void MapPool(ServerData *sdataPtr, Pool *poolPtr, int isdefault);
static ServerData *GetServer(CONST char *server);
static void ReturnHandle(Handle * handle) NS_GNUC_NONNULL(1);
static int CloseIfStale(Handle *, time_t now) NS_GNUC_NONNULL(1);
static int Connect(Handle *) NS_GNUC_NONNULL(1);
static int Connected(Handle *handlePtr) NS_GNUC_NONNULL(1);
static void CheckPool(Pool *poolPtr, int stale) NS_GNUC_NONNULL(1);
static Statement *ParseBindVars(Handle *handlePtr, CONST char *sql, int length);
static int DefineBindVar(Statement *stmtPtr, CONST char *name, Ns_DString *dsPtr);

static Ns_Callback FreeStatement;
static Ns_Callback FreeThreadHandles;

static Ns_Callback     ScheduledPoolCheck;
static Ns_ArgProc      PoolCheckArgProc;
static Ns_ShutdownProc AtShutdown;

/*
 * Static variables defined in this file
 */

static Tcl_HashTable  serversTable;
static Ns_Tls         tls;          /* Per-thread handle cache. */



/*
 *----------------------------------------------------------------------
 *
 * Dbi_LibInit --
 *
 *      Dbi library entry point. 
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Registers dbi commands for all servers.
 *
 *----------------------------------------------------------------------
 */

void
Dbi_LibInit(void)
{
    ServerData    *sdataPtr;
    char          *server;
    int            new;
    static int     once = 0;

    if (!once) {
	Ns_Set  *set;
	int      i;

        once = 1;

        Nsd_LibInit();
        Tcl_InitHashTable(&serversTable, TCL_STRING_KEYS);
        Ns_TlsAlloc(&tls, FreeThreadHandles);

        Ns_RegisterProcInfo((void *)ScheduledPoolCheck, "dbi:idlecheck", PoolCheckArgProc);
        Ns_RegisterProcInfo((void *)DbiInitInterp, "dbi:initinterp", NULL);

        set = Ns_ConfigGetSection("ns/servers");
        for (i = 0; i < Ns_SetSize(set); i++) {
	    Tcl_HashEntry *hPtr;

            server = Ns_SetKey(set, i);

            sdataPtr = ns_calloc(1, sizeof(ServerData));
            sdataPtr->server = server;
            Tcl_InitHashTable(&sdataPtr->poolsTable, TCL_STRING_KEYS);

            hPtr = Tcl_CreateHashEntry(&serversTable, server, &new);
            Tcl_SetHashValue(hPtr, sdataPtr);

            if (Ns_TclRegisterTrace(server, DbiInitInterp, server,
                                    NS_TCL_TRACE_CREATE) != NS_OK) {
                Ns_Log(Error, "dbi: error registering tcl commands for server '%s'",
                       server);
            }
        }
    }
}


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
                   CONST char *driver, CONST char *database,
                   CONST Dbi_DriverProc *procs, ClientData configData)
{
    ServerData            *sdataPtr;
    CONST Dbi_DriverProc  *procPtr;
    Pool                  *poolPtr;
    Tcl_HashEntry         *hPtr;
    Tcl_HashSearch         search;
    char                  *path;
    int                    nprocs, isdefault;

    poolPtr = ns_calloc(1, sizeof(Pool));
    poolPtr->drivername = driver;
    poolPtr->database = database;
    poolPtr->configData = configData;

    for (procPtr = procs, nprocs = 0; procPtr->proc != NULL; procPtr++) {
        switch (procPtr->id) {
        case Dbi_OpenProcId:
	    poolPtr->openProc = (Dbi_OpenProc *)procPtr->proc;
            break;
        case Dbi_CloseProcId:
	  poolPtr->closeProc = (Dbi_CloseProc *)procPtr->proc;
            break;
        case Dbi_ConnectedProcId:
            poolPtr->connectedProc = (Dbi_ConnectedProc *)procPtr->proc;
            break;
        case Dbi_PrepareProcId:
            poolPtr->prepareProc = (Dbi_PrepareProc *)procPtr->proc;
            break;
        case Dbi_PrepareCloseProcId:
            poolPtr->prepareCloseProc = (Dbi_PrepareCloseProc *)procPtr->proc;
            break;
        case Dbi_BindVarProcId:
            poolPtr->bindVarProc = (Dbi_BindVarProc *)procPtr->proc;
            break;
        case Dbi_ExecProcId:
            poolPtr->execProc = (Dbi_ExecProc *)procPtr->proc;
            break;
        case Dbi_NextRowProcId:
            poolPtr->nextRowProc = (Dbi_NextRowProc *)procPtr->proc;
            break;
        case Dbi_ColumnLengthProcId:
            poolPtr->columnLengthProc = (Dbi_ColumnLengthProc *)procPtr->proc;
            break;
        case Dbi_ColumnValueProcId:
            poolPtr->columnValueProc = (Dbi_ColumnValueProc *)procPtr->proc;
            break;
        case Dbi_ColumnNameProcId:
            poolPtr->columnNameProc = (Dbi_ColumnNameProc *)procPtr->proc;
            break;
        case Dbi_TransactionProcId:
            poolPtr->transProc = (Dbi_TransactionProc *)procPtr->proc;
            break;
        case Dbi_FlushProcId:
            poolPtr->flushProc = (Dbi_FlushProc *)procPtr->proc;
            break;
        case Dbi_ResetProcId:
            poolPtr->resetProc = (Dbi_ResetProc *)procPtr->proc;
            break;        
        default:
            Ns_Log(Error, "dbi: Dbi_RegisterDriver: invalid Dbi_ProcId: %d",
                   procPtr->id);
            ns_free(poolPtr);
            return NS_ERROR;
        }
        nprocs++;
    }

    /*
     * All callbacks are (currently) required.
     */

    if (nprocs < Dbi_ResetProcId) {
        Ns_Log(Error, "dbi: Dbi_RegisterDriver: driver callback(s) missing");
        ns_free(poolPtr);
        return NS_ERROR;
    }

    /*
     * Configure this pool.
     */

    path = Ns_ConfigGetPath(server, module, NULL);
    if (path == NULL) {
        Ns_Log(Error, "dbi[%s]: no configuration for db", module);
        return NS_ERROR;
    }

    Ns_MutexSetName2(&poolPtr->lock, "dbi", module);
    Ns_CondInit(&poolPtr->cond);

    poolPtr->module     = ns_strdup(module);
    poolPtr->cachesize  = Ns_ConfigIntRange(path, "cachesize",  1024*1024,  0, INT_MAX);
    poolPtr->maxhandles = Ns_ConfigIntRange(path, "maxhandles", 0,          0, INT_MAX);
    poolPtr->maxRows    = Ns_ConfigIntRange(path, "maxrows",    1000,    1000, INT_MAX);
    poolPtr->maxidle    = Ns_ConfigIntRange(path, "maxidle",    0,          0, INT_MAX);
    poolPtr->maxopen    = Ns_ConfigIntRange(path, "maxopen",    0,          0, INT_MAX);
    poolPtr->maxqueries = Ns_ConfigIntRange(path, "maxqueries", 0,          0, INT_MAX);
    poolPtr->timeout    = Ns_ConfigIntRange(path, "timeout",    10,         0, INT_MAX);

    if (poolPtr->maxidle || poolPtr->maxopen) {
        Ns_ScheduleProc(ScheduledPoolCheck, poolPtr, 0,
                        Ns_ConfigIntRange(path, "checkinterval", 600, 30, INT_MAX));
    }
    Ns_RegisterAtShutdown(AtShutdown, poolPtr);

    /*
     * Make pool available to this virtual server, or to all virtual servers
     * if module is global.
     */

    isdefault = Ns_ConfigBool(path, "default", 0); /* Default pool for this server. */

    /*
     * Hard to believe: when we use "if (server != NULL) ..." as the
     * next command, gcc version 4.8.2 (on GenToo and MacPorts)
     * produces at least with -O3 code the enters the first branch
     * even when "server" is NULL. This leads to a crash in
     * Tcl_FindHashEntry(). Compiling with -O0 is fine for this gcc
     * version, clang is as well ok.
     */
    if (server != NULL) {
	assert(server != NULL);
        hPtr = Tcl_FindHashEntry(&serversTable, server);
        assert(hPtr != NULL);
        sdataPtr = Tcl_GetHashValue(hPtr);
        MapPool(sdataPtr, poolPtr, isdefault);
    } else {
        hPtr = Tcl_FirstHashEntry(&serversTable, &search);
        while (hPtr != NULL) {
            sdataPtr = Tcl_GetHashValue(hPtr);
            MapPool(sdataPtr, poolPtr, isdefault);
            hPtr = Tcl_NextHashEntry(&search);
        }
    }

    return NS_OK;
}

static void
MapPool(ServerData *sdataPtr, Pool *poolPtr, int isdefault)
{
    Tcl_HashEntry *hPtr;
    int            new;

    hPtr = Tcl_CreateHashEntry(&sdataPtr->poolsTable, poolPtr->module, &new);
    Tcl_SetHashValue(hPtr, poolPtr);
    if (isdefault) {
        sdataPtr->defpoolPtr = (Dbi_Pool *) poolPtr;
    }
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
        Ns_Log(Error, "dbi: invalid server '%s' while getting db '%s'", server, poolname);
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
            Ns_Log(Error, "dbi: no default db for server '%s'", server); 
        } else {
            Ns_Log(Error, "dbi: invalid db '%s' for server '%s'", poolname, server);
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
 *      Get a single handle from a pool within the given timeout.
 *
 * Results:
 *      NS_OK, NS_TIMEOUT or NS_ERROR.
 *
 * Side effects:
 *      New database handle may be opened if needed.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_GetHandle(Dbi_Pool *pool, Ns_Time *timeoutPtr, Dbi_Handle **handlePtrPtr)
{
    Pool       *poolPtr = (Pool *) pool;
    Handle     *handlePtr, *threadHandlePtr;
    Ns_Time     time;
    int         maxhandles, status;

    /*
     * Check the thread-local handle cache for a non-pooled handle.
     */

    handlePtr = NULL;
    threadHandlePtr = Ns_TlsGet(&tls);

    while (threadHandlePtr != NULL) {
        if (STREQ(poolPtr->module, threadHandlePtr->poolPtr->module)) {
            handlePtr = threadHandlePtr;
            break;
        }
        threadHandlePtr = threadHandlePtr->nextPtr;
    }

    maxhandles = -1;
    status = NS_OK;

    if (handlePtr == NULL) {

        if (timeoutPtr == NULL) {
            Ns_GetTime(&time);
            Ns_IncrTime(&time, poolPtr->timeout, 0);
            timeoutPtr = &time;
        }

        Ns_MutexLock(&poolPtr->lock);

        poolPtr->stats.handlegets++;

        while (status == NS_OK
               && !poolPtr->stopping
               && poolPtr->firstPtr == NULL
               && (poolPtr->maxhandles > 0
                   && poolPtr->nhandles >= poolPtr->maxhandles)) {
            status = Ns_CondTimedWait(&poolPtr->cond, &poolPtr->lock, timeoutPtr);
        }

        if (poolPtr->stopping) {
            status = NS_ERROR;
            handlePtr = NULL;

        } else if (poolPtr->firstPtr == NULL) {

            if (poolPtr->maxhandles == 0
                || poolPtr->nhandles < poolPtr->maxhandles) {
		char buf[100];

                poolPtr->nhandles++;

                snprintf(buf, sizeof(buf), "dbi:stmts:%s:%d",
                         poolPtr->module, poolPtr->nhandles);

                handlePtr = ns_calloc(1, sizeof *handlePtr);
                handlePtr->poolPtr = poolPtr;
                Ns_DStringInit(&handlePtr->dsExceptionMsg);
                handlePtr->cache = Ns_CacheCreateSz(buf, TCL_STRING_KEYS,
                                                    poolPtr->cachesize, FreeStatement);
                handlePtr->transDepth = -1;
                handlePtr->n = poolPtr->nhandles;
                handlePtr->epoch = poolPtr->epoch;

            } else {
                handlePtr = NULL;
                poolPtr->stats.handlemisses++;
            }
        } else {

            handlePtr = poolPtr->firstPtr;
            poolPtr->firstPtr = handlePtr->nextPtr;
            handlePtr->nextPtr = NULL;
            if (poolPtr->lastPtr == handlePtr) {
                poolPtr->lastPtr = NULL;
            }
            poolPtr->idlehandles--;
            handlePtr->n = poolPtr->maxhandles - poolPtr->idlehandles;
        }

        maxhandles = poolPtr->maxhandles;

        Ns_MutexUnlock(&poolPtr->lock);
    }

    /*
     * If we got a handle, make sure its connected, otherwise
     * return it. Save a per-thread connected handle.
     */

    if (handlePtr != NULL) {

        if (!Connected(handlePtr)
                && Connect(handlePtr) != NS_OK) {

            Ns_MutexLock(&poolPtr->lock);
            ReturnHandle(handlePtr);
            Ns_CondSignal(&poolPtr->cond);
            Ns_MutexUnlock(&poolPtr->lock);

            status = NS_ERROR;

        } else {
            *handlePtrPtr = (Dbi_Handle *) handlePtr;

            if (maxhandles == 0) {
                handlePtr->nextPtr = Ns_TlsGet(&tls);
                Ns_TlsSet(&tls, handlePtr);
                handlePtr->n = -1;
            }
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
 *      Handle is reset and closed as required.
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
     * Cleanup the handle.
     */

    if (Dbi_Reset(handle) != NS_OK) {
        /* Force the handle closed...? */
        Ns_Log(Warning, "Dbi_PutHandle: Reset failed...");
    }

    if (handlePtr->n != -1) {
	int  closed;

        /*
         * For non-thread handles which are going back to the pool
         * check for staleness and possibly close.
         */

        time(&now);
        handlePtr->atime = now;

        Ns_MutexLock(&poolPtr->lock);
        closed = CloseIfStale(handlePtr, now);
        ReturnHandle(handlePtr);
        if (!closed) {
            Ns_CondSignal(&poolPtr->cond);
        }
        Ns_MutexUnlock(&poolPtr->lock);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Prepare --
 *
 *      Parse the sql for bind variables.
 *
 * Results:
 *      NS_OK, or NS_ERROR if statement contains more than DBI_MAX_BIND
 *      bind variables.
 *
 * Side effects:
 *      Statement is parsed by driver callback and any bind variables
 *      are converted to driver specific notation.
 *
 *      Statement text is cached for handle.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Prepare(Dbi_Handle *handle, CONST char *sql, int length)
{
    Handle          *handlePtr = (Handle *) handle;
    Pool            *poolPtr = handlePtr->poolPtr;
    Statement       *stmtPtr;
    Ns_Entry        *entry;
    unsigned int     numVars;
    int              new;

    /*
     * Find the statement in the handle cache.
     */

    entry = Ns_CacheCreateEntry(handlePtr->cache, sql, &new);
    if (new) {
        if ((stmtPtr = ParseBindVars(handlePtr, sql, length)) == NULL) {
            Ns_CacheFlushEntry(entry);
            return NS_ERROR;
        }
        Ns_CacheSetValueSz(entry, stmtPtr, sizeof(Statement) + stmtPtr->length);
    } else {
        stmtPtr = Ns_CacheGetValue(entry);
    }
    handlePtr->stmtPtr = stmtPtr;

    /*
     * Prepare the query if not already done.
     */

    Log(handle, Debug, "Dbi_PrepareProc: id: %u, nqueries: %u, sql: %s",
        stmtPtr->id, stmtPtr->nqueries, stmtPtr->sql);

    numVars = stmtPtr->numVars;
    if ((*poolPtr->prepareProc)(handle, (Dbi_Statement *) stmtPtr,
                                &numVars, &stmtPtr->numCols) != NS_OK) {
        goto error;
    }
    if (numVars != stmtPtr->numVars) {
        Dbi_SetException(handle, "HY000",
            "bug: dbi found %u variables, driver found: %u",
            stmtPtr->numVars, numVars);
        goto error;
    }

    return NS_OK;

 error:
    handlePtr->stmtPtr = NULL;
    return NS_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_NumVariables --
 *
 *      Return the number of bind variables found in the prepared
 *      statement.
 *
 * Results:
 *      Number of bind variables for current statement (which
 *      may be zero).
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

unsigned int
Dbi_NumVariables(Dbi_Handle *handle)
{
    Statement *stmtPtr = ((Handle *) handle)->stmtPtr;

    assert(stmtPtr);

    return stmtPtr->numVars;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_VariableName --
 *
 *      Get the name of the bind variable at the given index for the
 *      statement currently prepared for the handle.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_VariableName(Dbi_Handle *handle, unsigned int index, CONST char **namePtr)
{
    Statement *stmtPtr = ((Handle *) handle)->stmtPtr;

    assert(stmtPtr);

    if (index >= stmtPtr->numVars) {
        Dbi_SetException(handle, "HY000",
            "bug: variable index out of bounds: index: %u, variables: %u",
            index, stmtPtr->numVars);
    }
    *namePtr = stmtPtr->vars[index].name;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_NumColumns --
 *
 *      Return the number of columns in the result of the current
 *      statement for the given handle.
 *
 * Results:
 *      Number of columns, which may be 0 if the statement is DML
 *      or DDL etc.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

unsigned int
Dbi_NumColumns(Dbi_Handle *handle)
{
    Statement *stmtPtr = ((Handle *) handle)->stmtPtr;

    assert(stmtPtr);

    return stmtPtr->numCols;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ColumnName --
 *
 *      Fetch the name if the column at index as a UTF8 string. The
 *      string belongs to the driver and should be copied if needed.
 *      The first column is at index 0.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ColumnName(Dbi_Handle *handle, unsigned int index, CONST char **namePtr)
{
    Handle        *handlePtr = (Handle *) handle;
    Pool          *poolPtr   = handlePtr->poolPtr;
    Dbi_Statement *stmt      = (Dbi_Statement *) handlePtr->stmtPtr;    

    assert(stmt);
    assert(namePtr);

    Log(handle, Debug, "Dbi_ColumnNameProc: column index: %u", index);

    return (*poolPtr->columnNameProc)(handle, stmt, index, namePtr);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Exec --
 *
 *      Execute the statement prepared for this handle with the given
 *      values bound to any variables.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Depends on statement.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Exec(Dbi_Handle *handle, Dbi_Value *values, int maxRows)
{
    Handle     *handlePtr = (Handle *) handle;
    Statement  *stmtPtr   = handlePtr->stmtPtr;
    Pool       *poolPtr   = handlePtr->poolPtr;

    assert(stmtPtr);
    assert(stmtPtr->numVars == 0
           || (stmtPtr->numVars > 0 && values != NULL));

    Log(handle, Debug, "Dbi_ExecProc: id: %u, variables: %u",
        stmtPtr->id, stmtPtr->numVars);

    handlePtr->maxRows = maxRows > -1 ? maxRows : poolPtr->maxRows;

    if ((*poolPtr->execProc)(handle, (Dbi_Statement *) stmtPtr,
                             values, stmtPtr->numVars) != NS_OK) {
        return NS_ERROR;
    }
    handlePtr->fetchingRows = NS_TRUE;
    handlePtr->stats.queries++;
    stmtPtr->nqueries++;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ExecDirect --
 *
 *      Prepare and execute an SQL statement without binding any values.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ExecDirect(Dbi_Handle *handle, CONST char *sql)
{
    if (Dbi_Prepare(handle, sql, -1) != NS_OK) {
        return NS_ERROR;
    }
    if (Dbi_NumVariables(handle) > 0) {
        Dbi_SetException(handle, "HY000",
            "bug: Dbi_ExecDirect: statement requires bind variables");
        return NS_ERROR;
    }
    return Dbi_Exec(handle, NULL, -1);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_NextRow --
 *
 *      Fetch the next row of the result set and make it current. Rows
 *      proceed monotonically.
 *
 *      To be called repeatedly until *endPtr is set to 1.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      The current row counter is advanced.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_NextRow(Dbi_Handle *handle, int *endPtr)
{
    Handle        *handlePtr = (Handle *) handle;
    Pool          *poolPtr   = handlePtr->poolPtr;
    Dbi_Statement *stmt      = (Dbi_Statement *) handlePtr->stmtPtr;
    int            end, maxRows, status;

    assert(stmt);
    assert(endPtr);

    if (!handlePtr->fetchingRows) {
        Dbi_SetException(handle, "HY000",
            "bug: Dbi_NextRow: no pending rows");
        return NS_ERROR;
    }

    handlePtr->nextRow++;
    handlePtr->rowIdx = handlePtr->nextRow - 1;

    Log(handle, Debug, "Dbi_NextRowProc: id: %u, row: %u",
        stmt->id, handlePtr->rowIdx);

    end = 0;
    status = (*poolPtr->nextRowProc)(handle, stmt, &end);
    *endPtr = end;

    if (status != NS_OK || end) {
        handlePtr->fetchingRows = NS_FALSE;
    }

    maxRows = handlePtr->maxRows;
    if (!end && handlePtr->rowIdx + 1 > maxRows) {
        Dbi_SetException(handle, "HY000",
            "query returned more than %d row%s",
            maxRows, maxRows > 1 ? "s" : "");
        return NS_ERROR;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ColumnLength --
 *
 *      Fetch the indicated column value length for the current row.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ColumnLength(Dbi_Handle *handle, unsigned int index,
                 size_t *lengthPtr, int *binaryPtr)
{
    Handle        *handlePtr = (Handle *) handle;
    Pool          *poolPtr   = handlePtr->poolPtr;
    Dbi_Statement *stmt      = (Dbi_Statement *) handlePtr->stmtPtr;

    if (!handlePtr->fetchingRows) {
        Dbi_SetException(handle, "HY000",
            "bug: Dbi_ColumnLength: no pending rows");
        return NS_ERROR;
    }

    if (index > handlePtr->stmtPtr->numCols) {
        Dbi_SetException(handle, "HY000",
            "bug: Dbi_ColumnLength: column index out of range: %u", index);
        return NS_ERROR;
    }

    Log(handle, Debug, "Dbi_ColumnLengthProc: id: %u, column: %u, row: %u",
        stmt->id, index, handlePtr->rowIdx);

    return (*poolPtr->columnLengthProc)(handle, stmt, index,
                                        lengthPtr, binaryPtr);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ColumnValue --
 *
 *      Fetch length bytes from the indicated column value for the
 *      current row. The bytes are written into value, which must be
 *      the correct length.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ColumnValue(Dbi_Handle *handle, unsigned int index,
                char *value, size_t length)
{
    Handle        *handlePtr = (Handle *) handle;
    Pool          *poolPtr   = handlePtr->poolPtr;
    Dbi_Statement *stmt      = (Dbi_Statement *) handlePtr->stmtPtr;

    if (!handlePtr->fetchingRows) {
        Dbi_SetException(handle, "HY000",
            "bug: Dbi_ColumnValue: no pending rows");
        return NS_ERROR;
    }

    if (index > handlePtr->stmtPtr->numCols) {
        Dbi_SetException(handle, "HY000",
            "bug: Dbi_ColumnValue: column index out of range: %u", index);
        return NS_ERROR;
    }

    Log(handle, Debug, "Dbi_ColumnValueProc: id: %u, column: %u, row: %u, length: %u",
        stmt->id, index, handlePtr->rowIdx, (unsigned int) length);

    return (*poolPtr->columnValueProc)(handle, stmt, index, value, length);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Begin --
 *
 *      Begin a new transaction or establish a new save point if
 *      a transaction is already in progress.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Begin(Dbi_Handle *handle, Dbi_Isolation isolation)
{
    Handle *handlePtr = (Handle *) handle;
    Pool   *poolPtr   = handlePtr->poolPtr;
    int     status;

    if (handlePtr->transDepth++ == 0) {
        handlePtr->isolation = isolation;
    } else if (handlePtr->isolation > isolation) {
        Dbi_SetException(handle, "HY000",
            "Transaction already in progress, cannot increase the isolation level.");
        return NS_ERROR;
    }

    Log(handle, Debug, "Dbi_TransactionProc: Dbi_TransactionBegin: depth: %d, isolation: %d",
        handlePtr->transDepth, isolation);

    status = (*poolPtr->transProc)(handle,
                                   (unsigned int) handlePtr->transDepth,
                                   Dbi_TransactionBegin, isolation);

    if (status != NS_OK) {
        handlePtr->transDepth--;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Commit --
 *
 *      Commit the active transaction or up to the most recent
 *      savepoint of the active transaction.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Commit(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;
    Pool   *poolPtr   = handlePtr->poolPtr;
    int     status;

    if (handlePtr->transDepth == -1) {
        Dbi_SetException(handle, "HY000",
                         "No transaction in progress to commit.");
        return NS_ERROR;
    }

    Log(handle, Debug, "Dbi_TransactionProc: Dbi_TransactionCommit: depth: %d",
        handlePtr->transDepth);

    status = (*poolPtr->transProc)(handle,
                                   (unsigned int) handlePtr->transDepth,
                                   Dbi_TransactionCommit, handlePtr->isolation);
    handlePtr->transDepth--;

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Rollback --
 *
 *      Rollback the active transaction or the most recent savepoint
 *      within the active transaction.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Work done by the database will be undone.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Rollback(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;
    Pool   *poolPtr   = handlePtr->poolPtr;
    int     status;

    if (handlePtr->transDepth == -1) {
        Dbi_SetException(handle, "HY000",
                         "No transaction in progress to rollback.");
        return NS_ERROR;
    }

    Log(handle, Debug, "Dbi_TransactionProc: Dbi_TransactionRollback: depth: %d",
        handlePtr->transDepth);

    status = (*poolPtr->transProc)(handle,
                                   (unsigned int) handlePtr->transDepth,
                                   Dbi_TransactionRollback, handlePtr->isolation);
    handlePtr->transDepth--;

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Flush --
 *
 *      Ready the handle for new queries by flushing any pending rows
 *      and resetting the exception code.
 *
 * Results:
 *      None.
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
    Handle        *handlePtr = (Handle *) handle;
    Pool          *poolPtr   = handlePtr->poolPtr;
    Dbi_Statement *stmt      = (Dbi_Statement *) handlePtr->stmtPtr;

    if (stmt != NULL) {
        Log(handle, Debug, "Dbi_FlushProc: id: %u, nqueries: %u",
            stmt->id, stmt->nqueries);

        (*poolPtr->flushProc)(handle, stmt);

        handlePtr->fetchingRows = NS_FALSE;
        handlePtr->rowIdx = handlePtr->nextRow = 0;
    }
    Dbi_ResetException(handle);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Reset --
 *
 *      Reset a handle to it's default state, closing any open
 *      transactions.
 *
 * Results:
 *      NS_OK if handle reset, NS_ERROR if reset failed and handle
 *      should be returned to pool, e.g. because the connection died.
 *
 * Side effects:
 *      Calls Dbi_Flush.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Reset(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;
    Pool   *poolPtr   = handlePtr->poolPtr;
    int     status;

    Dbi_Flush(handle);

    Log(handle, Debug, "Dbi_ResetProc: nqueries: %u",
        handlePtr->stats.queries);

    status = (*poolPtr->resetProc)(handle);

    if (Dbi_ExceptionPending(handle)) {
        Dbi_LogException(handle, Error);
    }
    handlePtr->stmtPtr = NULL;

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

    Ns_MutexLock(&poolPtr->lock);
    CheckPool(poolPtr, 1);
    Ns_CondBroadcast(&poolPtr->cond);
    Ns_MutexUnlock(&poolPtr->lock);
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
                     poolPtr->stats.querycloses, poolPtr->epoch);
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
    return ((Pool *) pool)->drivername;
}

CONST char *
Dbi_DatabaseName(Dbi_Pool *pool)
{
    return ((Pool *) pool)->database;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Config --
 *
 *      Return the current value of the configuration option and if
 *      newValue is >= 0, update the config.
 *
 * Results:
 *      The old value of the config option.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Config(Dbi_Pool *pool, DBI_CONFIG_OPTION opt, int newValue)
{
    Pool *poolPtr = (Pool *) pool;
    int   oldValue;

    Ns_MutexLock(&poolPtr->lock);
    switch (opt) {
    case DBI_CONFIG_MAXHANDLES:
        oldValue = poolPtr->maxhandles;
        if (newValue >= 0) {
            poolPtr->maxhandles = newValue;
        }
        break;
    case DBI_CONFIG_MAXROWS:
        oldValue = poolPtr->maxRows;
        if (newValue >= 0) {
            poolPtr->maxRows = newValue;
        }
        break;
    case DBI_CONFIG_MAXIDLE:
        oldValue = poolPtr->maxidle;
        if (newValue >= 0) {
            poolPtr->maxidle = newValue;
        }
        break;
    case DBI_CONFIG_MAXOPEN:
        oldValue = poolPtr->maxopen;
        if (newValue >= 0) {
            poolPtr->maxopen = newValue;
        }
        break;
    case DBI_CONFIG_MAXQUERIES:
        oldValue = poolPtr->maxqueries;
        if (newValue >= 0) {
            poolPtr->maxqueries = newValue;
        }
        break;
    case DBI_CONFIG_TIMEOUT:
        oldValue = poolPtr->timeout;
        if (newValue >= 0) {
            poolPtr->timeout = newValue;
        }
        break;
    default:
	oldValue = -1;
    }
    Ns_MutexUnlock(&poolPtr->lock);

    return oldValue;
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

    strncpy(handlePtr->cExceptionCode, sqlstate, 6);
    handlePtr->cExceptionCode[5] = '\0';

    if (fmt != NULL) {
	int len;

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
 * Dbi_LogException --
 *
 *      Log the current handle exception.
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
Dbi_LogException(Dbi_Handle *handle, Ns_LogSeverity severity)
{
    char *code, *msg;

    code = Dbi_ExceptionCode(handle);
    if (code[0] == '\0') {
        code = "(no code)";
    }
    msg = Dbi_ExceptionMsg(handle);
    if (msg == NULL) {
        msg = "(no message)";
    }
    Log(handle, severity, "%s: %s", code, msg);
    Dbi_ResetException(handle);
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

    if (poolPtr->stopping
        || poolPtr->nhandles > poolPtr->maxhandles) {

        Ns_CacheDestroy(handlePtr->cache);
        Ns_DStringFree(&handlePtr->dsExceptionMsg);
        ns_free(handlePtr);
        poolPtr->nhandles--;

        return;
    }

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
    poolPtr->idlehandles++;
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
 *      NB: Pool must be locked or handle otherwise protected.
 *
 *----------------------------------------------------------------------
 */

static int
CloseIfStale(Handle *handlePtr, time_t now)
{
    Dbi_Handle *handle  = (Dbi_Handle *) handlePtr;
    Pool       *poolPtr = handlePtr->poolPtr;

    if (Connected(handlePtr)) {
	char *reason  = NULL;

        if (poolPtr->stopping) {
            reason = "stopped";
        } else if (poolPtr->epoch > handlePtr->epoch) {
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

            (void) Ns_CacheFlush(handlePtr->cache);

            Log(handle, Notice, "closing %s handle, %d queries",
                reason, handlePtr->stats.queries);

            (*poolPtr->closeProc)(handle);

            handlePtr->driverData = NULL;
            handlePtr->atime = handlePtr->otime = 0;
            poolPtr->stats.queries += handlePtr->stats.queries;
            handlePtr->stats.queries = 0;

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
    Handle  *handlePtr, *nextPtr;
    time_t   now;

    if (stale) {
        poolPtr->epoch++;
    }
    handlePtr = poolPtr->firstPtr;
    poolPtr->firstPtr = poolPtr->lastPtr = NULL;
    poolPtr->idlehandles = 0;

    time(&now);

    while (handlePtr != NULL) {
        nextPtr = handlePtr->nextPtr;
        (void) CloseIfStale(handlePtr, now);
        ReturnHandle(handlePtr);
        handlePtr = nextPtr;
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

    Ns_MutexLock(&poolPtr->lock);
    CheckPool(poolPtr, 0);
    Ns_CondBroadcast(&poolPtr->cond);
    Ns_MutexUnlock(&poolPtr->lock);
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
        Ns_CondBroadcast(&poolPtr->cond);
        Ns_MutexUnlock(&poolPtr->lock);
    } else {
	int status;

        Ns_DStringInit(&ds);
        Ns_Log(Notice, "dbi[%s:%s]: %s", poolPtr->drivername, poolPtr->module,
               Dbi_Stats(&ds, (Dbi_Pool *) poolPtr));
        Ns_DStringFree(&ds);

        Ns_MutexLock (&poolPtr->lock);
        do {
            status = NS_OK;
            while (status == NS_OK
                   && poolPtr->nhandles > 0
                   && poolPtr->firstPtr == NULL) {
                status = Ns_CondTimedWait(&poolPtr->cond, &poolPtr->lock, toPtr);
            }
            if (poolPtr->firstPtr != NULL) {
                CheckPool(poolPtr, 1);
            }
        } while (poolPtr->nhandles > 0 && status == NS_OK);

        Ns_MutexUnlock(&poolPtr->lock);
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
    int         status  = NS_ERROR;

    if (!poolPtr->stopping) {

        Log(handle, Debug, "Dbi_OpenProc: opens: %u",
            poolPtr->stats.handleopens);

        status = (*poolPtr->openProc)(poolPtr->configData, handle);
        poolPtr->stats.handleopens++;

        if (status != NS_OK) {
            poolPtr->stats.handlefailures++;
            Dbi_LogException(handle, Error);
        } else {
	    char  *msg;

            handlePtr->atime = handlePtr->otime = time(NULL);
            msg = Dbi_ExceptionMsg(handle);
            Log(handle, Notice, "opened handle %d/%d%s%s",
                handlePtr->n, poolPtr->maxhandles,
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
    Dbi_Handle *handle  = (Dbi_Handle *) handlePtr;
    Pool       *poolPtr = handlePtr->poolPtr;

    return (*poolPtr->connectedProc)(handle);
}


/*
 *----------------------------------------------------------------------
 *
 * FreeThreadHandles --
 *
 *      Free cached handles on thread exit.
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
FreeThreadHandles(void *arg)
{
    Handle *nextPtr, *handlePtr = arg;
    Pool   *poolPtr;

    if (handlePtr != NULL) {
        poolPtr = handlePtr->poolPtr;
        Ns_MutexLock(&poolPtr->lock);
        while (handlePtr != NULL) {
            nextPtr = handlePtr->nextPtr;
            ReturnHandle(handlePtr);
            handlePtr = nextPtr;
        }
        Ns_CondBroadcast(&poolPtr->cond);
        Ns_MutexUnlock(&poolPtr->lock);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * FreeStatement --
 *
 *      Cache eviction callback to free a prepared statement.
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
    Pool       *poolPtr;

    if (stmtPtr->driverData != NULL) {
	Dbi_Handle *handle = (Dbi_Handle *) stmtPtr->handlePtr;

        poolPtr = stmtPtr->handlePtr->poolPtr;

        Log(stmtPtr->handlePtr, Debug,
            "Dbi_PrepareCloseProc(FreeStatement): nqueries: %u",
            stmtPtr->nqueries);

        (*poolPtr->prepareCloseProc)(handle, (Dbi_Statement *) stmtPtr);
    }
    Tcl_DeleteHashTable(&stmtPtr->bindTable);
    ns_free(stmtPtr);
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
     * for driver notation which may (but is unlikely to) be
     * larger than the original. Check for overrun at the end.
     */

    if (origLength < 0) {
        origLength = strlen(origSql);
    }
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
        stmtPtr->id = handlePtr->stmtid++;
        strncpy(stmtPtr->driverSql, Ns_DStringValue(&ds), Ns_DStringLength(&ds));
        stmtPtr->sql = stmtPtr->driverSql;
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
    Pool          *poolPtr = stmtPtr->handlePtr->poolPtr;
    Tcl_HashEntry *hPtr;
    int            new, index;

    index = (int) stmtPtr->numVars;
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
        Tcl_SetHashValue(hPtr, (ClientData)(intptr_t) index);
    }
    stmtPtr->vars[index].name = Tcl_GetHashKey(&stmtPtr->bindTable, hPtr);
    stmtPtr->numVars++;

    (*poolPtr->bindVarProc)(dsPtr, name, index);

    return NS_OK;
}
