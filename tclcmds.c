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
 * tclcmds.c --
 *
 *      Tcl database access routines.
 */

#include "dbi.h"


/*
 * The following structure maintains per-interp data.
 */

typedef struct InterpData {
    char          *server;
    int            cleanup;
    Tcl_HashTable  handles;
    Tcl_Interp    *interp;
} InterpData;

/*
 * Static functions defined in this file
 */

static Dbi_Pool* GetPool(InterpData *idataPtr, char *pool);
static Dbi_Handle *GetHandle(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[]);
static void ReleaseHandle(InterpData *idataPtr, Dbi_Handle *handle);
static void ReleaseAllHandles(Tcl_Interp *interp, void *arg);
static int SqlException(InterpData *idataPtr, Dbi_Handle *handle);
static int Exception(Tcl_Interp *interp, char *code, char *info, char *msg, ...);
static int SingleRowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[], int req);

static Ns_TclDeferProc ReleaseAllHandles;
static Tcl_InterpDeleteProc FreeData;

static Tcl_ObjCmdProc
    Tcl0or1rowCmd, Tcl1rowCmd, TclRowsCmd, TclDmlCmd, TclReleasehandlesCmd,
    TclPoolCmd, TclPoolsCmd, TclDefaultpoolCmd, TclBouncepoolCmd;


/*
 * Static variables defined in this file.
 */

static struct Cmd {
    char           *name;
    Tcl_ObjCmdProc *objProc;
} cmds[] = {
    {"dbi_0or1row",        Tcl0or1rowCmd},
    {"dbi_1row",           Tcl1rowCmd},
    {"dbi_rows",           TclRowsCmd},
    {"dbi_dml",            TclDmlCmd},
    {"dbi_releasehandles", TclReleasehandlesCmd},
    {"dbi_pool",           TclPoolCmd},
    {"dbi_pools",          TclPoolsCmd},
    {"dbi_defaultpool",    TclDefaultpoolCmd},
    {"dbi_bouncepool",     TclBouncepoolCmd},
    {NULL, NULL}
};

static char *datakey = "dbi:data";



/*
 *----------------------------------------------------------------------
 *
 * DbiAddCmds --
 *
 *      Add the dbi commands.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
DbiAddCmds(Tcl_Interp *interp, void *arg)
{
    InterpData *idataPtr;
    int i;

    idataPtr = ns_malloc(sizeof(InterpData));
    idataPtr->server  = arg;
    idataPtr->cleanup = 0;
    idataPtr->interp  = interp;
    Tcl_InitHashTable(&idataPtr->handles, TCL_STRING_KEYS);
    Tcl_SetAssocData(interp, datakey, FreeData, idataPtr);

    for (i = 0; cmds[i].name != NULL; ++i) {
        Tcl_CreateObjCommand(interp, cmds[i].name, cmds[i].objProc, idataPtr, NULL);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Tcl0or1rowCmd --
 *
 *      Perform a query which may return only one row and set the
 *      result as Tcl variables. Return 1 if a row was returned, 0
 *      otherwise.
 *
 * Tcl1rowCmd --
 *
 *      Perform a query returning exactly one row and set the result
 *      as Tcl variables.  Throw an error if no rows are returned.
 *
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Tcl0or1rowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    return SingleRowCmd(clientData, interp, objc, objv, 0);
}

static int
Tcl1rowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    return SingleRowCmd(clientData, interp, objc, objv, 1);
}

static int
SingleRowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[], int req)
{
    InterpData *idataPtr = clientData;
    Dbi_Handle *handle;
    char       *column, *value;
    int         cLen, vLen, nrows, status, ignore;
    Tcl_Obj    *colObjPtr, *valObjPtr;

    if (objc < 2 || objc > 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? ?-timeout timeout? sql");
        return TCL_ERROR;
    }
    handle = GetHandle(idataPtr, objc, objv);
    if (handle == NULL) {
        return TCL_ERROR;
    }
    if (Dbi_0or1Row(handle, Tcl_GetString(objv[objc - 1]), &nrows, &ignore) != NS_OK) {
        return SqlException(idataPtr, handle);
    }
    if (nrows == 0) {
        if (req == 1) {
            Exception(interp, NULL, NULL, "Query was not a statement returning rows.");
            ReleaseHandle(idataPtr, handle);
            return TCL_ERROR;
        } else {
            Tcl_SetObjResult(interp, Tcl_NewIntObj(0));
            ReleaseHandle(idataPtr, handle);
            return TCL_OK;
        }
    }
    do {
        if ((status = Dbi_NextValue(handle, &value, &vLen, &column, &cLen)) == NS_ERROR) {
            break;
        }
        colObjPtr = Tcl_NewStringObj(column, cLen);
        valObjPtr = Tcl_NewStringObj(value, vLen);
        if (Tcl_ObjSetVar2(interp, colObjPtr, NULL, valObjPtr, TCL_LEAVE_ERR_MSG) == NULL) {
            status = NS_ERROR;
            break;
        }
    } while (status != DBI_END_DATA && status != NS_ERROR);

    if (status != DBI_END_DATA) {
        return SqlException(idataPtr, handle);
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(1));
    ReleaseHandle(idataPtr, handle);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclBouncepoolCmd --
 *
 *      Implements dbi_bouncepool command.
 *      See: Dbi_BouncePool.
 *
 * Results:
 *      Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
TclBouncepoolCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    char       *pool;
    Dbi_Pool   *poolPtr;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "pool");
        return TCL_ERROR;
    }

    pool = Tcl_GetString(objv[1]);
    if ((poolPtr = GetPool(idataPtr, pool)) == NULL) {
        return TCL_ERROR;
    }
    if (Dbi_BouncePool(poolPtr) == NS_ERROR) {
        return Exception(interp, NULL, NULL, "could not bounce pool: %s", pool);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclDmlCmd --
 *
 *      Implements dbi_dml command.
 *
 * Results:
 *      Tcl result.
 *
 * Side effects:
 *      Handle may be cached.
 *
 *----------------------------------------------------------------------
 */

static int
TclDmlCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    Dbi_Handle *handle;
    int         nrows, ignore;

    if (objc < 2 || objc > 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? ?-timeout timeout? sql");
        return TCL_ERROR;
    }
    handle = GetHandle(idataPtr, objc, objv);
    if (handle == NULL) {
        return TCL_ERROR;
    }
    if (Dbi_DML(handle, Tcl_GetString(objv[objc - 1]), &nrows, &ignore) != NS_OK) {
        return SqlException(idataPtr, handle);
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(nrows));
    ReleaseHandle(idataPtr, handle);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclPoolCmd --
 *
 *      Implements the dbi_pool command.
 *
 * Results:
 *      Depends on sub-command.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
TclPoolCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    char       *pool     = NULL;
    Dbi_Pool   *poolPtr;

    static CONST char *opts[] = {
        "datasource", "dbtype", "description", "driver",
        "nhandles", "password", "user", NULL
    };

    enum IPoolIdx {
        IDatasourceIdx, IDbtypeIdx, IDescriptionIdx, IDriverIdx,
        INhandlesIdx, IPasswordIdx, IUserIdx
    } opt;

    if (objc != 2 && objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "option ?pool?");
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[1], opts, "option", 0, (int *) &opt)
        != TCL_OK) {
        return TCL_ERROR;
    }

    if (objc == 3) {
        pool = Tcl_GetString(objv[2]);
    }
    if ((poolPtr = GetPool(idataPtr, pool)) == NULL) {
        return TCL_ERROR;
    }

    switch (opt) {

    case IDatasourceIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->datasource, -1));
        break;

    case IDbtypeIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolDbType(poolPtr), -1));
        break;

    case IDescriptionIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->description, -1));
        break;

    case IDriverIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(((Pool *)poolPtr)->driverPtr->name, -1));
        break;

    case INhandlesIdx:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(poolPtr->nhandles));
        break;

    case IPasswordIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->password, -1));
        break;

    case IUserIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->user, -1));
        break;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclPoolsCmd --
 *
 *      Implements dbi_pools command.
 *      Return a list of pools available to this server.
 *
 * Results:
 *      Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
TclPoolsCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    Tcl_Obj    *result;
    Ns_DString  ds;
    char       *p, *q;

    Ns_DStringInit(&ds);
    if (Dbi_PoolList(&ds, idataPtr->server) != NS_OK) {
        return TCL_ERROR;
    }
    result = Tcl_GetObjResult(interp);
    p = Ns_DStringValue(&ds);
    q = p;
    while ((p = strchr(Ns_DStringValue(&ds), ' ')) != NULL) {
        *p++ = '\0';
        Tcl_ListObjAppendElement(interp, result, Tcl_NewStringObj(q, -1));
        q = p;
    }
    /* last element of list... */
    Tcl_ListObjAppendElement(interp, result, Tcl_NewStringObj(q, -1));
    Ns_DStringFree(&ds);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclDefaultpoolCmd --
 *
 *      Implements dbi_defaultpool.
 *      Return the name of the default pool for this server, if any.
 *
 * Results:
 *      Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
TclDefaultpoolCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    Dbi_Pool *poolPtr;

    poolPtr = Dbi_PoolDefault(idataPtr->server);
    if (poolPtr != NULL) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->name, -1));
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclRowsCmd --
 *
 *      Implements dbi_rows command.
 *      Append the value of each column as a list in the Tcl result.
 *
 * Results:
 *      Tcl result.
 *
 * Side effects:
 *      Handle may be cached.
 *
 *----------------------------------------------------------------------
 */

static int
TclRowsCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    Dbi_Handle  *handle;
    char        *value;
    int          len, status, nrows, ncols;
    Tcl_Obj     *result;
    

    if (objc < 2 || objc > 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? -timeout timeout? sql");
        return TCL_ERROR;
    }
    handle = GetHandle(idataPtr, objc, objv);
    if (handle == NULL) {
        return TCL_ERROR;
    }
    if (Dbi_Select(handle, Tcl_GetString(objv[objc - 1]), &nrows, &ncols) != NS_OK) {
        return SqlException(idataPtr, handle);
    }
    if (nrows == 0) {
        ReleaseHandle(idataPtr, handle);
        return TCL_OK;
    }
    result = Tcl_GetObjResult(interp);
    do {
        if ((status = Dbi_NextValue(handle, &value, &len, NULL, NULL)) == NS_ERROR) {
            break;
        }
        Tcl_ListObjAppendElement(interp, result, Tcl_NewStringObj(value, len));
    } while (status != DBI_END_DATA && status != NS_ERROR);

    if (status != DBI_END_DATA) {
        return SqlException(idataPtr, handle);
    }
    ReleaseHandle(idataPtr, handle);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclReleasehandlesCmd --
 *
 *      Implements dbi_releasehandles command.
 *      Release all handles currently cached for this interpreter.
 *
 * Results:
 *      Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
TclReleasehandlesCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;

    ReleaseAllHandles(interp, idataPtr);
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 * GetHandle --
 *
 *      Common function to get a database handle for commands which need
 *      one.  Use server default pool if none specified.
 *
 * Results:
 *      Return pointer to handle or NULL if timeout or other error.
 *
 * Side effects:
 *      Tcl error result may be set on error.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Handle*
GetHandle(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[])
{
    Tcl_Interp    *interp = idataPtr->interp;
    Dbi_Handle    *handlePtr;
    Dbi_Pool      *poolPtr;
    Tcl_HashEntry *hPtr;
    char          *pool;
    int            i, timeout, new;

    static CONST char *opts[] = {"-pool", "-timeout", NULL};
    enum IHandleOptsIdx {IPoolIdx, ITimeoutIdx} opt;

    /*
     * Parse any options and determine pool and timeout.
     */

    timeout = -1;
    pool = NULL;
    poolPtr = NULL;

    for (i = 1; i < objc - 1; i += 2) {
        if (Tcl_GetIndexFromObj(interp, objv[i], opts, "option", 0, (int*) &opt) != TCL_OK) {
            return NULL;
        }
        switch (opt) {
        case IPoolIdx:
            pool = Tcl_GetString(objv[i+1]);
            break;
        case ITimeoutIdx:
            if (Tcl_GetIntFromObj(interp, objv[i+1], &timeout) != TCL_OK) {
                Exception(interp, NULL, NULL, "Invalid timeout value: %s", Tcl_GetString(objv[i+1]));
                return NULL;
            }
            break;
        }
    }

    /*
     * Check for a cached handle from the desired pool for this interp.
     */

    if (pool == NULL) {
        poolPtr = GetPool(idataPtr, pool);
        if (poolPtr == NULL) {
            return NULL;
        } else {
            pool = poolPtr->name;
        }
    }
    hPtr = Tcl_FindHashEntry(&idataPtr->handles, pool);
    if (hPtr != NULL) {
        handlePtr = (Dbi_Handle*) Tcl_GetHashValue(hPtr);
        return handlePtr;
    }

    /*
     * Get a fresh handle straight from the pool.
     */

    if (poolPtr == NULL) {
        poolPtr = GetPool(idataPtr, pool);
        if (poolPtr == NULL) {
            return NULL;
        }
    }
    handlePtr = NULL;
    switch (Dbi_PoolTimedGetHandle(&handlePtr, poolPtr, timeout)) {
    case NS_TIMEOUT:
        Exception(interp, "TIMEOUT", NULL, "wait for database handle timed out");
        break;
    case NS_ERROR:
        Exception(interp, NULL, NULL, "database handle allocation failed");
        break;
    }

    /*
     * Cache the handle for future use by this interp
     */

    if (((Pool *)poolPtr)->cache_handles) {
        hPtr = Tcl_CreateHashEntry(&idataPtr->handles, pool, &new);
        Tcl_SetHashValue(hPtr, handlePtr);
        if (!idataPtr->cleanup) {
            Ns_TclRegisterDeferred(interp, ReleaseAllHandles, idataPtr);
            idataPtr->cleanup = 1;
        }
    }

    return handlePtr;
}


/*
 *----------------------------------------------------------------------
 * ReleaseHandle --
 *
 *      Release a handle back to it's pool so it can be reused by
 *      other interps unless handle caching is enabled for this pool.
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
ReleaseHandle(InterpData *idataPtr, Dbi_Handle *handle)
{
    Pool *poolPtr = (Pool *) handle->poolPtr;

    if (poolPtr->cache_handles) {
        Dbi_Flush(handle);
        Dbi_ResetHandle(handle);
    } else {
        Dbi_PoolPutHandle(handle);
    }
}


/*
 *----------------------------------------------------------------------
 * ReleaseAllHandles --
 *
 *      Release any database handles owned by this interp.
 *      Called by AOLserver on interp cleanup.
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
ReleaseAllHandles(Tcl_Interp *interp, void *arg)
{
    InterpData     *idataPtr = arg;
    Dbi_Handle     *handle;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;
    int i = 0;

    hPtr = Tcl_FirstHashEntry(&idataPtr->handles, &search);
    while (hPtr != NULL) {
        i++;
    	handle = Tcl_GetHashValue(hPtr);
   	Dbi_PoolPutHandle(handle);
    	hPtr = Tcl_NextHashEntry(&search);
    }
    Tcl_DeleteHashTable(&idataPtr->handles);
    Tcl_InitHashTable(&idataPtr->handles, TCL_STRING_KEYS);
}


/*
 *----------------------------------------------------------------------
 * GetPool --
 *
 *      Grab a Dbi_Pool, handling defaults, permissions and non
 *      existant pools.
 *
 * Results:
 *      Return pointer to Dbi_Pool or NULL on error.
 *
 * Side effects:
 *      Tcl error result may be left in interp.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Pool*
GetPool(InterpData *idataPtr, char *pool)
{
    Tcl_Interp *interp = idataPtr->interp;
    Dbi_Pool *poolPtr;

    if (pool == NULL || *pool == '\0') {
        poolPtr = Dbi_PoolDefault(idataPtr->server);
        if (poolPtr == NULL) {
            Exception(interp, NULL, NULL, "no pool specified and no default configured");
            return NULL;
        }
    } else {
        poolPtr = Dbi_GetPool(idataPtr->server, pool);
        if (poolPtr == NULL) {
            Exception(interp, NULL, NULL, "pool '%s' not valid or not available for server '%s'",
                      pool, idataPtr->server);
            return NULL;
        }
    }

    return poolPtr;
}


/*
 *----------------------------------------------------------------------
 * SqlException --
 *
 *      Set current handle exception code and message as Tcl exception.
 *
 * Results:
 *      Always TCL_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
SqlException(InterpData *idataPtr, Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;
    char   *code      = NULL;
    char   *message   = "No exception message available.";

    if (handlePtr->cExceptionCode[0] != '\0') {
        code = handlePtr->cExceptionCode;
    }
    if (handlePtr->dsExceptionMsg.length > 0) {
        message = Ns_DStringValue(&handlePtr->dsExceptionMsg);
    }
    Exception(idataPtr->interp, code, NULL, message);
    if (handle != NULL) {
        ReleaseHandle(idataPtr, handle);
    }

    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 * Exception --
 *
 *      Set a Tcl exception with optional code and message.
 *
 * Results:
 *      Always TCL_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Exception(Tcl_Interp *interp, char *code, char *info, char *msg, ...)
{
    Tcl_Obj *objPtr;
    Ns_DString ds;
    va_list ap;

    objPtr = Tcl_NewStringObj("DBI", 3);
    if (code != NULL) {
        Tcl_ListObjAppendElement(interp, objPtr, Tcl_NewStringObj(code, -1));
    }
    Tcl_SetObjErrorCode(interp, objPtr);

    if (info != NULL) {
        Tcl_AddObjErrorInfo(interp, info, -1);
    }
    Ns_DStringInit(&ds);
    va_start(ap, msg);
    Ns_DStringVPrintf(&ds, msg, ap);
    va_end(ap);
    Tcl_DStringResult(interp, &ds);
    Ns_DStringFree(&ds);

    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 * FreeData --
 *
 *      Free per-interp data at interp delete time.
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
FreeData(ClientData arg, Tcl_Interp *interp)
{
    InterpData *idataPtr = arg;

    ReleaseAllHandles(interp, idataPtr);
    Tcl_DeleteHashTable(&idataPtr->handles);
    ns_free(idataPtr);
}
