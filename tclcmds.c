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

NS_RCSID("@(#) $Header$");


/*
 * The following structure maintains per-interp data.
 */

typedef struct InterpData {
    const char    *server;
    int            cleanup;
    Tcl_HashTable  handles;
    Tcl_Interp    *interp;
} InterpData;


/*
 * Static functions defined in this file
 */

static int ParseOptions(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[],
                        Dbi_Handle **handlePtrPtr, Dbi_Statement **stmtPtrPtr) _nsnonnull();
static Dbi_Handle* GetHandle(InterpData *idataPtr, const char *pool, int timeout) _nsnonnull();
static Dbi_Pool* GetPool(InterpData *idataPtr, const char *pool) _nsnonnull(1);
static Dbi_Statement *BindVars(Tcl_Interp *interp, Dbi_Pool *pool, Tcl_Obj *dictObjPtr, Tcl_Obj *sqlObjPtr) _nsnonnull();
static char *GetVar(Tcl_Interp *interp, Tcl_Obj *dictObjPtr, char *name, int *len) _nsnonnull();
static int DictRowResult(Tcl_Interp *interp, Dbi_Handle *handle, Dbi_Statement *stmt) _nsnonnull();
static void ReleaseHandle(Dbi_Handle *handle) _nsnonnull();
static void ReleaseAllHandles(Tcl_Interp *interp, void *arg) _nsnonnull(1);
static int Exception(Tcl_Interp *interp, const char *code, const char *msg, ...)
     _nsprintflike(3, 4) _nsnonnull(1);
static int SqlException(Tcl_Interp *interp, Dbi_Handle *handle) _nsnonnull();

static Ns_TclDeferProc ReleaseAllHandles;
static Tcl_InterpDeleteProc FreeData;

static Tcl_ObjCmdProc
    Tcl0or1rowCmd, Tcl1rowCmd, TclRowsCmd, TclDmlCmd, TclReleasehandlesCmd,
    TclPoolCmd, TclPoolsCmd, TclDefaultpoolCmd, TclBouncepoolCmd;


/*
 * Static variables defined in this file.
 */

static char *datakey = "dbi:data";

/*
 * The dbi Tcl commands.
 */

static struct Cmd {
    char           *name;
    Tcl_ObjCmdProc *objProc;
} const cmds[] = {
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

/*
 * The following list of potentialy blocking commands are
 * commonly called without first releasing db handles.
 */

static char *blockingCmds[] = {
    "ns_return", "ns_respond", "ns_returnfile", "ns_returnfp",
    "ns_returnbadrequest", "ns_returnerror", "ns_returnnotice",
    "ns_returnadminnotice", "ns_returnredirect", "ns_returnforbidden",
    "ns_returnunauthorized", "ns_returnnotfound", "ns_write",
    "ns_connsendfp", NULL
};



/*
 *----------------------------------------------------------------------
 *
 * DbiAddCmds --
 *
 *      Add the dbi commands to an initialising interp.
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
 * DbiAddTraces --
 *
 *      Register a trace for each blocking command to release handles
 *      before continuing.
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
DbiAddTraces(Tcl_Interp *interp, void *arg _nsunused)
{
    int i;

    for (i = 0; blockingCmds[i] != NULL; ++i) {
        Tcl_VarEval(interp, "trace add execution ", blockingCmds[i],
                    " enter dbi_releasehandles", NULL);
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Tcl1rowCmd --
 *
 *      Perform a query returning exactly one row and set the result
 *      as a Tcl dictionary.  Throw an error if no rows are returned.
 *
 * Tcl0or1rowCmd --
 *
 *      Perform a query which may return only one row and set the
 *      result as a Tcl dictionary.  The dictionary will be empty if
 *      no rows were returned.
 *
 *
 * Results:
 *      TCL_OK or TCL_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Tcl1rowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData    *idataPtr = clientData;
    Dbi_Handle    *handle;
    Dbi_Statement *stmt;
    int            status = TCL_ERROR;

    if (ParseOptions(idataPtr, objc, objv, &handle, &stmt) != NS_OK) {
        return TCL_ERROR;
    }
    if (Dbi_1Row(handle, stmt, NULL) != NS_OK) {
        SqlException(interp, handle);
        goto done;
    }
    if (DictRowResult(interp, handle, stmt) != NS_OK) {
        goto done;
    }
    status = TCL_OK;
 done:
    Dbi_StatementFree(stmt);
    ReleaseHandle(handle);
    return status;
}

static int
Tcl0or1rowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData    *idataPtr = clientData;
    Dbi_Handle    *handle;
    Dbi_Statement *stmt;
    int            nrows, status = TCL_ERROR;

    if (ParseOptions(idataPtr, objc, objv, &handle, &stmt) != NS_OK) {
        return TCL_ERROR;
    }
    if (Dbi_0or1Row(handle, stmt, &nrows, NULL) != NS_OK) {
        SqlException(interp, handle);
        goto done;
    }
    if (nrows == 0) {
        Tcl_SetObjResult(interp, Tcl_NewDictObj());
        status = TCL_OK;
        goto done;
    }
    if (DictRowResult(interp, handle, stmt) != NS_OK) {
        goto done;
    }
    status = TCL_OK;
 done:
    Dbi_StatementFree(stmt);
    ReleaseHandle(handle);
    return status;
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
    InterpData    *idataPtr = clientData;
    Dbi_Handle    *handle;
    Dbi_Statement *stmt;
    const char    *value;
    int            vLen, nrows, status;
    Tcl_Obj       *listObjPtr;
    int            result = TCL_ERROR;

    if (ParseOptions(idataPtr, objc, objv, &handle, &stmt) != NS_OK) {
        return TCL_ERROR;
    }
    if (Dbi_Select(handle, stmt, &nrows, NULL) != NS_OK) {
        SqlException(interp, handle);
        goto done;
    }
    if (nrows == 0) {
        result = TCL_OK;
        goto done;
    }
    listObjPtr = Tcl_GetObjResult(interp);
    do {
        status = Dbi_NextValue(stmt, &value, &vLen, NULL, NULL);
        if (status == NS_ERROR) {
            break;
        }
        Tcl_ListObjAppendElement(interp, listObjPtr, Tcl_NewStringObj((char *) value, vLen));
    } while (status != DBI_END_DATA);

    if (status != DBI_END_DATA) {
        SqlException(interp, handle);
        goto done;
    }
    result = TCL_OK;
 done:
    Dbi_StatementFree(stmt);
    ReleaseHandle(handle);

    return result;
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
    InterpData    *idataPtr = clientData;
    Dbi_Handle    *handle;
    Dbi_Statement *stmt;
    int            nrows;
    int            result = TCL_ERROR;

    if (ParseOptions(idataPtr, objc, objv, &handle, &stmt) != NS_OK) {
        return TCL_ERROR;
    }
    if (Dbi_DML(handle, stmt, &nrows, NULL) != NS_OK) {
        SqlException(interp, handle);
        goto done;
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(nrows));
    result = TCL_OK;
 done:
    Dbi_StatementFree(stmt);
    ReleaseHandle(handle);
    return result;
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
    const char *pool;
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
        return Exception(interp, NULL, "could not bounce pool: %s", pool);
    }

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
    const char *pool     = NULL;
    Dbi_Handle *handle   = NULL;
    Dbi_Pool   *poolPtr  = NULL;

    static CONST char *opts[] = {
        "datasource", "dbtype", "description", "driver",
        "nhandles", "password", "user", NULL
    };

    enum IPoolIdx {
        IDatasourceIdx, IDbtypeIdx, IDescriptionIdx, IDriverIdx,
        INhandlesIdx, IPasswordIdx, IUserIdx
    } _nsmayalias opt;

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
    if (opt == IDbtypeIdx || opt == IDriverIdx) {
        if ((handle = GetHandle(idataPtr, pool, -1)) == NULL) {
            return TCL_ERROR;
        }
    } else {
        if ((poolPtr = GetPool(idataPtr, pool)) == NULL) {
            return TCL_ERROR;
        }
    }

    switch (opt) {

    case IDriverIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj((char *) Dbi_DriverName(handle), -1));
        ReleaseHandle(handle);
        break;

    case IDbtypeIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj((char *) Dbi_DriverDbType(handle), -1));
        ReleaseHandle(handle);
        break;

    case IDatasourceIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->datasource, -1));
        break;

    case IDescriptionIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->description, -1));
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
TclPoolsCmd(ClientData clientData, Tcl_Interp *interp, int objc _nsunused, Tcl_Obj *CONST objv[] _nsunused)
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
TclDefaultpoolCmd(ClientData clientData, Tcl_Interp *interp, int objc _nsunused, Tcl_Obj *CONST objv[] _nsunused)
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
TclReleasehandlesCmd(ClientData clientData, Tcl_Interp *interp,
                     int objc _nsunused, Tcl_Obj *CONST objv[] _nsunused)
{
    InterpData *idataPtr = clientData;

    ReleaseAllHandles(interp, idataPtr);
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 * ParseOptions --
 *
 *      Parse common command options to determine which pool to use, how
 *      long to wait when acquiring a handle, and how bind variables are
 *      specified.  Also create the statement from passed in SQL.
 *
 * Results:
 *      NS_OK if handle acquired, NS_ERROR otherwise.
 *
 * Side effects:
 *      Tcl error result may be set.
 *
 *----------------------------------------------------------------------
 */

static int
ParseOptions(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[],
             Dbi_Handle **handlePtrPtr, Dbi_Statement **stmtPtrPtr)
{
    Tcl_Interp    *interp = idataPtr->interp;
    Dbi_Handle    *handle;
    Dbi_Statement *stmt;
    Tcl_Obj       *dictObjPtr;
    const char    *poolname;
    int            i, timeout;

    static CONST char *opts[] = {"-pool", "-timeout", "-bind", NULL};
    enum IHandleOptsIdx {IPoolIdx, ITimeoutIdx, IBindIdx} _nsmayalias opt;

    if (objc < 2 || objc > 8 || objc % 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? -timeout timeout? ?-bind dict? sql");
        return TCL_ERROR;
    }

    timeout    = -1;
    poolname   = NULL;
    dictObjPtr = NULL;

    for (i = 1; i < objc - 1; i += 2) {
        if (Tcl_GetIndexFromObj(interp, objv[i], opts, "option", 0, (int*) &opt) != TCL_OK) {
            return NS_ERROR;
        }
        switch (opt) {
        case IPoolIdx:
            poolname = Tcl_GetString(objv[i+1]);
            break;
        case ITimeoutIdx:
            if (Tcl_GetIntFromObj(interp, objv[i+1], &timeout) != TCL_OK) {
                Exception(interp, NULL, "Invalid timeout value: %s", Tcl_GetString(objv[i+1]));
                return NS_ERROR;
            }
            break;
        case IBindIdx:
            dictObjPtr = objv[i+1];
            break;
        }
    }
    handle = GetHandle(idataPtr, poolname, timeout);
    if (handle == NULL) {
        return NS_ERROR;
    }
    stmt = BindVars(interp, handle->pool, dictObjPtr, objv[objc-1]);
    if (stmt == NULL) {
        ReleaseHandle(handle);
        return NS_ERROR;
    }
    *handlePtrPtr = handle;
    *stmtPtrPtr = stmt;
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 * GetHandle --
 *
 *      Get 1 handle from the given pool, first checking the interp cache.
 *
 * Results:
 *      Pointer to handle or NULL on error.
 *
 * Side effects:
 *      Tcl error result may be set.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Handle*
GetHandle(InterpData *idataPtr, const char *pool, int timeout)
{
    Tcl_Interp    *interp = idataPtr->interp;
    Dbi_Handle    *handlePtr;
    Dbi_Pool      *poolPtr;
    Tcl_HashEntry *hPtr;
    char          *poolName;
    int            new;

    if ((poolPtr = GetPool(idataPtr, pool)) == NULL) {
        return NULL;
    }
    poolName = poolPtr->name;

    /*
     * Check for a cached handle from the desired pool for this interp.
     */

    hPtr = Tcl_FindHashEntry(&idataPtr->handles, poolName);
    if (hPtr != NULL) {
        handlePtr = (Dbi_Handle*) Tcl_GetHashValue(hPtr);
        return handlePtr;
    }

    /*
     * Get a fresh handle directly from the pool.
     */

    handlePtr = NULL;
    switch (Dbi_PoolTimedGetHandle(&handlePtr, poolPtr, timeout)) {
    case NS_OK:
        /* groovy... */
        break;
    case NS_TIMEOUT:
        Exception(interp, "TIMEOUT", "wait for database handle timed out");
        break;
    default:
        Exception(interp, NULL, "database handle allocation failed");
        break;
    }

    /*
     * Cache the handle for future use by this interp
     */

    if (handlePtr != NULL && ((Pool *)poolPtr)->cache_handles) {
        hPtr = Tcl_CreateHashEntry(&idataPtr->handles, poolName, &new);
        Tcl_SetHashValue(hPtr, handlePtr);
        if (!idataPtr->cleanup) {
            idataPtr->cleanup = 1;
            Ns_TclRegisterDeferred(interp, ReleaseAllHandles, idataPtr);
        }
    }

    return handlePtr;
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
GetPool(InterpData *idataPtr, const char *pool)
{
    Tcl_Interp *interp = idataPtr->interp;
    Dbi_Pool *poolPtr;

    if (pool == NULL || *pool == '\0') {
        poolPtr = Dbi_PoolDefault(idataPtr->server);
        if (poolPtr == NULL) {
            Exception(interp, NULL, "no pool specified and no default configured");
            return NULL;
        }
    } else {
        poolPtr = Dbi_GetPool(idataPtr->server, pool);
        if (poolPtr == NULL) {
            Exception(interp, NULL, "pool '%s' not valid or not available for server '%s'",
                      pool, idataPtr->server);
            return NULL;
        }
    }

    return poolPtr;
}


/*
 *----------------------------------------------------------------------
 * BindVars --
 *
 *      Construct a statement with the given SQL and bind Tcl values to
 *      the named bind variables using either the passed in dictionary
 *      or variables from the current scope.
 *
 * Results:
 *      Pointer to valid statment or NULL on error.
 *
 * Side effects:
 *      Tcl error result left in interp.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Statement *
BindVars(Tcl_Interp *interp, Dbi_Pool *pool, Tcl_Obj *dictObjPtr, Tcl_Obj *sqlObjPtr)
{
    Dbi_Statement  *stmt;
    Tcl_HashSearch  search;
    Tcl_HashEntry  *hPtr;
    char           *sql, *name, *value;
    int             sqlLen, valueLen;

    sql  = Tcl_GetStringFromObj(sqlObjPtr, &sqlLen);
    stmt = Dbi_StatementAlloc(pool, sql, sqlLen);
    if (stmt != NULL) {
        for_each_hash_entry(hPtr, &stmt->bindVars, &search) {
            name = Tcl_GetHashKey(&stmt->bindVars, hPtr);
            if ((value = GetVar(interp, dictObjPtr, name, &valueLen)) == NULL) {
                Dbi_StatementFree(stmt);
                return NULL;
            }
            Dbi_StatementBindValue(stmt, name, value, valueLen);
        }
    }
    return stmt;
}


/*
 *----------------------------------------------------------------------
 * GetVar --
 *
 *      Get a reference to the bytes of a Tcl object.
 *
 * Results:
 *      Pointer to value of named variable, or NULL on error.
 *
 * Side effects:
 *      Error result may be left in interp.
 *
 *----------------------------------------------------------------------
 */

static char *
GetVar(Tcl_Interp *interp, Tcl_Obj *dictObjPtr, char *name, int *len)
{
    Tcl_Obj    *nameObjPtr, *valueObjPtr;
    char       *data = NULL;

    nameObjPtr = Tcl_NewStringObj(name, -1);
    if (dictObjPtr != NULL) {
        if (Tcl_DictObjGet(interp, dictObjPtr, nameObjPtr, &valueObjPtr) != TCL_OK) {
            Tcl_AddObjErrorInfo(interp,
                "\nnsdbi: bind variable lookup in dictionary failed: ", -1);
            Tcl_AddObjErrorInfo(interp, name, -1);
            goto done;
        }
    } else {
        valueObjPtr = Tcl_ObjGetVar2(interp, nameObjPtr, NULL, TCL_LEAVE_ERR_MSG);
        if (valueObjPtr == NULL) {
            Tcl_AddObjErrorInfo(interp, "\nnsdbi: bind variable lookup failed: ", -1);
            Tcl_AddObjErrorInfo(interp, name, -1);
            goto done;
        }
    }
    data = Tcl_GetStringFromObj(valueObjPtr, len);
 done:
    Tcl_DecrRefCount(nameObjPtr);
    return data;
}


/*
 *----------------------------------------------------------------------
 *
 * DictRowResult --
 *
 *      Set the result of the given Tcl interp to a dict representing
 *      a single row result.
 *
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
DictRowResult(Tcl_Interp *interp, Dbi_Handle *handle, Dbi_Statement *stmt)
{
    const char *column, *value;
    int         cLen, vLen, status;
    Tcl_Obj     *dictObjPtr, *colObjPtr, *valObjPtr;

    dictObjPtr = Tcl_NewDictObj();
    colObjPtr  = NULL;
    valObjPtr  = NULL;
    do {
        status = Dbi_NextValue(stmt, &value, &vLen, &column, &cLen);
        if (status == NS_ERROR) {
            break;
        }
        colObjPtr = Tcl_NewStringObj((char *) column, cLen);
        valObjPtr = Tcl_NewStringObj((char *) value, vLen);
        Tcl_IncrRefCount(colObjPtr);
        Tcl_IncrRefCount(valObjPtr);
        if (Tcl_DictObjPut(interp, dictObjPtr, colObjPtr, valObjPtr) != TCL_OK) {
            status = NS_ERROR;
            break;
        }
    } while (status == NS_OK);

    if (colObjPtr) {
        Tcl_DecrRefCount(colObjPtr);
    }
    if (valObjPtr) {
        Tcl_DecrRefCount(valObjPtr);
    }
    if (status != DBI_END_DATA) {
        Tcl_DecrRefCount(dictObjPtr);
        return SqlException(interp, handle);
    }
    Tcl_SetObjResult(interp, dictObjPtr);

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 * ReleaseHandle --
 *
 *      Release handle back to it's pool, unless handle caching is
 *      enabled, in which case just reset the handle.
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
ReleaseHandle(Dbi_Handle *handle)
{
    Pool *poolPtr = (Pool *) handle->pool;

    if (poolPtr->cache_handles) {
        Dbi_ResetHandle(handle);
    } else {
        Dbi_PoolPutHandle(handle);
    }
}


/*
 *----------------------------------------------------------------------
 * ReleaseAllHandles --
 *
 *      Release all database handles owned by this interp.
 *      Called by AOLserver on interp cleanup, bu the dbi_releasehandles
 *      command, and by the blocking command traces.
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
ReleaseAllHandles(Tcl_Interp *interp _nsunused, void *arg)
{
    InterpData     *idataPtr = arg;
    Dbi_Handle     *handle;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;

    for_each_hash_entry(hPtr, &idataPtr->handles, &search) {
    	handle = Tcl_GetHashValue(hPtr);
   	Dbi_PoolPutHandle(handle);
        Tcl_DeleteHashEntry(hPtr);
    }
    idataPtr->cleanup = 0;
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
Exception(Tcl_Interp *interp, const char *code, const char *msg, ...)
{
    Tcl_Obj    *objPtr;
    Ns_DString  ds;
    va_list     ap;

    objPtr = Tcl_NewStringObj("DBI", 3);
    if (code != NULL) {
        Tcl_ListObjAppendElement(interp, objPtr, Tcl_NewStringObj((char *) code, -1));
    }
    Tcl_SetObjErrorCode(interp, objPtr);

    Ns_DStringInit(&ds);
    va_start(ap, msg);
    Ns_DStringVPrintf(&ds, (char *) msg, ap);
    va_end(ap);
    Tcl_DStringResult(interp, &ds);
    Ns_DStringFree(&ds);

    return TCL_ERROR;
}

static int
SqlException(Tcl_Interp *interp, Dbi_Handle *handle)
{
    Exception(interp, Dbi_ExceptionCode(handle),
              Dbi_ExceptionMsg(handle));
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
