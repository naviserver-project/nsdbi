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
 * The following structure tracks the handles currently
 * allocated for an interp.
 */

typedef struct InterpData {
    ServerData    *sdataPtr;
    Tcl_Interp    *interp;
    Tcl_HashTable  handles;
    int            cleanup;
} InterpData;


/*
 * Static functions defined in this file
 */

static Dbi_Pool *
GetPool(InterpData *idataPtr, Tcl_Obj *poolObjPtr)
     NS_GNUC_NONNULL(1);

static Dbi_Handle *
GetHandle(InterpData *idataPtr, Tcl_Obj *poolObjPtr, int timeout)
     NS_GNUC_NONNULL(1);

static Dbi_Statement *
GetStmt(Tcl_Obj *objPtr)
     NS_GNUC_NONNULL(1);

static int
GetHandleForStmt(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[],
                 Dbi_Handle **handlePtrPtr, Dbi_Statement **stmtPtrPtr)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3) NS_GNUC_NONNULL(4) NS_GNUC_NONNULL(5);

static void
PutHandle(InterpData *idataPtr, Dbi_Handle *handle)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

static int
ReleaseHandles(InterpData *idataPtr) NS_GNUC_NONNULL(1);

static int
BindVars(Tcl_Interp *interp, Dbi_Statement *stmt, char *array, char *set)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

static int
SingleRowResult(Tcl_Interp *interp, Dbi_Handle *handle, Dbi_Statement *stmt)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

static int
Exception(Tcl_Interp *interp, CONST char *code, CONST char *msg)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);

static int
SqlException(Tcl_Interp *interp, Dbi_Handle *handle)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

static Ns_TclDeferProc         InterpCleanup;
static Tcl_InterpDeleteProc    FreeInterpData;

static Tcl_FreeInternalRepProc FreeStmt;
static Tcl_DupInternalRepProc  DupStmt;
static Tcl_UpdateStringProc    UpdateStringOfStmt;
static Tcl_SetFromAnyProc      SetStmtFromAny;

static Tcl_ObjCmdProc
    Tcl0or1rowCmd, Tcl1rowCmd, TclRowsCmd, TclDmlCmd, TclReleasehandlesCmd,
    TclPoolCmd, TclListpoolsCmd, TclDefaultpoolCmd;


/*
 * Static variables defined in this file.
 */

static char *datakey = "dbi:data";

static Tcl_ObjType stmtType = {
    "nsdbi:statement",
    FreeStmt,
    DupStmt,
    UpdateStringOfStmt,
    SetStmtFromAny
};

static struct Cmd {
    char                  *name;
    Tcl_ObjCmdProc        *objProc;
} CONST cmds[] = {
    {"dbi_0or1row",        Tcl0or1rowCmd},
    {"dbi_1row",           Tcl1rowCmd},
    {"dbi_rows",           TclRowsCmd},
    {"dbi_dml",            TclDmlCmd},
    {"dbi_releasehandles", TclReleasehandlesCmd},
    {"dbi_pool",           TclPoolCmd},
    {"dbi_listpools",      TclListpoolsCmd},
    {"dbi_defaultpool",    TclDefaultpoolCmd},
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
 * DbiInitTclObjTypes --
 *
 *      One time initialization of the nsdbi Tcl_Obj types.
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
DbiInitTclObjTypes()
{
    Tcl_RegisterObjType(&stmtType);
}


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
    char       *server = arg;
    ServerData *sdataPtr;
    InterpData *idataPtr;
    int         i;

    sdataPtr = DbiGetServer(server);
    idataPtr = ns_malloc(sizeof(InterpData));
    idataPtr->sdataPtr  = sdataPtr;
    idataPtr->interp    = interp;
    idataPtr->cleanup   = 0;
    Tcl_InitHashTable(&idataPtr->handles, TCL_STRING_KEYS);
    Tcl_SetAssocData(interp, datakey, FreeInterpData, idataPtr);

    Tcl_RegisterObjType(&stmtType);

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
DbiAddTraces(Tcl_Interp *interp, void *arg)
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
    int            status;

    if (GetHandleForStmt(idataPtr, objc, objv, &handle, &stmt) != TCL_OK) {
        return TCL_ERROR;
    }
    if (Dbi_1Row(handle, stmt, NULL) != NS_OK) {
        status = SqlException(interp, handle);
        goto done;
    }
    status = SingleRowResult(interp, handle, stmt);
 done:
    PutHandle(idataPtr, handle);
    return status;
}

static int
Tcl0or1rowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData    *idataPtr = clientData;
    Dbi_Handle    *handle;
    Dbi_Statement *stmt;
    int            nrows, status = TCL_OK;

    if (GetHandleForStmt(idataPtr, objc, objv, &handle, &stmt) != TCL_OK) {
        return TCL_ERROR;
    }
    if (Dbi_0or1Row(handle, stmt, &nrows, NULL) != NS_OK) {
        status = SqlException(interp, handle);
        goto done;
    }
    if (nrows == 1) {
        status = SingleRowResult(interp, handle, stmt);
    }
 done:
    PutHandle(idataPtr, handle);
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
    CONST char    *value;
    int            vLen, nrows;
    Tcl_Obj       *listObjPtr;
    int            status = TCL_OK;

    if (GetHandleForStmt(idataPtr, objc, objv, &handle, &stmt) != TCL_OK) {
        return TCL_ERROR;
    }
    if (Dbi_Select(handle, stmt, &nrows, NULL) != NS_OK) {
        status = SqlException(interp, handle);
        goto done;
    }
    if (nrows > 0) {
        listObjPtr = Tcl_GetObjResult(interp);
        while (1) {
            status = Dbi_NextValue(stmt, &value, &vLen, NULL, NULL);
            if (status == NS_ERROR) {
                status = SqlException(interp, handle);
                break;
            }
            Tcl_ListObjAppendElement(interp, listObjPtr,
                                     Tcl_NewStringObj(value, vLen));
            if (status == DBI_END_DATA) {
                status = TCL_OK;
                break;
            }
        }
    }
 done:
    PutHandle(idataPtr, handle);

    return status;
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
    int            status = TCL_OK;

    if (GetHandleForStmt(idataPtr, objc, objv, &handle, &stmt) != TCL_OK) {
        return TCL_ERROR;
    }
    if (Dbi_DML(handle, stmt, &nrows, NULL) != NS_OK) {
        status = SqlException(interp, handle);
        goto done;
    }
    Tcl_SetIntObj(Tcl_GetObjResult(interp), nrows);

 done:
    PutHandle(idataPtr, handle);
    return status;
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
    InterpData  *idataPtr     = clientData;
    Tcl_Obj     *resultObjPtr = Tcl_GetObjResult(interp);
    Tcl_Obj     *poolObjPtr   = NULL;
    Dbi_Handle  *handle       = NULL;
    Dbi_Pool    *poolPtr      = NULL;
    Ns_DString   ds;
    int          opt;

    static CONST char *opts[] = {
        "bounce", "datasource", "dbtype", "description", "driver",
        "nhandles", "password", "stats", "user", NULL
    };

    enum IPoolIdx {
        IBounceIdx, IDatasourceIdx, IDbtypeIdx, IDescriptionIdx, IDriverIdx,
        INhandlesIdx, IPasswordIdx, IStatsIdx, IUserIdx
    };

    if (objc != 2 && objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "option ?pool?");
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[1], opts, "option", 0, (int *) &opt)
        != TCL_OK) {
        return TCL_ERROR;
    }
    if (objc == 3) {
        poolObjPtr = objv[2];
    }
    if (opt == IDbtypeIdx || opt == IDriverIdx) {
        if ((handle = GetHandle(idataPtr, poolObjPtr, -1)) == NULL) {
            return TCL_ERROR;
        }
    } else {
        if ((poolPtr = GetPool(idataPtr, poolObjPtr)) == NULL) {
            return TCL_ERROR;
        }
    }

    switch (opt) {

    case IBounceIdx:
        Dbi_BouncePool(poolPtr);
        break;

    case IDriverIdx:
        Tcl_SetStringObj(resultObjPtr, Dbi_DriverName(handle), -1);
        PutHandle(idataPtr, handle);
        break;

    case IDbtypeIdx:
        Tcl_SetStringObj(resultObjPtr, Dbi_DriverDbType(handle), -1);
        PutHandle(idataPtr, handle);
        break;

    case IDatasourceIdx:
        Tcl_SetStringObj(resultObjPtr, poolPtr->datasource, -1);
        break;

    case IDescriptionIdx:
        Tcl_SetStringObj(resultObjPtr, poolPtr->description, -1);
        break;

    case INhandlesIdx:
        Tcl_SetIntObj(Tcl_GetObjResult(interp), poolPtr->nhandles);
        break;

    case IPasswordIdx:
        Tcl_SetStringObj(resultObjPtr, poolPtr->password, -1);
        break;

    case IStatsIdx:
        Ns_DStringInit(&ds);
        Dbi_PoolStats(&ds, poolPtr);
        Tcl_DStringResult(interp, &ds);
        Ns_DStringFree(&ds);
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
 * TclListpoolsCmd --
 *
 *      Implements the dbi_listpools command.
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
TclListpoolsCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    Tcl_Obj    *result;
    Ns_DString  ds;
    char       *p, *q;

    Ns_DStringInit(&ds);
    if (Dbi_PoolList(&ds, idataPtr->sdataPtr->server) != NS_OK) {
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
 *      Implements the dbi_defaultpool command.
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
    Dbi_Pool   *poolPtr;

    poolPtr = Dbi_PoolDefault(idataPtr->sdataPtr->server);
    if (poolPtr != NULL) {
        Tcl_SetStringObj(Tcl_GetObjResult(interp), poolPtr->name, -1);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclReleasehandlesCmd --
 *
 *      Implements the dbi_releasehandles command.
 *      Release any handles currently cached for this interp, returning
 *      the number that were released.
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
    int         nHandles;

    nHandles = ReleaseHandles(idataPtr);
    Tcl_SetIntObj(Tcl_GetObjResult(interp), nHandles);
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 * GetHandleForStmt --
 *
 *      Parse common command options to determine which pool to use, how
 *      long to wait when acquiring a handle, and how bind variables are
 *      specified.  Convert the statement string into a Dbi_Statement
 *      and bind any variables.
 *
 * Results:
 *      On success TCL_OK will be returned and both handle and statement
 *      pointers will be valid.
 *
 * Side effects:
 *      A new Dbi_Statement may be allocated.
 *
 *----------------------------------------------------------------------
 */

static int
GetHandleForStmt(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[],
                 Dbi_Handle **handlePtrPtr, Dbi_Statement **stmtPtrPtr)
{
    Tcl_Interp    *interp = idataPtr->interp;
    Dbi_Handle    *handle;
    Dbi_Statement *stmt;
    Tcl_Obj       *stmtObjPtr, *poolObjPtr = NULL;
    char          *array = NULL, *set = NULL;
    int            timeout = -1;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObjPtr, NULL},
        {"-timeout",   Ns_ObjvInt,    &timeout,    NULL},
        {"-bindarray", Ns_ObjvString, &array,      NULL},
        {"-bindset",   Ns_ObjvString, &set,        NULL},
        {"--",         Ns_ObjvBreak,  NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"statement", Ns_ObjvObj, &stmtObjPtr, NULL},
        {NULL, NULL, NULL, NULL}
    };

    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }
    stmt = GetStmt(stmtObjPtr);
    if (BindVars(interp, stmt, array, set) != TCL_OK) {
        return TCL_ERROR;
    }
    handle = GetHandle(idataPtr, poolObjPtr, timeout);
    if (handle == NULL) {
        return TCL_ERROR;
    }
    *handlePtrPtr = handle;
    *stmtPtrPtr = stmt;

    return TCL_OK;
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
GetHandle(InterpData *idataPtr, Tcl_Obj *poolObjPtr, int timeout)
{
    Tcl_Interp    *interp = idataPtr->interp;
    Dbi_Handle    *handlePtr;
    Dbi_Pool      *poolPtr;
    Tcl_HashEntry *hPtr;

    if ((poolPtr = GetPool(idataPtr, poolObjPtr)) == NULL) {
        return NULL;
    }

    /*
     * Check for a cached handle from the desired pool for this interp.
     */

    hPtr = Tcl_FindHashEntry(&idataPtr->handles, poolPtr->name);
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
        /* ok, nothing to do */
        break;
    case NS_TIMEOUT:
        Exception(interp, "TIMEOUT", "wait for database handle timed out");
        break;
    default:
        Exception(interp, NULL, "database handle allocation failed");
        break;
    }

    return handlePtr;
}


/*
 *----------------------------------------------------------------------
 * GetPool --
 *
 *      Return a Dbi_Pool given a pool name or the default pool if no
 *      name is given.
 *
 * Results:
 *      Pointer to pool or NULL if no default pool.
 *
 * Side effects:
 *      The Tcl object may be converted to nsdbi:pool type, and en error
 *      may be left in the interp if conversion fails.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Pool *
GetPool(InterpData *idataPtr, Tcl_Obj *objPtr)
{
    Tcl_Interp  *interp   = idataPtr->interp;
    ServerData  *sdataPtr = idataPtr->sdataPtr;
    Dbi_Pool    *pool;
    const char  *poolType = "nsdbi:pool";

    if (objPtr == NULL) {
        pool = (Dbi_Pool *) sdataPtr->defpoolPtr;
        if (pool == NULL) {
            Tcl_AppendToObj(Tcl_GetObjResult(interp),
                "no pool specified and no default configured",
                -1);
            return NULL;
        }
    } else if (Ns_TclGetOpaqueFromObj(objPtr, poolType, (void **) &pool) != TCL_OK) {
        pool = DbiGetPool(sdataPtr, Tcl_GetString(objPtr));
        if (pool == NULL) {
            Tcl_AppendToObj(Tcl_GetObjResult(interp),
                "invalid pool name or pool not available to virtual server",
                -1);
            return NULL;
        }
        Ns_TclSetOpaqueObj(objPtr, poolType, pool);
    }

    return pool;
}


/*
 *----------------------------------------------------------------------
 * GetStmt --
 *
 *      Get a Dbi_Statement from a Tcl object, converting if necessary.
 *
 * Results:
 *      Pointer to statement or NULL on error.
 *
 * Side effects:
 *      The object internal rep may be converted.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Statement *
GetStmt(Tcl_Obj *objPtr)
{
    if (objPtr->typePtr == &stmtType
        || SetStmtFromAny(NULL, objPtr) == TCL_OK) {
        return (Dbi_Statement *) objPtr->internalRep.otherValuePtr;
    }
    return NULL;
}


/*
 *----------------------------------------------------------------------
 * BindVars --
 *
 *      Bind values to the variables of a statement, looking at the keys
 *      of the dictionary if given, or local variables otherwise.
 *
 * Results:
 *      TCL_OK or TCL_ERROR;
 *
 * Side effects:
 *      Error message may be left in interp.
 *
 *----------------------------------------------------------------------
 */

static int
BindVars(Tcl_Interp *interp, Dbi_Statement *stmt,
         char *array, char *set)
{
    Tcl_HashSearch  search;
    Tcl_HashEntry  *hPtr;
    Ns_Set         *bindSet;
    Tcl_Obj        *valObjPtr;
    char           *key, *value;
    int             len;

    if (set != NULL) {
        if (Ns_TclGetSet2(interp, set, &bindSet) != TCL_OK) {
            return TCL_ERROR;
        }
    }

    hPtr = Tcl_FirstHashEntry(&stmt->bindVars, &search);
    while (hPtr != NULL) {
        key = Tcl_GetHashKey(&stmt->bindVars, hPtr);
        value = NULL;

        if (bindSet != NULL) {
            if ((value = Ns_SetGet(bindSet, key)) != NULL) {
                len = strlen(value);
            }
        } else {
            valObjPtr = Tcl_GetVar2Ex(interp, array ? array : key,
                                      array ? key : NULL, TCL_LEAVE_ERR_MSG);
            if (valObjPtr != NULL) {
                value = Tcl_GetStringFromObj(valObjPtr, &len);
            }
        }
        if (value == NULL) {
            Tcl_AddObjErrorInfo(interp, "\nnsdbi: bind variable not found: ", -1);
            Tcl_AddObjErrorInfo(interp, key, -1);
            return TCL_ERROR;
        }
        
        Dbi_StatementBindValue(stmt, key, value, len);

        hPtr = Tcl_NextHashEntry(&search);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * SingleRowResult --
 *
 *      Set the result of the given Tcl interp to a dict representing
 *      a single row result.
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
SingleRowResult(Tcl_Interp *interp, Dbi_Handle *handle, Dbi_Statement *stmt)
{
    Tcl_Obj    *resObjPtr, *valObjPtr;
    CONST char *value;
    int         vLen, status;

    resObjPtr = Tcl_GetObjResult(interp);
    do {
        status = Dbi_NextValue(stmt, &value, &vLen, NULL, NULL);
        if (status != NS_ERROR) {
            valObjPtr = Tcl_NewStringObj((char *) value, vLen);
            if (Tcl_ListObjAppendElement(interp, resObjPtr, valObjPtr) != TCL_OK) {
                status = NS_ERROR;
            }
        }
    } while (status == NS_OK);

    if (status != DBI_END_DATA) {
        return SqlException(interp, handle);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 * PutHandle --
 *
 *      Return a dbi handle to it's pool.  If handle caching is
 *      enabled, and this is a conn thread, reset the handle and store
 *      in per-interp hash table.
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
PutHandle(InterpData *idataPtr, Dbi_Handle *handle)
{
    Pool          *poolPtr = (Pool *) handle->pool;
    Tcl_HashEntry *hPtr;
    int            new;

    if (poolPtr->cache_handles && Ns_GetConn()) {
        hPtr = Tcl_CreateHashEntry(&idataPtr->handles, poolPtr->name, &new);
        Tcl_SetHashValue(hPtr, handle);
        if (!idataPtr->cleanup) {
            idataPtr->cleanup = 1;
            Ns_TclRegisterDeferred(idataPtr->interp, InterpCleanup, idataPtr);
        }
        Dbi_ResetHandle(handle);
    } else {
        Dbi_PoolPutHandle(handle);
    }
}


/*
 *----------------------------------------------------------------------
 * ReleaseHandles --
 *
 *      Release all database handles owned by the given interp.
 *
 * Results:
 *      Number of handles released.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
ReleaseHandles(InterpData *idataPtr)
{
    Dbi_Handle     *handle;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;
    int             i = 0;

    hPtr = Tcl_FirstHashEntry(&idataPtr->handles, &search);
    while (hPtr != NULL) {
    	handle = Tcl_GetHashValue(hPtr);
        Dbi_PoolPutHandle(handle);
        Tcl_DeleteHashEntry(hPtr);
        i++;
        hPtr = Tcl_NextHashEntry(&search);
    }
    idataPtr->cleanup = 0;
    return i;
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
Exception(Tcl_Interp *interp, CONST char *code, CONST char *msg)
{
    Tcl_Obj    *objPtr;

    objPtr = Tcl_NewStringObj("DBI", -1);
    if (code != NULL) {
        Tcl_ListObjAppendElement(interp, objPtr, Tcl_NewStringObj((char *) code, -1));
    }
    Tcl_SetObjErrorCode(interp, objPtr);
    Tcl_SetStringObj(Tcl_GetObjResult(interp), msg, -1);

    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 * SqlException --
 *
 *      Set the exception code and message from the given handle as
 *      the Tcl result.
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
SqlException(Tcl_Interp *interp, Dbi_Handle *handle)
{
    Exception(interp, Dbi_ExceptionCode(handle),
              Dbi_ExceptionMsg(handle));
    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 * InterpCleanup --
 *
 *      Release all database handles owned by this interp.  Called by
 *      the server on interp cleanup after every connection.
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
InterpCleanup(Tcl_Interp *interp, void *arg)
{
    InterpData *idataPtr = arg;

    (void) ReleaseHandles(idataPtr);
}


/*
 *----------------------------------------------------------------------
 * FreeInterpData --
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
FreeInterpData(ClientData arg, Tcl_Interp *interp)
{
    InterpData *idataPtr = arg;

    (void) ReleaseHandles(idataPtr);
    Tcl_DeleteHashTable(&idataPtr->handles);
    ns_free(idataPtr);
}


/*
 *----------------------------------------------------------------------
 * FreeStmt --
 *
 *     This procedure is called to delete the internal rep of a
 *     statement Tcl object.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      The internal representation of the given object is deleted..
 *
 *----------------------------------------------------------------------
 */

static void
FreeStmt(Tcl_Obj *objPtr)
{
    Dbi_Statement *stmt;

    stmt = (Dbi_Statement *) objPtr->internalRep.otherValuePtr;
    Dbi_StatementFree(stmt);
}


/*
 *----------------------------------------------------------------------
 * DupStmt --
 *
 *     This procedure is called to copy the internal rep of a statement
 *     Tcl object to another object.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      The internal representation of the target object is updated
 *      and the type is set.
 *
 *----------------------------------------------------------------------
 */

static void
DupStmt(Tcl_Obj *srcObjPtr, Tcl_Obj *dupObjPtr)
{
    Statement     *srcStmtPtr = (Statement *) srcObjPtr->internalRep.otherValuePtr;
    Dbi_Statement *stmtPtr;

    stmtPtr = Dbi_StatementAlloc(srcStmtPtr->dsSql.string, srcStmtPtr->dsSql.length);
    Ns_TclSetOtherValuePtr(dupObjPtr, &stmtType, stmtPtr);
}


/*
 *----------------------------------------------------------------------
 * UpdateStringOfStmt --
 *
 *     This procedure is called to convert a Tcl object from statement
 *     internal form to it's string form: the original sql string.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      The string representation of the object is updated.
 *
 *----------------------------------------------------------------------
 */

static void
UpdateStringOfStmt(Tcl_Obj *objPtr)
{
    Statement *stmtPtr;

    stmtPtr = (Statement *) objPtr->internalRep.otherValuePtr;
    Ns_TclSetStringRep(objPtr, stmtPtr->dsSql.string, stmtPtr->dsSql.length);
}


/*
 *----------------------------------------------------------------------
 * SetStmtFromAny --
 *
 *      Attempt to convert a Tcl object to nsdbi:statement type.
 *
 * Results:
 *      TCL_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
SetStmtFromAny(Tcl_Interp *interp, Tcl_Obj *objPtr)
{
    Dbi_Statement *stmt;
    char          *string;
    int            len;

    string = Tcl_GetStringFromObj(objPtr, &len);
    stmt = Dbi_StatementAlloc(string, len);
    Ns_TclSetOtherValuePtr(objPtr, &stmtType, stmt);
    return TCL_OK;
}
