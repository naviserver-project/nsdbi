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
 *      Tcl commands to access the db.
 */

#include "nsdbi.h"

NS_RCSID("@(#) $Header$");


/*
 * The following struct maintains state for the currently
 * executing command.
 */

typedef struct InterpData {
    Tcl_Interp       *interp;
    CONST char       *server;
    Dbi_Handle       *handle;      /* Current handle. */
    Dbi_Pool         *pool;        /* Pool of current handle. */
    int               transaction; /* In transaction. */
} InterpData;


/*
 * Static functions defined in this file
 */

static Tcl_ObjCmdProc
    RowsObjCmd,
    DmlObjCmd,
    ZeroOrOneRowObjCmd,
    OneRowObjCmd,
    EvalObjCmd,
    TransactionObjCmd,
    CtlObjCmd;

static void FreeData(ClientData arg, Tcl_Interp *interp);
static int RowCmd(ClientData arg, Tcl_Interp *interp,
                  int objc, Tcl_Obj *CONST objv[],
                  int n);
static DBI_EXEC_STATUS ExecCmd(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[],
                               Dbi_Handle **handlePtrPtr);
static int EvalCmd(ClientData arg, Tcl_Interp *interp,
                   int objc, Tcl_Obj *CONST objv[],
                   int transaction);
static int ExecDirect(Tcl_Interp *interp, Dbi_Handle *handle, CONST char *sql);

static Dbi_Pool *GetPool(InterpData *, Tcl_Obj *poolObj);
static Dbi_Handle *GetHandle(InterpData *, Dbi_Pool *, Ns_Time *);
static void CleanupHandle(InterpData *idataPtr, Dbi_Handle *handle);
static int BindVars(Tcl_Interp *interp, Dbi_Statement *stmt,
                    CONST char **values, unsigned int *lengths,
                    char *array, char *setid);
static int RowResult(Tcl_Interp *interp, Dbi_Handle *handle);
static void SqlError(Tcl_Interp*, Dbi_Handle *);



/*
 *----------------------------------------------------------------------
 *
 * DbiInitInterp --
 *
 *      Add the dbi commands and data.
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
DbiInitInterp(Tcl_Interp *interp, void *arg)
{
    CONST char  *server = arg;
    InterpData  *idataPtr;
    int          i;

    static struct {
        CONST char     *name;
        Tcl_ObjCmdProc *proc;
    } cmds[] = {
        {"dbi_rows",        RowsObjCmd},
        {"dbi_dml",         DmlObjCmd},
        {"dbi_0or1row",     ZeroOrOneRowObjCmd},
        {"dbi_1row",        OneRowObjCmd},
        {"dbi_eval",        EvalObjCmd},
        {"dbi_transaction", TransactionObjCmd},
        {"dbi_ctl",         CtlObjCmd}
    };

    idataPtr = ns_calloc(1, sizeof(InterpData));
    idataPtr->server = server;
    idataPtr->interp = interp;
    Tcl_SetAssocData(interp, "dbi:data", FreeData, idataPtr);

    for (i = 0; i < (sizeof(cmds) / sizeof(cmds[0])); i++) {
        Tcl_CreateObjCommand(interp, cmds[i].name, cmds[i].proc, idataPtr, NULL);
    }

    return TCL_OK;
}

static void
FreeData(ClientData arg, Tcl_Interp *interp)
{
    ns_free(arg);
}


/*
 *----------------------------------------------------------------------
 *
 * RowsObjCmd --
 *
 *      Implements dbi_rows.
 *
 * Results:
 *      Standard Tcl result.
 *
 * Side effects:
 *      See Exec().
 *
 *----------------------------------------------------------------------
 */

static int
RowsObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData  *idataPtr = arg;
    Dbi_Handle  *handle = NULL;
    int          status = TCL_ERROR;

    switch (ExecCmd(idataPtr, objc, objv, &handle)) {
    case DBI_EXEC_ROWS:
        if (Dbi_NumRows(handle) > 0) {
            status = RowResult(interp, handle);
        } else {
            status = TCL_OK;
        }
        break;

    case DBI_EXEC_DML:
        Tcl_SetResult(interp, "query was not a statment returning rows.",
                      TCL_STATIC);
        break;

    case DBI_EXEC_ERROR:
        break;
    }
    CleanupHandle(idataPtr, handle);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * DmlObjCmd --
 *
 *      Implements dbi_dml.
 *
 * Results:
 *      Standard Tcl result.
 *
 * Side effects:
 *      See Exec().
 *
 *----------------------------------------------------------------------
 */

static int
DmlObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = arg;
    Dbi_Handle *handle = NULL;
    int         status = TCL_ERROR;

    switch (ExecCmd(idataPtr, objc, objv, &handle)) {
    case DBI_EXEC_DML:
        Tcl_SetIntObj(Tcl_GetObjResult(interp), Dbi_NumRows(handle));
        status = TCL_OK;
        break;

    case DBI_EXEC_ROWS:
        Tcl_SetResult(interp, "query was not a DML or DDL command", TCL_STATIC);
        break;

    case DBI_EXEC_ERROR:
        break;
    }
    CleanupHandle(idataPtr, handle);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * ZeroOrOneRowObjCmd, OneRowObjCmd --
 *
 *      Implements dbi_0or1row / dbi_1row.
 *
 * Results:
 *      Standard Tcl result.
 *
 * Side effects:
 *      See Exec().
 *
 *----------------------------------------------------------------------
 */

static int
ZeroOrOneRowObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    return RowCmd(arg, interp, objc, objv, 0);
}

static int
OneRowObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    return RowCmd(arg, interp, objc, objv, 1);
}

static int
RowCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[],
       int n)
{
    InterpData *idataPtr = arg;
    Dbi_Handle *handle = NULL;
    int         nrows, status = TCL_ERROR;

    switch (ExecCmd(idataPtr, objc, objv, &handle)) {
    case DBI_EXEC_ROWS:
        nrows = Dbi_NumRows(handle);
        if (nrows > 1) {
            Tcl_SetResult(interp, "query returned more than one row", TCL_STATIC);
        } else if (nrows == 0) {
            if (n == 1) {
                Tcl_SetResult(interp, "query was not a statement returning rows",
                              TCL_STATIC);
            } else {
                status = TCL_OK;
            }
        } else {
            status = RowResult(interp, handle);
        }
        break;

    case DBI_EXEC_DML:
        Tcl_SetResult(interp, "query was not a statement returning rows",
                      TCL_STATIC);
        break;

    case DBI_EXEC_ERROR:
        break;
    }
    CleanupHandle(idataPtr, handle);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * EvalObjCmd, TransactionObjCmd --
 *
 *      Implements dbi_eval / dbi_transaction.
 *
 *      Evaluate the dbi commands in the given block of Tcl with a
 *      single database handle.
 *
 * Results:
 *      Standard Tcl result.
 *
 * Side effects:
 *      Transaction may be rolled back on error.
 *
 *----------------------------------------------------------------------
 */

static int
EvalObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    return EvalCmd(arg, interp, objc, objv, 0);
}

static int
TransactionObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    return EvalCmd(arg, interp, objc, objv, 1);
}

static int
EvalCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[],
        int transaction)
{
    InterpData      *idataPtr = arg;
    Dbi_Pool        *pool;
    Dbi_Handle      *handle;
    Ns_Time         *timeoutPtr = NULL;
    Tcl_Obj         *scriptObj, *poolObj = NULL;
    int              status;

    Ns_ObjvSpec opts[] = {
        {"-pool",     Ns_ObjvObj,   &poolObj,    NULL},
        {"-timeout",  Ns_ObjvTime,  &timeoutPtr, NULL},
        {"--",        Ns_ObjvBreak, NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"script",    Ns_ObjvObj,   &scriptObj, NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    /*
     * Don't allow nested calls to this Tcl command.  We might support
     * true nested transactions one day and don't want people getting
     * the wrong idea.
     */

    if (idataPtr->handle != NULL) {
        Tcl_AppendResult(interp, Tcl_GetString(objv[0]),
                         " already in progress", NULL);
        return TCL_ERROR;
    }

    /*
     * Grab a free handle.
     */

    if ((pool = GetPool(idataPtr, poolObj)) == NULL
        || (handle = GetHandle(idataPtr, pool, timeoutPtr)) == NULL) {
        return TCL_ERROR;
    }

    if (transaction) {
        if (ExecDirect(interp, handle, "begin transaction") != TCL_OK) {
            Dbi_PutHandle(handle);
            return TCL_ERROR;
        }
        idataPtr->transaction = 1;
    }

    /*
     * Cache the handle and run the script.
     */

    idataPtr->pool = pool;
    idataPtr->handle = handle;
    status = Tcl_EvalObjEx(interp, scriptObj, 0);
    idataPtr->handle = NULL;
    idataPtr->pool = NULL;

    /*
     * Commit or rollback an active transaction.
     */

    if (transaction) {
        idataPtr->transaction = 0;
        if (status != TCL_OK) {
            status = TCL_ERROR;
            Tcl_AddErrorInfo(interp, "\n    dbi transaction status:\nrollback");
            (void) ExecDirect(interp, handle, "rollback");
        } else if (ExecDirect(interp, handle, "commit") != TCL_OK) {
            status = TCL_ERROR;
        }
    }

    Dbi_PutHandle(handle);

    return status;
}

static int
ExecDirect(Tcl_Interp *interp, Dbi_Handle *handle, CONST char *sql)
{
    if (Dbi_ExecDirect(handle, sql) == DBI_EXEC_ERROR) {
        SqlError(interp, handle);
        return TCL_ERROR;
    } else {
        Dbi_Flush(handle);
    }
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * CtlObjCmd --
 *
 *      Implements dbi_ctl.
 *
 * Results:
 *      Standard Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
CtlObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = arg;
    Dbi_Pool   *pool;
    Ns_DString  ds;
    int         cmd;

    static CONST char *cmds[] = {
        "bounce", "default", "info", "pools", "stats", NULL
    };
    enum CmdIdx {
        CBounceCmd, CDefaultCmd, CInfoCmd, CPoolsCmd, CStatsCmd
    };
    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "command ?args?");
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[1], cmds, "command", 0,
                            &cmd) != TCL_OK) {
        return TCL_ERROR;
    }

    switch (cmd) {
    case CDefaultCmd:
        pool = Dbi_DefaultPool(idataPtr->server);
        if (pool != NULL) {
            Tcl_SetStringObj(Tcl_GetObjResult(interp), Dbi_PoolName(pool), -1);
        }
        break;

    case CPoolsCmd:
        Ns_DStringInit(&ds);
        if (Dbi_ListPools(&ds, idataPtr->server) != NS_OK) {
            return TCL_ERROR;
        }
        Tcl_DStringResult(interp, &ds);
        break;

    case CBounceCmd:
    case CInfoCmd:
    case CStatsCmd:
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "pool");
            return TCL_ERROR;
        }
        if ((pool = GetPool(idataPtr, objv[2])) == NULL) {
            return TCL_ERROR;
        }

        if (cmd == CBounceCmd) {
            Dbi_BouncePool(pool);
        } else if (cmd == CStatsCmd) {
            Ns_DStringInit(&ds);
            Dbi_Stats(&ds, pool);
            Tcl_DStringResult(interp, &ds);
        } else {
            Tcl_AppendElement(interp, "driver");
            Tcl_AppendElement(interp, Dbi_DriverName(pool));
            Tcl_AppendElement(interp, "database");
            Tcl_AppendElement(interp, Dbi_DatabaseName(pool));
        }
        break;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ExecCmd --
 *
 *      Parse command arguments and execute the given SQL statement.
 *
 * Results:
 *      DBI_EXEC_ROWS, DBI_EXEC_DML or DBI_EXEC_ERROR.
 *      handlePtrPtr updated with active db handle on successfull retun.
 *
 * Side effects:
 *      Error message may be left in interp.
 *      Prepared statement may be cached. See: Dbi_Exec().
 *
 *----------------------------------------------------------------------
 */

static DBI_EXEC_STATUS
ExecCmd(InterpData *idataPtr, int objc, Tcl_Obj *CONST objv[],
        Dbi_Handle **handlePtrPtr)
{
    Tcl_Interp       *interp = idataPtr->interp;
    Dbi_Pool         *pool;
    Dbi_Handle       *handle;
    Dbi_Statement    *stmt;
    CONST char       *values[DBI_MAX_BIND];
    unsigned int      lengths[DBI_MAX_BIND];
    DBI_EXEC_STATUS   dbistat;
    int               length;
    Ns_Time          *timeoutPtr = NULL;
    Tcl_Obj          *sqlObj, *poolObj = NULL;
    char             *sql, *array = NULL, *setid = NULL;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bindarray", Ns_ObjvString, &array,      NULL},
        {"-bindset",   Ns_ObjvString, &setid,      NULL},
        {"--",         Ns_ObjvBreak,  NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"sql",        Ns_ObjvObj, &sqlObj, NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return DBI_EXEC_ERROR;
    }

    /*
     * Grab a free handle, possibly from the interp cache.
     */

    if ((pool = GetPool(idataPtr, poolObj)) == NULL
        || (handle = GetHandle(idataPtr, pool, timeoutPtr)) == NULL) {
        return DBI_EXEC_ERROR;
    }

    /*
     * Prepare the statement for our handle.
     */

    dbistat = DBI_EXEC_ERROR;

    sql = Tcl_GetStringFromObj(sqlObj, &length);
    stmt = Dbi_Prepare(handle, sql, length);
    if (stmt == NULL) {
        SqlError(interp, handle);
        goto done;
    }

    /*
     * Bind values to variable as required and execute the statement.
     */

    if (BindVars(interp, stmt, values, lengths, array, setid) != TCL_OK) {
        goto done;
    }
    dbistat = Dbi_Exec(handle, stmt, values, lengths);
    if (dbistat == DBI_EXEC_ERROR) {
        SqlError(interp, handle);
        goto done;
    }

 done:
    if (dbistat == DBI_EXEC_ERROR) {
        CleanupHandle(idataPtr, handle);
    } else {
        *handlePtrPtr = handle;
    }

    return dbistat;
}


/*
 *----------------------------------------------------------------------
 *
 * GetPool --
 *
 *      Return a Dbi_Pool given a pool name or the default pool if no
 *      name is given.
 *
 * Results:
 *      Pointer to pool or NULL if no default pool.
 *
 * Side effects:
 *      The Tcl object may be converted to dbi:pool type, and en error
 *      may be left in the interp if conversion fails.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Pool *
GetPool(InterpData *idataPtr, Tcl_Obj *poolObj)
{
    Dbi_Pool    *pool;
    const char  *poolType = "dbi:pool";

    if (poolObj == NULL) {
        if (idataPtr->pool != NULL) {
            pool = idataPtr->pool;
        } else {
            pool = Dbi_DefaultPool(idataPtr->server);
            if (pool == NULL) {
                Tcl_SetResult(idataPtr->interp,
                              "no pool specified and no default configured", TCL_STATIC);
                return NULL;
            }
        }
    } else if (Ns_TclGetOpaqueFromObj(poolObj, poolType, (void **) &pool) != TCL_OK) {
        pool = Dbi_GetPool(idataPtr->server, Tcl_GetString(poolObj));
        if (pool == NULL) {
            Tcl_SetResult(idataPtr->interp,
                "invalid pool name or pool not available to virtual server", TCL_STATIC);
            return NULL;
        }
        Ns_TclSetOpaqueObj(poolObj, poolType, pool);
    }

    return pool;
}


/*
 *----------------------------------------------------------------------
 *
 * GetHandle --
 *
 *      Get a handle from the given pool within timeout. Use the handle
 *      cache in InterpData if the pools match.
 *
 * Results:
 *      Pointer to handle or NULL on error.
 *
 * Side effects:
 *      Error message left in interp.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Handle *
GetHandle(InterpData *idataPtr, Dbi_Pool *pool, Ns_Time *timeoutPtr)
{
    Tcl_Interp *interp = idataPtr->interp;
    Dbi_Handle *handle;
    Ns_Time     time;

    /*
     * First check the handle cache.
     */

    if (idataPtr->handle != NULL
        && idataPtr->handle->pool == pool) {
        return idataPtr->handle;
    }

    /*
     * Make sure the timeout, if given, is an absolute time in
     * the future and grab a handle from the pool.
     */

    if (timeoutPtr != NULL) {
        timeoutPtr = Ns_AbsoluteTime(&time, timeoutPtr);
    }

    switch (Dbi_GetHandle(&handle, pool, timeoutPtr)) {
    case NS_OK:
        return handle;
        break;
    case NS_TIMEOUT:
        Tcl_SetErrorCode(interp, "NS_TIMEOUT", NULL);
        Tcl_SetResult(interp, "wait for database handle timed out", TCL_STATIC);
        break;
    default:
        Tcl_SetResult(interp, "handle allocation failed", TCL_STATIC);
        break;
    }
    return NULL;
}


/*
 *----------------------------------------------------------------------
 *
 * CleanupHandle --
 *
 *      Flush or return a handle to it's pool, as appropriate.
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
CleanupHandle(InterpData *idataPtr, Dbi_Handle *handle)
{
    if (idataPtr->handle != NULL) {
        /*
         * Handle is cached -- reset.
         */
        Dbi_Flush(handle);

    } else if (handle != NULL) {
        /*
         * Handle was accuired for this command only -- return to pool.
         */
        Dbi_PutHandle(handle);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * BindVars --
 *
 *      Bind values to the variables of a statement, looking at the keys
 *      of the dictionary if given, or local variables otherwise.
 *
 * Results:
 *      TCL_OK or TCL_ERROR.
 *
 * Side effects:
 *      Error message may be left in interp.
 *
 *----------------------------------------------------------------------
 */

static int
BindVars(Tcl_Interp *interp, Dbi_Statement *stmt,
         CONST char **values, unsigned int *lengths,
         char *array, char *setid)
{
    Ns_Set         *set = NULL;
    Tcl_Obj        *valObjPtr;
    CONST char     *key, *value;
    int             i, nbind, length;

    nbind = Dbi_GetNumVariables(stmt);
    if (nbind == 0) {
        return TCL_OK;
    }

    if (setid != NULL) {
        if (Ns_TclGetSet2(interp, setid, &set) != TCL_OK) {
            return TCL_ERROR;
        }
    }

    for (i = 0; i < nbind; i++) {

        if (Dbi_GetBindVariable(stmt, i, &key) != NS_OK) {
            Ns_Log(Bug, "dbi: BindVars: bind variable out of range");
            return TCL_ERROR;
        }

        value = NULL;

        if (set != NULL) {
            if ((value = Ns_SetGet(set, key)) != NULL) {
                length = strlen(value);
            }
        } else {
            valObjPtr = Tcl_GetVar2Ex(interp, array ? array : key,
                                      array ? key : NULL, TCL_LEAVE_ERR_MSG);
            if (valObjPtr != NULL) {
                value = Tcl_GetStringFromObj(valObjPtr, &length);
            }
        }
        if (value == NULL) {
            Tcl_AddObjErrorInfo(interp, "\ndbi: bind variable not found: ", -1);
            Tcl_AddObjErrorInfo(interp, key, -1);
            return TCL_ERROR;
        }

        values[i] = value;
        lengths[i] = length;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * RowResult --
 *
 *      Set the result of the given Tcl interp to a list representing
 *      the values of a result set.
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
RowResult(Tcl_Interp *interp, Dbi_Handle *handle)
{
    Tcl_Obj          *resObj;
    CONST char       *value;
    int               vLen;
    DBI_VALUE_STATUS  dbistat;

    resObj = Tcl_GetObjResult(interp);
    do {
        dbistat = Dbi_NextValue(handle, &value, &vLen, NULL, NULL);
        if (dbistat == DBI_VALUE_ERROR) {
            SqlError(interp, handle);
            return TCL_ERROR;
        }
        if (Tcl_ListObjAppendElement(interp, resObj,
                Tcl_NewStringObj((char *) value, vLen)) != TCL_OK) {
            return TCL_ERROR;
        }
    } while (dbistat != DBI_END_ROWS);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * SqlError --
 *
 *      Set the Tcl error from the handle code and message.
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
SqlError(Tcl_Interp *interp, Dbi_Handle *handle)
{
    Tcl_SetErrorCode(interp, Dbi_ExceptionCode(handle), NULL);
    Tcl_AppendResult(interp, Dbi_ExceptionMsg(handle), NULL);
}
