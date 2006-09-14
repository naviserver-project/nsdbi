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

#include "nsdbi.h"

NS_RCSID("@(#) $Header$");


/*
 * The following struct maintains state for the currently
 * executing command.
 */

typedef struct InterpData {
    Tcl_Interp       *interp;
    CONST char       *server;
    Dbi_Handle       *handle;
    int               transaction;
} InterpData;

/*
 * Static functions defined in this file
 */

static Tcl_ObjCmdProc TclDbiCmd;
static void FreeData(ClientData arg, Tcl_Interp *interp);

static Dbi_Pool *
GetPool(InterpData *, Tcl_Obj *poolObj)
    NS_GNUC_NONNULL(1);

static Dbi_Handle *
GetHandle(InterpData *, Dbi_Pool *, Ns_Time *)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

static int
BindVars(Tcl_Interp *interp, Dbi_Statement *stmt,
         CONST char **values, unsigned int *lengths,
         char *array, char *setid)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

static int
RowResult(Tcl_Interp *interp, Dbi_Handle *handle)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

static void
SqlError(Tcl_Interp*, Dbi_Handle *)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);


/*
 * Static variables defined in this file.
 */

static CONST char *datakey = "dbi:data";



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
    CONST char *server = arg;
    InterpData *idataPtr;

    idataPtr = ns_calloc(1, sizeof(InterpData));
    idataPtr->server = server;
    idataPtr->interp = interp;

    Tcl_SetAssocData(interp, datakey, FreeData, idataPtr);
    Tcl_CreateObjCommand(interp, "dbi", TclDbiCmd, idataPtr, NULL);

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
 * TclDbiCmd --
 *
 *      Implements the dbi commands.
 *
 * Results:
 *      Tcl result.
 *
 * Side effects:
 *      Various.
 *
 *----------------------------------------------------------------------
 */

static int
TclDbiCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData       *idataPtr = arg;
    Dbi_Pool         *pool;
    Dbi_Handle       *handle;
    Ns_DString        ds;
    int               cmd, status;
    Tcl_Obj          *poolObj = NULL;
    Ns_Time          *timeoutPtr = NULL;

    static CONST char *cmds[] = {
        "0or1row", "1row", "bounce", "default", "dml", "info",
        "pools", "rows", "stats", "transaction", "withhandle", NULL
    };
    enum CmdIdx {
        C0or1rowCmd, C1rowCmd, CBounceCmd, CDefaultCmd, CDmlCmd, CInfoCmd,
        CPoolsCmd, CRowsCmd, CStatsCmd, CTransactionCmd, CWithHandleCmd
    };
    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "command ?args?");
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[1], cmds, "command", 0,
                            &cmd) != TCL_OK) {
        return TCL_ERROR;
    }

    status = TCL_OK;

    switch (cmd) {

    case CRowsCmd:
    case C1rowCmd:
    case C0or1rowCmd:
    case CDmlCmd: {

        Tcl_Obj          *sqlObj;
        Dbi_Statement    *stmt;
        CONST char       *values[DBI_MAX_BIND];
        unsigned int      lengths[DBI_MAX_BIND];
        DBI_EXEC_STATUS   dbistat;
        int               nrows, length;
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
        if (Ns_ParseObjv(opts, args, interp, 2, objc, objv) != NS_OK) {
            return TCL_ERROR;
        }

        /*
         * Grab a free handle, possibly from cache.
         */

        if ((pool = GetPool(idataPtr, poolObj)) == NULL
            || (handle = GetHandle(idataPtr, pool, timeoutPtr)) == NULL) {
            return TCL_ERROR;
        }

        /*
         * Prepare the statement for our handle.
         *
         * From here on we need to cleanup the handle before
         * returning an error.
         */

        status = TCL_ERROR;

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
        nrows = Dbi_NumRows(handle);        

        /*
         * Check assertions about the result set.
         */

        if ((cmd != CDmlCmd && dbistat != DBI_EXEC_ROWS)
            || (cmd == C1rowCmd && nrows == 0)) {
            Tcl_SetResult(interp, "query was not a statement returning rows", TCL_STATIC);
        } else if ((cmd == C0or1rowCmd || cmd == C1rowCmd) && nrows > 1) {
            Tcl_SetResult(interp, "query returned more than 1 row", TCL_STATIC);
        } else if (cmd == CDmlCmd && dbistat != DBI_EXEC_DML) {
            Tcl_SetResult(interp, "query was not a DML or DDL command", TCL_STATIC);
        } else if (cmd == CDmlCmd) {
            Tcl_SetIntObj(Tcl_GetObjResult(interp), nrows);
            status = TCL_OK;
        } else if (nrows > 0) {
            status = RowResult(interp, handle);
        } else {
            status = TCL_OK; /* 0or1row, no rows */
        }

        done:
        if (idataPtr->handle == NULL) {
            Dbi_PutHandle(handle);
        } else {
            Dbi_Flush(handle);
        }
    }
        break;

    case CTransactionCmd:
    case CWithHandleCmd: {

        Tcl_Obj         *scriptObj;
        DBI_EXEC_STATUS  dbistat;

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
        if (Ns_ParseObjv(opts, args, interp, 2, objc, objv) != NS_OK) {
            return TCL_ERROR;
        }

        /*
         * Don't allow nested calls to this Tcl command.  We might support
         * true nested transactions one day and don't want people getting
         * the wrong idea.
         */

        if (idataPtr->handle != NULL) {
            Tcl_AppendResult(interp, "nested call to ",
                             Tcl_GetString(objv[0]), " ",
                             Tcl_GetString(objv[1]), NULL);
            return TCL_ERROR;
        }

        /*
         * Grab a free handle.
         */

        if ((pool = GetPool(idataPtr, poolObj)) == NULL
            || (handle = GetHandle(idataPtr, pool, timeoutPtr)) == NULL) {
            return TCL_ERROR;
        }


        if (cmd == CTransactionCmd) {

            /*
             * Start a transaction.
             */

            dbistat = Dbi_ExecDirect(handle, "begin transaction");
            if (dbistat != DBI_EXEC_DML) {
                SqlError(interp, handle);
                Dbi_PutHandle(handle);
                return TCL_ERROR;
            }
            Dbi_Flush(handle);
            idataPtr->transaction = 1;
        }

        /*
         * Cache the handle and run the script.
         */

        idataPtr->handle = handle;
        status = Tcl_EvalObjEx(interp, scriptObj, 0);
        idataPtr->handle = NULL;
        idataPtr->transaction = 0;

        /*
         * Commit or rollback an active transaction.
         */

        if (cmd == CTransactionCmd) {
            if (status != TCL_OK) {
                Tcl_AddErrorInfo(interp, "\n    dbi transaction status:\nrollback");
                dbistat = Dbi_ExecDirect(handle, "rollback");
                if (dbistat != DBI_EXEC_DML) {
                    SqlError(interp, handle);
                    status = TCL_ERROR;
                }
            } else {
                dbistat = Dbi_ExecDirect(handle, "commit");
                if (dbistat != DBI_EXEC_DML) {
                    SqlError(interp, handle);
                    status = TCL_ERROR;
                }
            }
        }

        Dbi_PutHandle(handle);

        return status;
    }
        break;

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
    
    case CStatsCmd:
    case CBounceCmd:
    case CInfoCmd:
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

    return status;
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
        pool = Dbi_DefaultPool(idataPtr->server);
        if (pool == NULL) {
            Tcl_SetResult(idataPtr->interp,
                "no pool specified and no default configured", TCL_STATIC);
            return NULL;
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
