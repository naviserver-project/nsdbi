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
 * Static functions defined in this file
 */

static Tcl_ObjCmdProc TclDbiCmd;

static Dbi_Pool *
GetPool(ServerData *sdataPtr, Tcl_Interp *interp, Tcl_Obj *poolObj)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

static int
BindVars(Tcl_Interp *interp, Dbi_Statement *stmt,
         CONST char **values, unsigned int *lengths,
         char *array, char *setid)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

static int
RowResult(Tcl_Interp *interp, Dbi_Handle *handle)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

static int
Exception(Tcl_Interp *interp, CONST char *code, CONST char *msg)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);

#define SqlException(interp, handle)                   \
    Exception((interp), Dbi_ExceptionCode((handle)),   \
              Dbi_ExceptionMsg((handle)))


/*
 * Static variables defined in this file.
 */

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
 * DbiInitInterp --
 *
 *      Add the dbi commands and traces to an initialising interp.
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
    char       *server = arg;
    ServerData *sdataPtr;
    int         i;

    sdataPtr = DbiGetServer(server);

    Tcl_CreateObjCommand(interp, "dbi", TclDbiCmd, sdataPtr, NULL);

    /*
     * Add traces to release db handles before lengthy IO
     * opperations begin.
     */

    for (i = 0; blockingCmds[i] != NULL; ++i) {
        Tcl_VarEval(interp, "trace add execution ", blockingCmds[i],
                    " enter dbi releasehandles", NULL);
    }

    return TCL_OK;
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
    ServerData       *sdataPtr = arg;
    CONST char       *server = sdataPtr->server;
    Dbi_Pool         *pool;
    Dbi_Handle       *handle;
    Dbi_Statement    *stmt;
    Ns_DString        ds;
    DBI_EXEC_STATUS   dbistat;
    CONST char       *values[DBI_MAX_BIND];
    unsigned int      lengths[DBI_MAX_BIND];
    Ns_Conn          *conn = NULL;
    char             *sql, *array = NULL, *setid = NULL;
    Tcl_Obj          *sqlObj, *poolObj = NULL;
    Ns_Time           time, *timeoutPtr = NULL;
    int               cmd, length, n, nrows, status = TCL_OK;

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

    static CONST char *cmds[] = {
        "0or1row", "1row", "bounce", "default", "dml", "info",
        "pools", "releasehandles", "rows", "stats", NULL
    };
    enum CmdIdx {
        C0or1rowCmd, C1rowCmd, CBounceCmd, CDefaultCmd, CDmlCmd, CInfoCmd,
        CPoolsCmd, CReleasehandlesCmd, CRowsCmd, CStatsCmd
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
        pool = Dbi_DefaultPool(server);
        if (pool != NULL) {
            Tcl_SetStringObj(Tcl_GetObjResult(interp), Dbi_PoolName(pool), -1);
        }
        break;

    case CPoolsCmd:
        Ns_DStringInit(&ds);
        if (Dbi_ListPools(&ds, server) != NS_OK) {
            return TCL_ERROR;
        }
        Tcl_DStringResult(interp, &ds);
        break;

    case CReleasehandlesCmd:
        conn = Ns_TclGetConn(interp);
        n = 0;
        if (conn != NULL) {
            n = Dbi_ReleaseConnHandles(conn);
        }
        Tcl_SetIntObj(Tcl_GetObjResult(interp), n);
        break;
    
    case CStatsCmd:
    case CBounceCmd:
    case CInfoCmd:
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "pool");
            return TCL_ERROR;
        }
        if ((pool = GetPool(sdataPtr, interp, objv[2])) == NULL) {
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

    default:

        /*
         * Parse common options for commands which run queries.
         */

        if (Ns_ParseObjv(opts, args, interp, 2, objc, objv) != NS_OK) {
            return TCL_ERROR;
        }

        /*
         * Grab a free handle.
         */

        if ((pool = GetPool(sdataPtr, interp, poolObj)) == NULL) {
            return TCL_ERROR;
        }
        conn = Ns_TclGetConn(interp);
        if (timeoutPtr != NULL) {
            timeoutPtr = Ns_AbsoluteTime(&time, timeoutPtr);
        }
        status = Dbi_GetHandle(&handle, pool, conn, timeoutPtr);
        if (status == NS_TIMEOUT) {
            Exception(interp, "NS_TIMEOUT", "wait for database handle timed out");
            return TCL_ERROR;
        } else if (status != NS_OK) {
            Exception(interp, NULL, "database handle allocation failed");
            return TCL_ERROR;
        }

        /*
         * Prepare the statement for our handle.
         */

        sql = Tcl_GetStringFromObj(sqlObj, &length);
        stmt = Dbi_Prepare(handle, sql, length);
        if (stmt == NULL) {
            status = SqlException(interp, handle);
            Dbi_ResetException(handle);
            goto done;
        }

        /*
         * Bind values to variable as required.
         */

        n = BindVars(interp, stmt, values, lengths, array, setid);
        if (n < 0) {
            status = TCL_ERROR;
            goto done;
        }

        /*
         * Execute the statement.
         */

        dbistat = Dbi_Exec(handle, stmt, n ? values : NULL, n ? lengths : NULL);

        if (dbistat == DBI_EXEC_ERROR) {
            status = SqlException(interp, handle);
            Dbi_ResetException(handle);
            goto done;
        }

        nrows = Dbi_NumRows(handle);

        /*
         * The query commands are assertions about the result set.
         */

        switch (cmd) {

        case CDmlCmd:
            if (dbistat != DBI_EXEC_DML) {
                status = Exception(interp, NULL, "query was not a DML or DDL command");
            } else {
                Tcl_SetIntObj(Tcl_GetObjResult(interp), nrows);
                status = TCL_OK;
            }
            break;

        case C0or1rowCmd:
            if (dbistat != DBI_EXEC_ROWS) {
                status = Exception(interp, NULL, "query was not a statement returning rows");
            } else if (nrows > 1) {
                status = Exception(interp, NULL, "query returned more than 1 row");
            } else if (nrows == 1) {
                status = RowResult(interp, handle);
            }
            break;

        case C1rowCmd:
            if (dbistat != DBI_EXEC_ROWS || nrows == 0) {
                status = Exception(interp, NULL, "query was not a statement returning rows");
            } else if (nrows > 1) {
                status = Exception(interp, NULL, "query returned more than 1 row");
            } else {
                status = RowResult(interp, handle);
            }
            break;

        case CRowsCmd:
            if (dbistat != DBI_EXEC_ROWS) {
                status = Exception(interp, NULL, "query was not a statement returning rows");
            } else if (nrows > 0) {
                status = RowResult(interp, handle);
            }
            break;
        }

        /*
         * Release the handle back to it's pool unless there's a connection
         * active to cache it in.
         */

    done:
        Dbi_Flush(handle);
        if (conn != NULL) {
            Dbi_Reset(handle);
        } else {
            Dbi_PutHandle(handle);
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
GetPool(ServerData *sdataPtr, Tcl_Interp *interp, Tcl_Obj *poolObj)
{
    Dbi_Pool    *pool;
    const char  *poolType = "dbi:pool";

    if (poolObj == NULL) {
        pool = sdataPtr->defpoolPtr;
        if (pool == NULL) {
            Tcl_AppendToObj(Tcl_GetObjResult(interp),
                "no pool specified and no default configured",
                -1);
            return NULL;
        }
    } else if (Ns_TclGetOpaqueFromObj(poolObj, poolType, (void **) &pool) != TCL_OK) {
        pool = DbiGetPool(sdataPtr, Tcl_GetString(poolObj));
        if (pool == NULL) {
            Tcl_AppendToObj(Tcl_GetObjResult(interp),
                "invalid pool name or pool not available to virtual server",
                -1);
            return NULL;
        }
        Ns_TclSetOpaqueObj(poolObj, poolType, pool);
    }

    return pool;
}


/*
 *----------------------------------------------------------------------
 * BindVars --
 *
 *      Bind values to the variables of a statement, looking at the keys
 *      of the dictionary if given, or local variables otherwise.
 *
 * Results:
 *      Number of variables bound, or -1 on error.
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
        return 0;
    }

    if (setid != NULL) {
        if (Ns_TclGetSet2(interp, setid, &set) != TCL_OK) {
            return -1;
        }
    }

    for (i = 0; i < nbind; i++) {

        if (Dbi_GetBindVariable(stmt, i, &key) != NS_OK) {
            Ns_Log(Bug, "dbi: BindVars: bind variable out of range");
            return -1;
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
            return -1;
        }

        values[i] = value;
        lengths[i] = length;
    }

    return nbind;
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
            Exception(interp, Dbi_ExceptionCode(handle),
                      Dbi_ExceptionMsg(handle));
            Dbi_ResetException(handle);
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
    Tcl_Obj *objPtr;

    objPtr = Tcl_NewStringObj("DBI", -1);
    if (code != NULL) {
        Tcl_ListObjAppendElement(interp, objPtr, Tcl_NewStringObj((char *) code, -1));
    }
    Tcl_SetObjErrorCode(interp, objPtr);
    Tcl_SetStringObj(Tcl_GetObjResult(interp), msg, -1);

    return TCL_ERROR;
}
