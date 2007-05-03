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
    FormatObjCmd,
    DmlObjCmd,
    ZeroOrOneRowObjCmd,
    OneRowObjCmd,
    EvalObjCmd,
    TransactionObjCmd,
    CtlObjCmd;

static void FreeData(ClientData arg, Tcl_Interp *interp);

static int RowCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[],
                  int *rowPtr);
static int EvalCmd(ClientData arg, Tcl_Interp *interp,
                   int objc, Tcl_Obj *CONST objv[],
                   int transaction);

static int Exec(InterpData *idataPtr, Tcl_Obj *poolObj, Ns_Time *timeoutPtr,
                CONST char *array, CONST char *setid, Tcl_Obj *queryObj, int dml,
                Dbi_Handle **handlePtrPtr);
static int ExecDirect(Tcl_Interp *interp, Dbi_Handle *handle, CONST char *sql);
static int BindVars(Tcl_Interp *interp, Dbi_Handle *,
                    CONST char **values, unsigned int *lengths,
                    CONST char *array, CONST char *setid);

static Dbi_Pool *GetPool(InterpData *, Tcl_Obj *poolObj);
static Dbi_Handle *GetHandle(InterpData *, Dbi_Pool *, Ns_Time *);
static void CleanupHandle(InterpData *idataPtr, Dbi_Handle *handle);

static void SqlError(Tcl_Interp*, Dbi_Handle *);

static int NextValue(Dbi_Handle *handle, Tcl_Obj **valueObjPtrPtr,
                     unsigned int *colIdxPtr, unsigned int *rowIdxPtr);

/*
 * Static variables defined in this file.
 */

static Tcl_ObjCmdProc *formatCmd; /* Tcl 'format' command. */



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
    Tcl_CmdInfo  info;
    int          i;
    static int   once = 0;

    if (!once) {
        once = 1;
        if (!Tcl_GetCommandInfo(interp, "format", &info)) {
            Ns_TclLogError(interp);
            return TCL_ERROR;
        }
        formatCmd = info.objProc;
    }

    static struct {
        CONST char     *name;
        Tcl_ObjCmdProc *proc;
    } cmds[] = {
        {"dbi_rows",        RowsObjCmd},
        {"dbi_format",      FormatObjCmd},
        {"dbi_0or1row",     ZeroOrOneRowObjCmd},
        {"dbi_1row",        OneRowObjCmd},
        {"dbi_dml",         DmlObjCmd},
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
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    Tcl_Obj      *resObj, *valueObj, *queryObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    CONST char   *array = NULL, *setid = NULL;
    int           status = TCL_ERROR;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bindarray", Ns_ObjvString, &array,      NULL},
        {"-bindset",   Ns_ObjvString, &setid,      NULL},
        {"--",         Ns_ObjvBreak,  NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"query",      Ns_ObjvObj, &queryObj, NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    /*
     * Get a handle, prepare, bind, and run the qeuery.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, array, setid, queryObj, 0,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Successful result is a flat list of all values (or an empty list)
     */

    resObj = Tcl_GetObjResult(interp);
    while (1) {
        switch (NextValue(handle, &valueObj, NULL, NULL)) {
        case DBI_VALUE:
            if (Tcl_ListObjAppendElement(interp, resObj, valueObj) != TCL_OK) {
                Tcl_DecrRefCount(valueObj);
                goto done;
            }
            break;

        case DBI_DONE:
            status = TCL_OK;
            goto done;

        default:
            SqlError(interp, handle);
            goto done;
        }
    }

 done:
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
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    Tcl_Obj      *queryObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    CONST char   *array = NULL, *setid = NULL;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bindarray", Ns_ObjvString, &array,      NULL},
        {"-bindset",   Ns_ObjvString, &setid,      NULL},
        {"--",         Ns_ObjvBreak,  NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"query",      Ns_ObjvObj, &queryObj, NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    /*
     * Get a handle, prepare, bind, and run the qeuery.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, array, setid, queryObj, 1,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /* FIXME: a clean way of returning the number of rows affected. */

    CleanupHandle(idataPtr, handle);

    return TCL_OK;
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
    int foundRow;

    return RowCmd(arg, interp, objc, objv, &foundRow);
}

static int
OneRowObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int foundRow;

    if (RowCmd(arg, interp, objc, objv, &foundRow) != TCL_OK) {
        return TCL_ERROR;
    }
    if (!foundRow) {
        Tcl_SetResult(interp, "query was not a statement returning rows",
                      TCL_STATIC);
        return TCL_ERROR;
    }
    return TCL_OK;
}

static int
RowCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[],
       int *foundRowPtr)
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    unsigned int  colIdx, rowIdx;
    Tcl_Obj      *valueObj, *queryObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    CONST char   *column, *array = NULL, *setid = NULL;
    int           found = 0, status = TCL_ERROR;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bindarray", Ns_ObjvString, &array,      NULL},
        {"-bindset",   Ns_ObjvString, &setid,      NULL},
        {"--",         Ns_ObjvBreak,  NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"query",      Ns_ObjvObj, &queryObj, NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    /*
     * Get handle, then prepare, bind, and run the qeuery.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, array, setid, queryObj, 0,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Make sure the result has no more than a single row in the result
     * and set the values of that row as variables in the callers frame..
     */

    while (1) {

        switch (NextValue(handle, &valueObj, &colIdx, &rowIdx)) {

        case DBI_DONE:
            status = TCL_OK;
            goto done;

        case DBI_VALUE:
            if (rowIdx == 1) {
                Tcl_SetResult(interp, "query returned more than one row", TCL_STATIC);
                goto done;
            }
            if (Dbi_ColumnName(handle, colIdx, &column) != NS_OK) {
                Tcl_DecrRefCount(valueObj);
                SqlError(interp, handle);
                goto done;
            }
            if (Tcl_SetVar2Ex(interp, column, NULL, valueObj,
                              TCL_LEAVE_ERR_MSG) == NULL) {
                Tcl_DecrRefCount(valueObj);
                goto done;
            }
            found++;
            break;

        default:
            SqlError(interp, handle);
            goto done;
        }
    }

 done:
    *foundRowPtr = found ? 1 : 0;
    CleanupHandle(idataPtr, handle);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * FormatObjCmd --
 *
 *      Implements dbi_format.
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
FormatObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    Tcl_Obj     **vobjv;
    int           vobjc;
    unsigned int  numCols, colIdx = 0, rowIdx = 0;
    Tcl_Obj      *resObj, *valueObj, *valueListObj;
    Tcl_Obj      *queryObj, *formatObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    char         *array = NULL, *setid = NULL;
    int           found, dbistat, status = TCL_ERROR;

    Ns_ObjvSpec opts[] = {
        {"-pool",        Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",     Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bindarray",   Ns_ObjvString, &array,      NULL},
        {"-bindset",     Ns_ObjvString, &setid,      NULL},
        {"--",           Ns_ObjvBreak,  NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"query",        Ns_ObjvObj,    &queryObj,   NULL},
        {"formatString", Ns_ObjvObj,    &formatObj,  NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    /*
     * Get handle then prepare, bind, and run the qeuery.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, array, setid, queryObj, 0,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Format each row and append to result.
     */

    numCols      = Dbi_NumColumns(handle);
    resObj       = Tcl_NewObj();
    valueListObj = Tcl_NewListObj(1, objv);
    if (Tcl_ListObjAppendElement(interp, valueListObj, formatObj)
            != TCL_OK) {
        goto done;
    }

    while (1) {
        found = 0;
        do {
            dbistat = NextValue(handle, &valueObj, &colIdx, &rowIdx);

            if (dbistat == DBI_ERROR) {
                SqlError(interp, handle);
                goto done;
            }
            if (dbistat == DBI_DONE) {
                break;
            }

            if (Tcl_ListObjAppendElement(interp, valueListObj, valueObj)
                    != TCL_OK) {
                Tcl_DecrRefCount(valueObj);
                goto done;
            }
            found++;

        } while (colIdx +1 != numCols);

        if (found) {
            if (Tcl_ListObjGetElements(interp, valueListObj, &vobjc, &vobjv) != TCL_OK
                    || (*formatCmd)(NULL, interp, vobjc, vobjv) != TCL_OK) {
                goto done;
            }
            Tcl_AppendObjToObj(resObj, Tcl_GetObjResult(interp));

            if (Tcl_ListObjReplace(interp, valueListObj, 2, numCols,
                                   0, NULL) != TCL_OK) {
                goto done;
            }
        } else {
            break;
        }
    }
    status = TCL_OK;

/*     do { */
/*         do { */
/*             if (NextValue(handle, &valueObj, &colIdx, &rowIdx) != TCL_OK) { */
/*                 SqlError(interp, handle); */
/*                 goto done; */
/*             } */
/*             if (valueObj != NULL) { */
/*                 if (Tcl_ListObjAppendElement(interp, valueListObj, valueObj) */
/*                         != TCL_OK) { */
/*                     Tcl_DecrRefCount(valueObj); */
/*                     goto done; */
/*                 } */
/*             } */

/*         } while (valueObj != NULL && (colIdx +1 != numCols)); */

/*         if (Tcl_ListObjGetElements(interp, valueListObj, &vobjc, &vobjv) != TCL_OK */
/*                || (*formatCmd)(NULL, interp, vobjc, vobjv) != TCL_OK) { */
/*             goto done; */
/*         } */
/*         Tcl_AppendObjToObj(resObj, Tcl_GetObjResult(interp)); */

/*         if (Tcl_ListObjReplace(interp, valueListObj, 2, numCols, */
/*                                0, NULL) != TCL_OK) { */
/*             goto done; */
/*         } */

/*     } while (valueObj != NULL); */

 done:
    CleanupHandle(idataPtr, handle);

    if (status == TCL_OK) {
        Tcl_SetObjResult(interp, resObj);
    } else {
        Tcl_DecrRefCount(resObj);
    }
    Tcl_DecrRefCount(valueListObj);

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
        if (ExecDirect(interp, handle, "begin") != TCL_OK) {
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
    if (Dbi_ExecDirect(handle, sql) != NS_OK) {
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
    int         cmd, oldValue, newValue;

    static CONST char *cmds[] = {
        "bounce", "database", "default", "driver",
        "maxhandles", "maxidle", "maxopen", "maxqueries", "maxwait",
        "pools", "stats", NULL
    };
    enum CmdIdx {
        CBounceCmd, CDatabaseCmd, CDefaultCmd, CDriverCmd,
        CMaxHandlesCmd, CMaxIdleCmd, CMaxOpenCmd, CMaxQueriesCmd, CMaxWaitCmd,
        CPoolsCmd, CStatsCmd
    };
    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "command ?args?");
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[1], cmds, "command", 0,
                            &cmd) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * The pools commands require no other arguments.
     */

    switch (cmd) {
    case CPoolsCmd:
        Ns_DStringInit(&ds);
        if (Dbi_ListPools(&ds, idataPtr->server) != NS_OK) {
            return TCL_ERROR;
        }
        Tcl_DStringResult(interp, &ds);
        return TCL_OK;

    case CDefaultCmd:
        pool = Dbi_DefaultPool(idataPtr->server);
        if (pool != NULL) {
            Tcl_SetResult(interp, (char *) Dbi_PoolName(pool), TCL_VOLATILE);
        }
        if (objc == 3) {
            
        }
        return TCL_OK;
    }

    /*
     * All other commands require a pool to opperate on.
     */

    if (objc != 3 && objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "pool ?args?");
        return TCL_ERROR;
    }
    if ((pool = GetPool(idataPtr, objv[2])) == NULL) {
        return TCL_ERROR;
    }

    switch (cmd) {
    case CBounceCmd:
        Dbi_BouncePool(pool);
        return TCL_OK;

    case CDriverCmd:
        Tcl_SetResult(interp, (char *) Dbi_DriverName(pool), TCL_VOLATILE);
        return TCL_OK;

    case CDatabaseCmd:
        Tcl_SetResult(interp, (char *) Dbi_DatabaseName(pool), TCL_VOLATILE);
        return TCL_OK;

    case CStatsCmd:
        Ns_DStringInit(&ds);
        Dbi_Stats(&ds, pool);
        Tcl_DStringResult(interp, &ds);
        return TCL_OK;
    }

    /*
     * The remaining commands all take an optional argument which
     * is the new value of the configuration parameter.
     */

    if (objc == 4) {
        if (Tcl_GetIntFromObj(interp, objv[3], &newValue) != TCL_OK) {
            return TCL_ERROR;
        }
    } else {
        newValue = -1;
    }

    switch (cmd) {
    case CMaxHandlesCmd:
        oldValue = Dbi_Config(pool, DBI_CONFIG_MAXHANDLES, newValue);
        break;

    case CMaxWaitCmd:
        oldValue = Dbi_Config(pool, DBI_CONFIG_MAXWAIT, newValue);
        break;

    case CMaxIdleCmd:
        oldValue = Dbi_Config(pool, DBI_CONFIG_MAXIDLE, newValue);
        break;

    case CMaxOpenCmd:
        oldValue = Dbi_Config(pool, DBI_CONFIG_MAXOPEN, newValue);
        break;

    case CMaxQueriesCmd:
        oldValue = Dbi_Config(pool, DBI_CONFIG_MAXQUERIES, newValue);
        break;
    }
    Tcl_SetIntObj(Tcl_GetObjResult(interp), oldValue);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Exec --
 *
 *      Get a handle, prepare, bind, and execute an SQL statement.
 *
 * Results:
 *      TCL_OK or TCL_ERROR. handlePtrPtr updated with active db handle
 *      on successfull return.
 *
 * Side effects:
 *      Error message may be left in interp.
 *      Prepared statement may be cached. See: Dbi_Exec().
 *
 *----------------------------------------------------------------------
 */

static int
Exec(InterpData *idataPtr, Tcl_Obj *poolObj, Ns_Time *timeoutPtr,
     CONST char *array, CONST char *setid, Tcl_Obj *queryObj, int dml,
     Dbi_Handle **handlePtrPtr)
{
    Tcl_Interp       *interp = idataPtr->interp;
    Dbi_Pool         *pool;
    Dbi_Handle       *handle;
    CONST char       *values[DBI_MAX_BIND];
    unsigned int      lengths[DBI_MAX_BIND];
    unsigned int      numCols;
    char             *query;
    int               qlength;

    /*
     * Grab a free handle, possibly from the interp cache.
     */

    if ((pool = GetPool(idataPtr, poolObj)) == NULL
            || (handle = GetHandle(idataPtr, pool, timeoutPtr)) == NULL) {
        return TCL_ERROR;
    }
    *handlePtrPtr = handle;

    /*
     * Prepare the statement for our handle and check that it is the
     * expected kind of statement.
     */

    query = Tcl_GetStringFromObj(queryObj, &qlength);

    if (Dbi_Prepare(handle, query, qlength) != NS_OK) {
        SqlError(interp, handle);
        goto error;
    }

    numCols = Dbi_NumColumns(handle);

    if (dml && numCols > 0) {
        Tcl_SetResult(interp, "query was not a DML or DDL command.",
                      TCL_STATIC);
        goto error;
    } else if (!dml && numCols == 0) {
        Tcl_SetResult(interp, "query was not a statment returning rows.",
                      TCL_STATIC);
        goto error;
    }

    /*
     * Bind values to variable as required and execute the statement.
     */

    if (BindVars(interp, handle, values, lengths, array, setid) != TCL_OK) {
        goto error;
    }
    if (Dbi_Exec(handle, values, lengths) != NS_OK) {
        SqlError(interp, handle);
        goto error;
    }

    return TCL_OK;

 error:
    CleanupHandle(idataPtr, handle);

    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * BindVars --
 *
 *      Bind values to the variables of a statement, looking at the keys
 *      of the array or set if given, or local variables otherwise.
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
BindVars(Tcl_Interp *interp, Dbi_Handle *handle,
         CONST char **values, unsigned int *lengths,
         CONST char *array, CONST char *setid)
{
    Ns_Set         *set = NULL;
    Tcl_Obj        *valueObj;
    CONST char     *key, *value;
    unsigned int    nbind;
    int             i, length;

    if (!(nbind = Dbi_NumVariables(handle))) {
        return TCL_OK;
    }

    if (setid != NULL) {
        if (Ns_TclGetSet2(interp, (char *) setid, &set) != TCL_OK) {
            return TCL_ERROR;
        }
    }

    for (i = 0; i < nbind; i++) {

        if (Dbi_VariableName(handle, i, &key) != NS_OK) {
            SqlError(interp, handle);
            return TCL_ERROR;
        }

        value = NULL;

        if (set != NULL) {
            if ((value = Ns_SetGet(set, key)) != NULL) {
                length = strlen(value);
            }
        } else {
            valueObj = Tcl_GetVar2Ex(interp, array ? array : key,
                                     array ? key : NULL, TCL_LEAVE_ERR_MSG);
            if (valueObj != NULL) {
                value = Tcl_GetStringFromObj(valueObj, &length);
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

    switch (Dbi_GetHandle(pool, timeoutPtr, &handle)) {
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
         * Handle was acquired for this command only -- return to pool.
         */
        Dbi_PutHandle(handle);
    }
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

static int
NextValue(Dbi_Handle *handle, Tcl_Obj **valueObjPtrPtr,
          unsigned int *colIdxPtr, unsigned int *rowIdxPtr)
{
    Dbi_Value value;

    assert(handle);
    assert(valueObjPtrPtr);


    switch (Dbi_NextValue(handle, &value, colIdxPtr, rowIdxPtr)) {

    case DBI_VALUE:
        *valueObjPtrPtr = value.binary
            ? Tcl_NewByteArrayObj(value.data, value.length)
            : Tcl_NewStringObj(value.data, value.length);
        return DBI_VALUE;

    case DBI_DONE:
        *valueObjPtrPtr = NULL;
        return DBI_DONE;

    default:
        return DBI_ERROR;
    }
}
