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


#define MAX_NESTING_DEPTH 32


/*
 * The following struct maintains state for the currently
 * executing command.
 *
 * The handle cache may contain the same handle in more than
 * one entry as 'depth' is used as a form of reference counting.
 */

typedef struct InterpData {
    Tcl_Interp *interp;
    CONST char *server;
    int         depth;                      /* Nesting depth for dbi_eval */
    Dbi_Handle *handles[MAX_NESTING_DEPTH]; /* Handle cache, indexed by depth. */
} InterpData;


/*
 * Static functions defined in this file
 */

static Tcl_ObjCmdProc
    RowsObjCmd,
    FormatObjCmd,
    Format2ObjCmd,
    SubstObjCmd,
    Subst2ObjCmd,
    DmlObjCmd,
    ZeroOrOneRowObjCmd,
    OneRowObjCmd,
    EvalObjCmd,
    CtlObjCmd;

static void FreeData(ClientData arg, Tcl_Interp *interp);

static int RowCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[],
                  int *rowPtr);

static int Exec(InterpData *idataPtr, Tcl_Obj *poolObj, Ns_Time *timeoutPtr,
                CONST char *array, CONST char *setid, Tcl_Obj *queryObj, int dml,
                Dbi_Handle **handlePtrPtr);
static int BindVars(Tcl_Interp *interp, Dbi_Handle *, Dbi_Value *values,
                    CONST char *array, CONST char *setid);

static Dbi_Pool *GetPool(InterpData *, Tcl_Obj *poolObj);
static Dbi_Handle *GetHandle(InterpData *, Dbi_Pool *, Ns_Time *);
static void CleanupHandle(InterpData *idataPtr, Dbi_Handle *handle);

static void SqlError(Tcl_Interp*, Dbi_Handle *);

/* static int NextValue(Dbi_Handle *handle, Tcl_Obj **valueObjPtrPtr, */
/*                      unsigned int *colIdxPtr, unsigned int *rowIdxPtr); */
static int NextValue(Tcl_Interp *interp, Dbi_Handle *handle, Tcl_Obj **valueObjPtr, unsigned int *colIdxPtr);

static int AppendFormatToObj(Tcl_Interp *interp, Tcl_Obj *appendObj, CONST char *format,
                             int objc, Tcl_Obj *CONST objv[]);
/*
 * Static variables defined in this file.
 */

static Tcl_ObjCmdProc *formatCmd;        /* Tcl 'format' command. */
static Tcl_ObjType    *bytearrayTypePtr;

/*
 * The following are the values that can be passed to the
 * dbi_eval '-transaction' option.
 */

static Ns_ObjvTable levels[] = {
    {"readuncommitted", Dbi_ReadUncommitted}, {"uncommitted", Dbi_ReadUncommitted},
    {"readcommitted",   Dbi_ReadCommitted},   {"committed",   Dbi_ReadCommitted},
    {"repeatableread",  Dbi_RepeatableRead},  {"repeatable",  Dbi_RepeatableRead},
    {"serializable",    Dbi_Serializable},
    {NULL, 0}
};

/*
 * The following defines the subst object type to cache
 * a subst-spec internal rep for the dbi_subst2 command.
 */

static void PushTextToken(Tcl_Parse *parsePtr, char *string, int length);
static int GetParseFromObj(Tcl_Interp *interp, Tcl_Obj *substObj,
                           Tcl_Parse **parsePtrPtr, int *numVarsPtr);
static void MapVariablesToColumns(Dbi_Handle *handle, Tcl_Parse *parsePtr,
                                  int *varColMap);
static Tcl_FreeInternalRepProc FreeSubst;

static Tcl_ObjType substType = {
    "dbi:subst",
    FreeSubst,
    (Tcl_DupInternalRepProc *) NULL,
    (Tcl_UpdateStringProc *) NULL,
    Ns_TclSetFromAnyError
};


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

        bytearrayTypePtr = Tcl_GetObjType("bytearray");
        if (bytearrayTypePtr == NULL) {
            Tcl_Panic("dbi: \"bytearray\" type not defined");
        }
    }

    static struct {
        CONST char     *name;
        Tcl_ObjCmdProc *proc;
    } cmds[] = {
        {"dbi_rows",        RowsObjCmd},
        {"dbi_format",      FormatObjCmd},
        {"dbi_format2",     Format2ObjCmd},
        {"dbi_subst",       SubstObjCmd},
        {"dbi_subst2",      Subst2ObjCmd},
        {"dbi_0or1row",     ZeroOrOneRowObjCmd},
        {"dbi_1row",        OneRowObjCmd},
        {"dbi_dml",         DmlObjCmd},
        {"dbi_eval",        EvalObjCmd},
        {"dbi_ctl",         CtlObjCmd}
    };

    idataPtr = ns_calloc(1, sizeof(InterpData));
    idataPtr->server = server;
    idataPtr->interp = interp;
    idataPtr->depth = -1;
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
    int           status;

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

    while ((status = NextValue(interp, handle, &valueObj, NULL)) == TCL_OK
               && valueObj != NULL) {
        if (Tcl_ListObjAppendElement(interp, resObj, valueObj) != TCL_OK) {
            Tcl_DecrRefCount(valueObj);
            status = TCL_ERROR;
            break;
        }
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
    Dbi_Value     value;
    unsigned int  colIdx, colMax;
    Tcl_Obj      *valueObj, *queryObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    CONST char   *column, *array = NULL, *setid = NULL;
    int           found, end, status;

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

    colMax = Dbi_NumColumns(handle) -1;
    found  = 0;

    do {
        if ((status = NextValue(interp, handle, &valueObj, &colIdx)) != TCL_OK
                || valueObj == NULL) {
            goto cleanup;
        }
        if (Dbi_ColumnName(handle, colIdx, &column) != NS_OK) {
            SqlError(interp, handle);
            goto cleanup;
        }
        if (Tcl_SetVar2Ex(interp, column, NULL, valueObj,
                          TCL_LEAVE_ERR_MSG) == NULL) {
            Tcl_DecrRefCount(valueObj);
            goto cleanup;
        }
        found++;

    } while (colIdx < colMax);

    /*
     * Expect end=1 when we attempt to fetch from the next row.
     */

    if (Dbi_NextValue(handle, &value, &end) != NS_OK) {
        SqlError(interp, handle);
        goto cleanup;
    }
    if (!end) {
        Tcl_SetResult(interp, "query returned more than one row", TCL_STATIC);
        goto cleanup;
    }

    status = TCL_OK;

 cleanup:

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
    unsigned int  colMax, colIdx;
    Tcl_Obj      *resObj, *valueObj, *valueListObj;
    Tcl_Obj      *queryObj, *formatObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    char         *array = NULL, *setid = NULL;
    int           status = TCL_ERROR;

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

    colMax = Dbi_NumColumns(handle) -1;

    resObj = Tcl_NewObj();
    Tcl_IncrRefCount(resObj);

    valueListObj = Tcl_NewListObj(1, objv);
    if (Tcl_ListObjAppendElement(interp, valueListObj, formatObj)
            != TCL_OK) {
        goto cleanup;
    }

    while (1) {

        /*
         * Append a row of objects to the format command arg list.
         */

        do {
            if (NextValue(interp, handle, &valueObj, &colIdx) != TCL_OK) {
                goto cleanup;
            }
            if (valueObj == NULL) {
                if (colIdx == 0) {
                    status = TCL_OK;
                }
                goto done;
            }
            if (Tcl_ListObjAppendElement(interp, valueListObj, valueObj)
                    != TCL_OK) {
                Tcl_DecrRefCount(valueObj);
                goto cleanup;
            }

        } while (colIdx != colMax);

        /*
         * Evaluate the constructed format command.
         */

        if (Tcl_ListObjGetElements(interp, valueListObj, &vobjc, &vobjv) != TCL_OK
                || (*formatCmd)(NULL, interp, vobjc, vobjv) != TCL_OK) {
            goto cleanup;
        }
        Tcl_AppendObjToObj(resObj, Tcl_GetObjResult(interp));

        /*
         * Reset the format arg list, leaving the formatObj intact
         * at the head.
         */

        if (Tcl_ListObjReplace(interp, valueListObj, 2, colMax+1,
                               0, NULL) != TCL_OK) {
            goto cleanup;
        }
    }

 done:
    Tcl_SetObjResult(interp, resObj);
    status = TCL_OK;

 cleanup:
    CleanupHandle(idataPtr, handle);
    Tcl_DecrRefCount(valueListObj);
    Tcl_DecrRefCount(resObj);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Format2ObjCmd --
 *
 *      Implements dbi_format2.
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
Format2ObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    Tcl_Obj     **vobjv;
    int           vobjc;
    unsigned int  colMax, colIdx;
    Tcl_Obj      *resObj, *valueObj, *valueListObj;
    Tcl_Obj      *queryObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    char         *format, *array = NULL, *setid = NULL;
    int           status = TCL_ERROR;

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
        {"formatString", Ns_ObjvString, &format,     NULL},
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

    colMax = Dbi_NumColumns(handle) -1;

    resObj = Tcl_NewObj();
    Tcl_IncrRefCount(resObj);
    valueListObj = Tcl_NewListObj(0, NULL);

    while (1) {

        /*
         * Append a row of objects to the format command arg list.
         */

        do {
            if (NextValue(interp, handle, &valueObj, &colIdx) != TCL_OK) {
                goto cleanup;
            }
            if (valueObj == NULL) {
                if (colIdx == 0) {
                    status = TCL_OK;
                }
                goto done;
            }
            if (Tcl_ListObjAppendElement(interp, valueListObj, valueObj)
                    != TCL_OK) {
                Tcl_DecrRefCount(valueObj);
                goto cleanup;
            }

        } while (colIdx != colMax);

        /*
         * Format the row.
         */

        if (Tcl_ListObjGetElements(interp, valueListObj, &vobjc, &vobjv) != TCL_OK
            || AppendFormatToObj(interp, resObj, format, vobjc, vobjv) != TCL_OK) {
            goto cleanup;
        }

        /*
         * Reset the format arg list.
         */

        Tcl_SetListObj(valueListObj, 0, NULL);
    }

 done:
    Tcl_SetObjResult(interp, resObj);
    status = TCL_OK;

 cleanup:
    CleanupHandle(idataPtr, handle);
    Tcl_DecrRefCount(valueListObj);
    Tcl_DecrRefCount(resObj);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * SubstObjCmd --
 *
 *      Implements dbi_subst.
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
SubstObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    unsigned int  colMax, colIdx;
    Tcl_Obj      *resObj, *rowObj, *valueObj;
    Tcl_Obj      *queryObj, *substObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    CONST char   *columnName;
    char         *array = NULL, *setid = NULL;
    int           status = TCL_ERROR;

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
        {"row-subst",    Ns_ObjvObj,    &substObj,  NULL},
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
     * Subst for each row and append to result.
     */

    colMax = Dbi_NumColumns(handle) -1;
    resObj = Tcl_NewObj();
    Tcl_IncrRefCount(resObj);

    while (1) {

        /*
         * Set each value in the row as a Tcl variable.
         */

        do {
            if (NextValue(interp, handle, &valueObj, &colIdx) != TCL_OK) {
                goto cleanup;
            }
            if (valueObj == NULL) {
                if (colIdx == 0) {
                    status = TCL_OK;
                }
                goto done;
            }
            if (Dbi_ColumnName(handle, colIdx, &columnName) != NS_OK) {
                SqlError(interp, handle);
                goto cleanup;
            }
            if (Tcl_SetVar2Ex(interp, columnName, NULL, valueObj,
                              TCL_LEAVE_ERR_MSG) == NULL) {
                Tcl_DecrRefCount(valueObj);
                goto cleanup;
            }

        } while (colIdx != colMax);

        /*
         * Subst the row variables.
         */

        rowObj = Tcl_SubstObj(interp, substObj,
                              TCL_SUBST_VARIABLES | TCL_SUBST_BACKSLASHES);
        if (rowObj == NULL) {
            goto cleanup;
        }
        Tcl_AppendObjToObj(resObj, rowObj);
    }

 done:
    Tcl_SetObjResult(interp, resObj);
    status = TCL_OK;

 cleanup:
    CleanupHandle(idataPtr, handle);
    Tcl_DecrRefCount(resObj);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Subst2ObjCmd --
 *
 *      Implements dbi_subst2.
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
Subst2ObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
/*     Dbi_Value     values[DBI_MAX_BIND], *valuePtr; */
    Tcl_Parse    *parsePtr;
    Tcl_Token    *tokenPtr;
    unsigned int  colIdx, colMax;
    Tcl_Obj      *resObj, *valueObj;
    Tcl_Obj      *queryObj, *substObj, *poolObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    CONST char   *columnName;
    char         *array = NULL, *setid = NULL;
/*     int          *varColMap = NULL; */
    int           numVarTokens, tokIdx, varIdx, status = TCL_ERROR;

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
        {"row-subst",    Ns_ObjvObj,    &substObj,  NULL},
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
     * Convert the row-subst into a stream of text/variable tokens.
     * Map the variable tokens to column indexes.
     */

    if (GetParseFromObj(interp, substObj,
                        &parsePtr, &numVarTokens) != TCL_OK) {
        goto cleanup;
    }

/*     varColMap = ns_malloc(numVarTokens * sizeof(int)); */
/*     MapVariablesToColumns(handle, parsePtr, varColMap); */

    /*
     * ...
     */

    colMax = Dbi_NumColumns(handle) -1;
    resObj = Tcl_NewObj();
    Tcl_IncrRefCount(resObj);

    while (1) {

        /*
         * Set each value in the row as a Tcl variable.
         */

        do {
            if (NextValue(interp, handle, &valueObj, &colIdx) != TCL_OK) {
                goto cleanup;
            }
            if (valueObj == NULL) {
                if (colIdx == 0) {
                    status = TCL_OK;
                }
                goto done;
            }
            if (Dbi_ColumnName(handle, colIdx, &columnName) != NS_OK) {
                SqlError(interp, handle);
                goto cleanup;
            }
            if (Tcl_SetVar2Ex(interp, columnName, NULL, valueObj,
                              TCL_LEAVE_ERR_MSG) == NULL) {
                Tcl_DecrRefCount(valueObj);
                goto cleanup;
            }

        } while (colIdx != colMax);

        /*
         * Subst the row variables.
         */

        for (tokIdx = 0; tokIdx < parsePtr->numTokens; tokIdx++) {

            tokenPtr = &parsePtr->tokenPtr[tokIdx];

            if (tokenPtr->type == TCL_TOKEN_VARIABLE) {
                if (Tcl_EvalTokensStandard(interp, tokenPtr,
                                           tokenPtr->numComponents) != TCL_OK) {
                    goto cleanup;
                }
                Tcl_AppendObjToObj(resObj, Tcl_GetObjResult(interp));
                /*
                 * Skip past the components of the variable name (e.g. array indices).
                 */
                tokIdx += tokenPtr->numComponents;
            } else {
                Tcl_AppendToObj(resObj, tokenPtr->start, tokenPtr->size);
            }
        }
    }

 done:
    Tcl_SetObjResult(interp, resObj);
    status = TCL_OK;

 cleanup:
    CleanupHandle(idataPtr, handle);
    Tcl_DecrRefCount(resObj);
/*     if (varColMap) { */
/*         ns_free(varColMap); */
/*     } */

    return status;
}

static int
GetParseFromObj(Tcl_Interp *interp, Tcl_Obj *substObj,
                Tcl_Parse **parsePtrPtr, int *numVarsPtr)
{
    Tcl_Parse  *parsePtr;
    Tcl_Token  *tokenPtr;
    char       *string, *p;
    int         length, varIdx, numVarTokens;

    /*
     * Check for cached representation.
     */

    if (substObj->typePtr == &substType) {
        *parsePtrPtr = substObj->internalRep.twoPtrValue.ptr1;
        *numVarsPtr = (int) substObj->internalRep.twoPtrValue.ptr2;
        return TCL_OK;
    }

    string = p = Tcl_GetStringFromObj(substObj, &length);

    /*
     * Initialise the parse struct as in Tcl generic/tclParse.c
     */

    parsePtr = ns_malloc(sizeof(Tcl_Parse));
	parsePtr->tokenPtr = parsePtr->staticTokens;
	parsePtr->numTokens = 0;
	parsePtr->tokensAvailable = NUM_STATIC_TOKENS;
	parsePtr->string = string;
	parsePtr->end = (string + length);
	parsePtr->interp = interp;
	parsePtr->errorType = TCL_PARSE_SUCCESS;

    /*
     * Scan the string for dollar and backslash substitutions. Variables are
     * added to the parse struct, backslash substitutions are performed now
     * and added to the parse struct as text. Everything else is added as text.
     */

    numVarTokens = 0;

    while (length) {
        switch (*p) {
        case '$':

            /*
             * First check for a pending run of text.
             */

            if (p != string) {
                PushTextToken(parsePtr, string, p-string);
                string = p;
            }

            /*
             * Check for a valid variable name.
             */

            varIdx = parsePtr->numTokens;
            if (Tcl_ParseVarName(interp, p, length, parsePtr, 1) != TCL_OK) {
                Tcl_FreeParse(parsePtr);
                ns_free(parsePtr);
                return TCL_ERROR;
            }

            tokenPtr = &parsePtr->tokenPtr[varIdx];
            if (tokenPtr->type == TCL_TOKEN_TEXT) {
                /*
                 * There isn't a variable name after all: the $ is
                 * just a $.
                 */
                parsePtr->numTokens--;
                p++; length--;
                break;
            }

            /*
             * Found a valid variable name.
             * Advance past all it's components.
             */

            numVarTokens++;

            p += tokenPtr->size;
            length -= tokenPtr->size;
            string = p;

            break;

        default:
            p++; length--;
            break;
        }
    }

    /*
     * Add any remaining trailing text, and make sure we found at
     * least one variable.
     */

    if (p != string) {
        PushTextToken(parsePtr, string, p-string);
    }

    if (numVarTokens == 0) {
        Tcl_SetResult(interp, "Invalid row-subst: no tokens", TCL_STATIC);
        Tcl_FreeParse(parsePtr);
        ns_free(parsePtr);
        return TCL_ERROR;
    }


    Ns_TclSetTwoPtrValue(substObj, &substType, parsePtr, (void *) numVarTokens);

    *parsePtrPtr = parsePtr;
    *numVarsPtr = numVarTokens;

    return TCL_OK;
}

/*
 * Map variable tokens to result column indexes.
 *
 * Variables without matching columns are marked '-1' and will
 * be substituted as Tcl variables when the result is processed.
 */

static void
MapVariablesToColumns(Dbi_Handle *handle, Tcl_Parse *parsePtr, int *varColMap)
{
    Tcl_Token  *tokenPtr;
    CONST char *tokenString, *colName;
    int         tokenSize, tokIdx, varIdx, colIdx, numCols;

    numCols = Dbi_NumColumns(handle);

    for (tokIdx = 0, varIdx = 0; tokIdx < parsePtr->numTokens; tokIdx++) {

        tokenPtr = &parsePtr->tokenPtr[tokIdx];

        if (tokenPtr->type == TCL_TOKEN_VARIABLE) {
            varColMap[varIdx] = -1;

            /* Skip past leading '$' */
            tokenString = tokenPtr->start +1;
            tokenSize   = tokenPtr->size  -1;

            Ns_Log(Warning, "---> MapVariablesToColumns: tokenString: %s, tokenSize: %d",
                   tokenString, tokenSize);

            /*
             * Search for a column with a matching name.
             */
            for (colIdx = 0; colIdx < numCols; colIdx++) {
                Dbi_ColumnName(handle, colIdx, &colName);
                if (strncmp(colName, tokenString, tokenSize) == 0) {
                    Ns_Log(Warning, "---> MapVariablesToColumns: mapped colIdx: %d", colIdx);
                    varColMap[varIdx] = colIdx;
                    break;
                }
            }
            varIdx++;

            /*
             * Skip past the components of a variables name (array indices etc.)
             */

            tokIdx += tokenPtr->numComponents;
        }
    }
}

static void
FreeSubst(Tcl_Obj *objPtr)
{
    Tcl_Parse *parsePtr;

    parsePtr = objPtr->internalRep.twoPtrValue.ptr1;
    Tcl_FreeParse(parsePtr);
    ns_free(parsePtr);
}

static void
PushTextToken(Tcl_Parse *parsePtr, char *string, int length)
{
    Tcl_Token *tokenPtr;
    int        newCount;

    if (parsePtr->numTokens == parsePtr->tokensAvailable) {
        /*
         * Expand the token array.
         */
        newCount = parsePtr->tokensAvailable * 2;
        tokenPtr = (Tcl_Token *) ckalloc((unsigned) (newCount * sizeof(Tcl_Token)));
        memcpy((void *) tokenPtr, (void *) parsePtr->tokenPtr,
               (size_t) (parsePtr->tokensAvailable * sizeof(Tcl_Token)));
        if (parsePtr->tokenPtr != parsePtr->staticTokens) {
            ckfree((char *) parsePtr->tokenPtr);
        }
        parsePtr->tokenPtr = tokenPtr;
        parsePtr->tokensAvailable = newCount;
    }

    tokenPtr = &parsePtr->tokenPtr[parsePtr->numTokens];
    tokenPtr->type = TCL_TOKEN_TEXT;
    tokenPtr->numComponents = 0;
    tokenPtr->start = string;
    tokenPtr->size = length;
    parsePtr->numTokens++;
}


/*
 *----------------------------------------------------------------------
 *
 * EvalObjCmd --
 *
 *      Implements dbi_eval.
 *
 *      Evaluate the dbi commands in the given block of Tcl with a
 *      single database handle. Use a new transaction if specified.
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
    InterpData      *idataPtr = arg;
    Dbi_Pool        *pool;
    Dbi_Handle      *handle;
    Ns_Time         *timeoutPtr = NULL;
    Tcl_Obj         *scriptObj, *poolObj = NULL;
    int              isolation = -1;
    int              status = TCL_ERROR;

    Ns_ObjvSpec opts[] = {
        {"-pool",        Ns_ObjvObj,   &poolObj,    NULL},
        {"-timeout",     Ns_ObjvTime,  &timeoutPtr, NULL},
        {"-transaction", Ns_ObjvIndex, &isolation,  levels},
        {"--",           Ns_ObjvBreak, NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"script",    Ns_ObjvObj,   &scriptObj, NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    if (idataPtr->depth++ == MAX_NESTING_DEPTH) {
        Ns_TclPrintfResult(interp, "exceeded maximum nesting depth: %d",
                           idataPtr->depth--);
        return TCL_ERROR;
    }

    /*
     * Grab a free handle, possibly from cache..
     */

    if ((pool = GetPool(idataPtr, poolObj)) == NULL
        || (handle = GetHandle(idataPtr, pool, timeoutPtr)) == NULL) {
        return TCL_ERROR;
    }

    /*
     * Begin a new transaction.
     */

    if (isolation != -1
            && Dbi_Begin(handle, isolation) != NS_OK) {
        SqlError(interp, handle);
        goto done;
    }

    /*
     * Cache the handle and run the script.
     */

    idataPtr->handles[idataPtr->depth] = handle;
    status = Tcl_EvalObjEx(interp, scriptObj, 0);
    idataPtr->handles[idataPtr->depth] = NULL;

    /*
     * Commit or rollback an active transaction.
     */

    if (isolation != -1) {
        if (status != TCL_OK) {
            status = TCL_ERROR;
            Tcl_AddErrorInfo(interp, "\n    dbi transaction status:\nrollback");
            if (Dbi_Rollback(handle) != NS_OK) {
                SqlError(interp, handle);
            }
        } else if (Dbi_Commit(handle) != NS_OK) {
            status = TCL_ERROR;
        }
    }

 done:
    idataPtr->depth--;
    CleanupHandle(idataPtr, handle);

    return status;
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
    Dbi_Value         values[DBI_MAX_BIND];
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

    if (BindVars(interp, handle, values, array, setid) != TCL_OK) {
        goto error;
    }
    if (Dbi_Exec(handle, values) != NS_OK) {
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
BindVars(Tcl_Interp *interp, Dbi_Handle *handle, Dbi_Value *values,
         CONST char *array, CONST char *setid)
{
    Ns_Set         *set = NULL;
    Tcl_Obj        *valueObj;
    CONST char     *key, *data;
    unsigned int    numVars;
    int             i, length, binary;

    if (!(numVars = Dbi_NumVariables(handle))) {
        return TCL_OK;
    }

    if (setid != NULL) {
        if (Ns_TclGetSet2(interp, (char *) setid, &set) != TCL_OK) {
            return TCL_ERROR;
        }
    }

    for (i = 0; i < numVars; i++) {

        if (Dbi_VariableName(handle, i, &key) != NS_OK) {
            SqlError(interp, handle);
            return TCL_ERROR;
        }

        data = NULL;
        binary = 0;

        if (set != NULL) {
            if ((data = Ns_SetGet(set, key)) != NULL) {
                length = strlen(data);
            }
        } else {
            valueObj = Tcl_GetVar2Ex(interp, array ? array : key,
                                     array ? key : NULL, TCL_LEAVE_ERR_MSG);
            if (valueObj != NULL) {
                if (valueObj->typePtr == bytearrayTypePtr) {
                    data = (char *) Tcl_GetByteArrayFromObj(valueObj, &length);
                    binary = 1;
                } else {
                    data = Tcl_GetStringFromObj(valueObj, &length);
                }
            }
        }
        if (data == NULL) {
            Tcl_AddObjErrorInfo(interp, "\ndbi: bind variable not found: ", -1);
            Tcl_AddObjErrorInfo(interp, key, -1);
            return TCL_ERROR;
        }

        values[i].data = length ? data : NULL; /* Coerce the empty string to null. */
        values[i].length = length;
        values[i].binary = binary;
        values[i].colIdx = i;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * GetPool --
 *
 *      Return a pool using one of 3 methods:
 *
 *        - Look up the pool using the given pool name
 *        - Use the pool of the most recently cached handle
 *        - Use the server default pool
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
        if (idataPtr->depth != -1
                && idataPtr->handles[idataPtr->depth] != NULL) {
            pool = idataPtr->handles[idataPtr->depth]->pool;
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
    int         i;

    /*
     * First check the handle cache for a handle from the right pool.
     */

    for (i = idataPtr->depth; i > -1; i--) {
        handle = idataPtr->handles[i];
        if (handle != NULL && handle->pool == pool) {
            return handle;
        }
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
    int i;

    if (handle != NULL) {

        /*
         * Search for the handle in the cache and flush.
         */

        for (i = idataPtr->depth; i > -1; i--) {
            if (idataPtr->handles[i] == handle) {
                Dbi_Flush(handle);
                return;
            }
        }

        /*
         * Return the handle to the system.
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
NextValue(Tcl_Interp *interp, Dbi_Handle *handle, Tcl_Obj **valueObjPtr,
          unsigned int *colIdxPtr)
{
    Dbi_Value value;
    int       end;

    if (Dbi_NextValue(handle, &value, &end) != NS_OK) {
        SqlError(interp, handle);
        return TCL_ERROR;
    }
    if (end) {
        *valueObjPtr = NULL;
    } else if (value.binary) {
        *valueObjPtr = Tcl_NewByteArrayObj((unsigned char *) value.data,
                                           value.length);
    } else {
        *valueObjPtr = Tcl_NewStringObj(value.data, value.length);
    }
    if (colIdxPtr != NULL) {
        *colIdxPtr = value.colIdx;
    }
    
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * AppendFormatToObj --
 *
 *	This function appends a list of Tcl_Obj's to a Tcl_Obj according to
 *	the formatting instructions embedded in the format string. The
 *	formatting instructions are inspired by sprintf(). Returns TCL_OK when
 *	successful. If there's an error in the arguments, TCL_ERROR is
 *	returned, and an error message is written to the interp, if non-NULL.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int
AppendFormatToObj(
    Tcl_Interp *interp,
    Tcl_Obj *appendObj,
    CONST char *format,
    int objc,
    Tcl_Obj *CONST objv[])
{
    CONST char *span = format;
    int numBytes = 0;
    int objIndex = 0;
    int gotXpg = 0, gotSequential = 0;
    int originalLength;
    CONST char *msg;
    CONST char *mixedXPG =
        "cannot mix \"%\" and \"%n$\" conversion specifiers";
    CONST char *badIndex[2] = {
        "not enough arguments for all format specifiers",
        "\"%n$\" argument index out of range"
    };

    if (Tcl_IsShared(appendObj)) {
        Tcl_Panic("%s called with shared object", "Tcl_AppendFormatToObj");
    }
    Tcl_GetStringFromObj(appendObj, &originalLength);

    /*
     * Format string is NUL-terminated.
     */

    while (*format != '\0') {
        char *end;
        int gotMinus, gotHash, gotZero, gotSpace, gotPlus, sawFlag;
        int width, gotPrecision, precision, useShort, useWide;
        int newXpg, numChars, allocSegment = 0;
        Tcl_Obj *segment;
        Tcl_UniChar ch;
        int step = Tcl_UtfToUniChar(format, &ch);

        format += step;
        if (ch != '%') {
            numBytes += step;
            continue;
        }
        if (numBytes) {
            Tcl_AppendToObj(appendObj, span, numBytes);
            numBytes = 0;
        }

        /*
         * Saw a % : process the format specifier.
         *
         * Step 0. Handle special case of escaped format marker (i.e., %%).
         */

        step = Tcl_UtfToUniChar(format, &ch);
        if (ch == '%') {
            span = format;
            numBytes = step;
            format += step;
            continue;
        }

        /*
         * Step 1. XPG3 position specifier
         */

        newXpg = 0;
        if (isdigit(UCHAR(ch))) {
            int position = strtoul(format, &end, 10);
            if (*end == '$') {
                newXpg = 1;
                objIndex = position - 1;
                format = end + 1;
                step = Tcl_UtfToUniChar(format, &ch);
            }
        }
        if (newXpg) {
            if (gotSequential) {
                msg = mixedXPG;
                goto errorMsg;
            }
            gotXpg = 1;
        } else {
            if (gotXpg) {
                msg = mixedXPG;
                goto errorMsg;
            }
            gotSequential = 1;
        }
        if ((objIndex < 0) || (objIndex >= objc)) {
            msg = badIndex[gotXpg];
            goto errorMsg;
        }

        /*
         * Step 2. Set of flags.
         */

        gotMinus = gotHash = gotZero = gotSpace = gotPlus = 0;
        sawFlag = 1;
        do {
            switch (ch) {
            case '-':
                gotMinus = 1;
                break;
            case '#':
                gotHash = 1;
                break;
            case '0':
                gotZero = 1;
                break;
            case ' ':
                gotSpace = 1;
                break;
            case '+':
                gotPlus = 1;
                break;
            default:
                sawFlag = 0;
            }
            if (sawFlag) {
                format += step;
                step = Tcl_UtfToUniChar(format, &ch);
            }
        } while (sawFlag);

        /*
         * Step 3. Minimum field width.
         */

        width = 0;
        if (isdigit(UCHAR(ch))) {
            width = strtoul(format, &end, 10);
            format = end;
            step = Tcl_UtfToUniChar(format, &ch);
        } else if (ch == '*') {
            if (objIndex >= objc - 1) {
                msg = badIndex[gotXpg];
                goto errorMsg;
            }
            if (Tcl_GetIntFromObj(interp, objv[objIndex], &width) != TCL_OK) {
                goto error;
            }
            if (width < 0) {
                width = -width;
                gotMinus = 1;
            }
            objIndex++;
            format += step;
            step = Tcl_UtfToUniChar(format, &ch);
        }

        /*
         * Step 4. Precision.
         */

        gotPrecision = precision = 0;
        if (ch == '.') {
            gotPrecision = 1;
            format += step;
            step = Tcl_UtfToUniChar(format, &ch);
        }
        if (isdigit(UCHAR(ch))) {
            precision = strtoul(format, &end, 10);
            format = end;
            step = Tcl_UtfToUniChar(format, &ch);
        } else if (ch == '*') {
            if (objIndex >= objc - 1) {
                msg = badIndex[gotXpg];
                goto errorMsg;
            }
            if (Tcl_GetIntFromObj(interp, objv[objIndex], &precision)
                != TCL_OK) {
                goto error;
            }

            /*
             * TODO: Check this truncation logic.
             */

            if (precision < 0) {
                precision = 0;
            }
            objIndex++;
            format += step;
            step = Tcl_UtfToUniChar(format, &ch);
        }

        /*
         * Step 5. Length modifier.
         */

        useShort = useWide = 0;
        if (ch == 'h') {
            useShort = 1;
            format += step;
            step = Tcl_UtfToUniChar(format, &ch);
        } else if (ch == 'l') {
            format += step;
            step = Tcl_UtfToUniChar(format, &ch);
#ifndef TCL_WIDE_INT_IS_LONG
            useWide = 1;
#endif
        }

        format += step;
        span = format;

        /*
         * Step 6. The actual conversion character.
         */

        segment = objv[objIndex];
        if (ch == 'i') {
            ch = 'd';
        }
        switch (ch) {
        case '\0':
            msg = "format string ended in middle of field specifier";
            goto errorMsg;
        case 's': {
            numChars = Tcl_GetCharLength(segment);
            if (gotPrecision && (precision < numChars)) {
                segment = Tcl_GetRange(segment, 0, precision - 1);
                Tcl_IncrRefCount(segment);
                allocSegment = 1;
            }
            break;
        }
        case 'c': {
            char buf[TCL_UTF_MAX];
            int code, length;
            if (Tcl_GetIntFromObj(interp, segment, &code) != TCL_OK) {
                goto error;
            }
            length = Tcl_UniCharToUtf(code, buf);
            segment = Tcl_NewStringObj(buf, length);
            Tcl_IncrRefCount(segment);
            allocSegment = 1;
            break;
        }

        case 'u':
        case 'd':
        case 'o':
        case 'x':
        case 'X': {
            short int s = 0;    /* Silence compiler warning; only defined and
                                 * used when useShort is true. */
            long l;
            Tcl_WideInt w;
            int isNegative = 0;

            if (useWide) {
                if (Tcl_GetWideIntFromObj(NULL, segment, &w) != TCL_OK) {
                    goto error;
                }
                isNegative = (w < (Tcl_WideInt)0);
            } else if (Tcl_GetLongFromObj(NULL, segment, &l) != TCL_OK) {
                if (Tcl_GetWideIntFromObj(NULL, segment, &w) != TCL_OK) {
                    goto error;
                } else {
                    l = Tcl_WideAsLong(w);
                }
                if (useShort) {
                    s = (short int) l;
                    isNegative = (s < (short int)0);
                } else {
                    isNegative = (l < (long)0);
                }
            } else if (useShort) {
                s = (short int) l;
                isNegative = (s < (short int)0);
            } else {
                isNegative = (l < (long)0);
            }

            segment = Tcl_NewObj();
            allocSegment = 1;
            Tcl_IncrRefCount(segment);

            if ((isNegative || gotPlus) && (ch == 'd')) {
                Tcl_AppendToObj(segment, (isNegative ? "-" : "+"), 1);
            }

            if (gotHash) {
                switch (ch) {
                case 'o':
                    Tcl_AppendToObj(segment, "0", 1);
                    precision--;
                    break;
                case 'x':
                case 'X':
                    Tcl_AppendToObj(segment, "0x", 2);
                    break;
                }
            }

            switch (ch) {
            case 'd': {
                int length;
                Tcl_Obj *pure;
                CONST char *bytes;

                if (useShort) {
                    pure = Tcl_NewIntObj((int)(s));
                } else if (useWide) {
                    pure = Tcl_NewWideIntObj(w);
                } else {
                    pure = Tcl_NewLongObj(l);
                }
                Tcl_IncrRefCount(pure);
                bytes = Tcl_GetStringFromObj(pure, &length);

                /*
                 * Already did the sign above.
                 */

                if (*bytes == '-') {
                    length--;
                    bytes++;
                }

                /*
                 * Canonical decimal string reps for integers are composed
                 * entirely of one-byte encoded characters, so "length" is the
                 * number of chars.
                 */

                if (gotPrecision) {
                    while (length < precision) {
                        Tcl_AppendToObj(segment, "0", 1);
                        length++;
                    }
                    gotZero = 0;
                }
                if (gotZero) {
                    length += Tcl_GetCharLength(segment);
                    while (length < width) {
                        Tcl_AppendToObj(segment, "0", 1);
                        length++;
                    }
                }
                Tcl_AppendToObj(segment, bytes, -1);
                Tcl_DecrRefCount(pure);
                break;
            }

            case 'u':
            case 'o':
            case 'x':
            case 'X': {
                Tcl_WideUInt bits = (Tcl_WideUInt)0;
                int length, numBits = 4, numDigits = 0, base = 16;
                int index = 0, shift = 0;
                Tcl_Obj *pure;
                char *bytes;

                if (ch == 'u') {
                    base = 10;
                }
                if (ch == 'o') {
                    base = 8;
                    numBits = 3;
                }
                if (useShort) {
                    unsigned short int us = (unsigned short int) s;

                    bits = (Tcl_WideUInt) us;
                    while (us) {
                        numDigits++;
                        us /= base;
                    }
                } else if (useWide) {
                    Tcl_WideUInt uw = (Tcl_WideUInt) w;

                    bits = uw;
                    while (uw) {
                        numDigits++;
                        uw /= base;
                    }
                } else {
                    unsigned long int ul = (unsigned long int) l;

                    bits = (Tcl_WideUInt) ul;
                    while (ul) {
                        numDigits++;
                        ul /= base;
                    }
                }

                /*
                 * Need to be sure zero becomes "0", not "".
                 */

                if ((numDigits == 0) && !((ch == 'o') && gotHash)) {
                    numDigits = 1;
                }
                pure = Tcl_NewObj();
                Tcl_SetObjLength(pure, numDigits);
                bytes = Tcl_GetString(pure);
                length = numDigits;
                while (numDigits--) {
                    int digitOffset;

                    digitOffset = (int) (bits % base);
                    if (digitOffset > 9) {
                        bytes[numDigits] = 'a' + digitOffset - 10;
                    } else {
                        bytes[numDigits] = '0' + digitOffset;
                    }
                    bits /= base;
                }
                if (gotPrecision) {
                    while (length < precision) {
                        Tcl_AppendToObj(segment, "0", 1);
                        length++;
                    }
                    gotZero = 0;
                }
                if (gotZero) {
                    length += Tcl_GetCharLength(segment);
                    while (length < width) {
                        Tcl_AppendToObj(segment, "0", 1);
                        length++;
                    }
                }
                Tcl_AppendObjToObj(segment, pure);
                Tcl_DecrRefCount(pure);
                break;
            }

            }
            break;
        }

        case 'e':
        case 'E':
        case 'f':
        case 'g':
        case 'G': {
#define MAX_FLOAT_SIZE 320
            char spec[2*TCL_INTEGER_SPACE + 9], *p = spec;
            double d;
            int length = MAX_FLOAT_SIZE;
            char *bytes;

            if (Tcl_GetDoubleFromObj(interp, segment, &d) != TCL_OK) {
                /* TODO: Figure out ACCEPT_NAN here */
                goto error;
            }
            *p++ = '%';
            if (gotMinus) {
                *p++ = '-';
            }
            if (gotHash) {
                *p++ = '#';
            }
            if (gotZero) {
                *p++ = '0';
            }
            if (gotSpace) {
                *p++ = ' ';
            }
            if (gotPlus) {
                *p++ = '+';
            }
            if (width) {
                p += sprintf(p, "%d", width);
            }
            if (gotPrecision) {
                *p++ = '.';
                p += sprintf(p, "%d", precision);
                length += precision;
            }

            /*
             * Don't pass length modifiers!
             */

            *p++ = (char) ch;
            *p = '\0';

            segment = Tcl_NewObj();
            allocSegment = 1;
            Tcl_SetObjLength(segment, length);
            bytes = Tcl_GetString(segment);
            Tcl_SetObjLength(segment, sprintf(bytes, spec, d));
            break;
        }
        default:
            if (interp != NULL) {
                char buf[40];

                sprintf(buf, "bad field specifier \"%c\"", ch);
                Tcl_SetObjResult(interp, Tcl_NewStringObj(buf, -1));
            }
            goto error;
        }

        switch (ch) {
        case 'E':
        case 'G':
        case 'X': {
            Tcl_SetObjLength(segment, Tcl_UtfToUpper(Tcl_GetString(segment)));
        }
        }

        numChars = Tcl_GetCharLength(segment);
        if (!gotMinus) {
            while (numChars < width) {
                Tcl_AppendToObj(appendObj, (gotZero ? "0" : " "), 1);
                numChars++;
            }
        }
        Tcl_AppendObjToObj(appendObj, segment);
        if (allocSegment) {
            Tcl_DecrRefCount(segment);
        }
        while (numChars < width) {
            Tcl_AppendToObj(appendObj, (gotZero ? "0" : " "), 1);
            numChars++;
        }

        objIndex += gotSequential;
    }
    if (numBytes) {
        Tcl_AppendToObj(appendObj, span, numBytes);
        numBytes = 0;
    }

    return TCL_OK;

 errorMsg:
    if (interp != NULL) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj(msg, -1));
    }

 error:
    Tcl_SetObjLength(appendObj, originalLength);

    return TCL_ERROR;
}
