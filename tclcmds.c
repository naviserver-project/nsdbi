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
 * Copyright (C) 2004-2008 Stephen Deasey <sdeasey@gmail.com>
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


extern int
DbiTclSubstTemplate(Tcl_Interp *, Dbi_Handle *,
                    Tcl_Obj *templateObj, Tcl_Obj *defaultObj);


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
    DmlObjCmd,
    ZeroOrOneRowObjCmd,
    OneRowObjCmd,
    EvalObjCmd,
    CtlObjCmd;

static InterpData *GetInterpData(Tcl_Interp *interp);
static Tcl_InterpDeleteProc FreeInterpData;

static int RowCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[],
                  int *rowPtr);

static int Exec(InterpData *idataPtr, Tcl_Obj *poolObj, Ns_Time *timeoutPtr,
                Tcl_Obj *queryObj, Tcl_Obj *valuesObj, int maxRows, int dml,
                Dbi_Handle **handlePtrPtr);

static Dbi_Pool *GetPool(InterpData *, Tcl_Obj *poolObj);
static Dbi_Handle *GetHandle(InterpData *, Dbi_Pool *, Ns_Time *);
static void PutHandle(InterpData *idataPtr, Dbi_Handle *handle);

static int NextRow(Tcl_Interp *interp, Dbi_Handle *handle, int *endPtr);
static int ColumnValue(Tcl_Interp *interp, Dbi_Handle *handle, unsigned int index,
                       Tcl_Obj **valueObjPtr);


/*
 * Static variables defined in this file.
 */

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
    InterpData  *idataPtr;
    int          i;
    static int   once = 0;

    if (!once) {
        once = 1;

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
        {"dbi_0or1row",     ZeroOrOneRowObjCmd},
        {"dbi_1row",        OneRowObjCmd},
        {"dbi_dml",         DmlObjCmd},
        {"dbi_eval",        EvalObjCmd},
        {"dbi_ctl",         CtlObjCmd}
    };

    idataPtr = GetInterpData(interp);

    for (i = 0; i < (sizeof(cmds) / sizeof(cmds[0])); i++) {
        Tcl_CreateObjCommand(interp, cmds[i].name, cmds[i].proc, idataPtr, NULL);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * GetInterpData, FreeInterpData --
 *
 *      Get/free the interp data.
 *
 * Results:
 *      Pointer to InterpData.
 *
 * Side effects:
 *      Allocates and initialises on first use by DbiInitInterps.
 *
 *----------------------------------------------------------------------
 */

static InterpData *
GetInterpData(Tcl_Interp *interp)
{
    InterpData        *idataPtr;
    static const char *key = "dbi:data";

    idataPtr = Tcl_GetAssocData(interp, key, NULL);
    if (idataPtr == NULL) {
        idataPtr = ns_calloc(1, sizeof(InterpData));
        idataPtr->interp = interp;
        idataPtr->server = Ns_TclInterpServer(interp);
        idataPtr->depth = -1;
        Tcl_SetAssocData(interp, key, FreeInterpData, idataPtr);
    }
    return idataPtr;
}

static void
FreeInterpData(ClientData arg, Tcl_Interp *interp)
{
    ns_free(arg);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_TclGetPool, GetPool --
 *
 *      Return a pool using one of 3 methods:
 *
 *      If no pool is specified:
 *        1) Return the pool of the current handle from dbi_eval
 *        2) Use the server default pool. If this fails, return NULL
 *
 *      Otheriwse:
 *        3) Look up the pool using the given pool name. If no such
 *           pool, return NULL.
 *
 * Results:
 *      Pointer to Dbi_Pool or NULL on error.
 *
 * Side effects:
 *      The Tcl object may be converted to dbi:pool type. An error
 *      may be left in the interp if conversion fails.
 *
 *----------------------------------------------------------------------
 */

Dbi_Pool *
Dbi_TclGetPool(Tcl_Interp *interp, Tcl_Obj *poolObj)
{
    return GetPool(GetInterpData(interp), poolObj);
}

Dbi_Pool *
GetPool(InterpData *idataPtr, Tcl_Obj *poolObj)
{
    Tcl_Interp        *interp = idataPtr->interp;
    Dbi_Pool          *pool;
    static const char *poolType = "dbi:pool";

    if (poolObj != NULL) {
        if (Ns_TclGetOpaqueFromObj(poolObj, poolType, (void **) &pool)
                != TCL_OK) {
            pool = Dbi_GetPool(idataPtr->server, Tcl_GetString(poolObj));
            if (pool != NULL) {
                Ns_TclSetOpaqueObj(poolObj, poolType, pool);
            } else {
                Tcl_SetResult(interp,
                    "invalid pool name or pool not available to virtual server",
                    TCL_STATIC);
            }
        }
    } else if (idataPtr->depth != -1
               && idataPtr->handles[idataPtr->depth] != NULL) {
        pool = idataPtr->handles[idataPtr->depth]->pool;
    } else {
        pool = Dbi_DefaultPool(idataPtr->server);
        if (pool == NULL) {
            Tcl_SetResult(interp,
                "no pool specified and no default configured",
                 TCL_STATIC);
        }
    }

    return pool;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_TclGetHandle, GetHandle --
 *
 *      Get a handle from the given pool within the timeout. Use the
 *      the current handle from dbi_eval if the pools match.
 *
 * Results:
 *      Pointer to handle or NULL on error.
 *
 * Side effects:
 *      Error message left in interp.
 *
 *----------------------------------------------------------------------
 */

Dbi_Handle *
Dbi_TclGetHandle(Tcl_Interp *interp, Dbi_Pool *pool, Ns_Time *timeoutPtr)
{
    return GetHandle(GetInterpData(interp), pool, timeoutPtr);
}

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
 * Dbi_TclPutHandle, PutHandle --
 *
 *      Cleanup a handle. If it's nested within a call to dbi_eval
 *      simply flush it. Otherwise, return it to it's pool.
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
Dbi_TclPutHandle(Tcl_Interp *interp, Dbi_Handle *handle)
{
    PutHandle(GetInterpData(interp), handle);
}

static void
PutHandle(InterpData *idataPtr, Dbi_Handle *handle)
{
    int i;

    for (i = idataPtr->depth; i > -1; i--) {
        if (idataPtr->handles[i] == handle) {
            Dbi_Flush(handle);
            return;
        }
    }
    Dbi_PutHandle(handle);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_TclBindVariables --
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

int
Dbi_TclBindVariables(Tcl_Interp *interp, Dbi_Handle *handle,
                     Dbi_Value *dbValues, Tcl_Obj *tclValues)
{
    Ns_Set         *set;
    Tcl_Obj        *valueObj;
    CONST char     *key, *data;
    char           *name;
    unsigned int    numVars, i;
    int             length, binary;

    numVars = Dbi_NumVariables(handle);
    if (numVars == 0) {
        return TCL_OK;
    }

    /*
     * FIXME: This is ugly. Need to eficiently distinguish types.
     *        Also, handle Tcl 8.5 dicts.
     */

    name = NULL;
    set = NULL;

    if (tclValues != NULL) {
        name = Tcl_GetString(tclValues);

        if (name[0] != '\0'
            && (name[0] == 'd' || name[0] == 't')
            && name[1] != '\0'
            && isdigit(UCHAR(name[1]))
            && Ns_TclGetSet2(interp, name, &set) != TCL_OK) {

            return TCL_ERROR;
        }
    }

    for (i = 0; i < numVars; i++) {

        if (Dbi_VariableName(handle, i, &key) != NS_OK) {
            Dbi_TclErrorResult(interp, handle);
            return TCL_ERROR;
        }

        data = NULL;
        binary = 0;

        if (set != NULL) {
            if ((data = Ns_SetGet(set, key)) != NULL) {
                length = strlen(data);
            }
        } else {

            /*
             * NB: handle both array and variable lookup here.
             */

            valueObj = Tcl_GetVar2Ex(interp, name ? name : key,
                                     name ? key : NULL, TCL_LEAVE_ERR_MSG);
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

        dbValues[i].data = length ? data : NULL; /* Coerce the empty string to null. */
        dbValues[i].length = length;
        dbValues[i].binary = binary;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_TclErrorResult --
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

void
Dbi_TclErrorResult(Tcl_Interp *interp, Dbi_Handle *handle)
{
    Tcl_SetErrorCode(interp, Dbi_ExceptionCode(handle), NULL);
    Tcl_SetResult(interp, Dbi_ExceptionMsg(handle), TCL_VOLATILE);
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
    unsigned int  colIdx, numCols;
    Tcl_Obj      *resObj, *valueObj, *queryObj;
    Tcl_Obj      *poolObj = NULL, *valuesObj = NULL;
    Tcl_Obj      *templateObj = NULL, *defaultObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    int           end, status, maxRows = -1;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bind",      Ns_ObjvObj,    &valuesObj,  NULL},
        {"-max",       Ns_ObjvInt,    &maxRows,    NULL},
        {"--",         Ns_ObjvBreak,  NULL,        NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"query",      Ns_ObjvObj, &queryObj,    NULL},
        {"?template",  Ns_ObjvObj, &templateObj, NULL},
        {"?default",   Ns_ObjvObj, &defaultObj,  NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    /*
     * Get a handle, prepare, bind, and run the qeuery.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, queryObj, valuesObj, maxRows, 0,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Successful result is a flat list of all values (or an empty list)
     */

    if (templateObj != NULL) {
        status = DbiTclSubstTemplate(interp, handle,
                                     templateObj, defaultObj);
    } else {

        resObj = Tcl_GetObjResult(interp);
        numCols = Dbi_NumColumns(handle);

        while ((status = NextRow(interp, handle, &end)) == TCL_OK && !end) {

            for (colIdx = 0; colIdx < numCols; colIdx ++) {
                if ((status = ColumnValue(interp, handle, colIdx, &valueObj))
                        != TCL_OK) {
                    goto done;
                }
                if ((status = Tcl_ListObjAppendElement(interp, resObj, valueObj))
                        != TCL_OK) {
                    Tcl_DecrRefCount(valueObj);
                    goto done;
                }
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
    Tcl_Obj      *queryObj, *poolObj = NULL, *valuesObj = NULL;
    Ns_Time      *timeoutPtr = NULL;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bind",      Ns_ObjvObj,    &valuesObj,  NULL},
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

    if (Exec(idataPtr, poolObj, timeoutPtr, queryObj, valuesObj, -1, 1,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /* FIXME: a clean way of returning the number of rows affected. */

    PutHandle(idataPtr, handle);

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
    unsigned int  colIdx, numCols;
    Tcl_Obj      *valueObj, *queryObj;
    Tcl_Obj      *poolObj = NULL, *valuesObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    CONST char   *column;
    int           found, end, status;

    Ns_ObjvSpec opts[] = {
        {"-pool",      Ns_ObjvObj,    &poolObj,    NULL},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bind",      Ns_ObjvObj,    &valuesObj,  NULL},
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

    if (Exec(idataPtr, poolObj, timeoutPtr, queryObj, valuesObj, 1, 0,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Make sure the result has no more than a single row in the result
     * and set the values of that row as variables in the callers frame..
     */

    status = TCL_ERROR;
    found = 0;

    if (NextRow(interp, handle, &end) != TCL_OK) {
        goto cleanup;
    }
    if (end) {
        goto done;
    }

    found = 1;
    numCols = Dbi_NumColumns(handle);

    for (colIdx = 0; colIdx < numCols; colIdx++) {
        if (ColumnValue(interp, handle, colIdx, &valueObj) != TCL_OK) {
            goto cleanup;
        }
        if (Dbi_ColumnName(handle, colIdx, &column) != NS_OK) {
            Dbi_TclErrorResult(interp, handle);
            goto cleanup;
        }
        if (Tcl_SetVar2Ex(interp, column, NULL, valueObj,
                          TCL_LEAVE_ERR_MSG) == NULL) {
            Tcl_DecrRefCount(valueObj);
            goto cleanup;
        }
    }

    /*
     * Try to fetch again to check for more than 1 row.
     */

    if (NextRow(interp, handle, &end) != TCL_OK) {
        goto cleanup;
    }

 done:
    status = TCL_OK;

 cleanup:

    *foundRowPtr = found;
    PutHandle(idataPtr, handle);

    return status;
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
        Dbi_TclErrorResult(interp, handle);
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
                Dbi_TclErrorResult(interp, handle);
            }
        } else if (Dbi_Commit(handle) != NS_OK) {
            status = TCL_ERROR;
        }
    }

 done:
    idataPtr->depth--;
    PutHandle(idataPtr, handle);

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
    CONST char *server = idataPtr->server;
    Dbi_Pool   *pool;
    Ns_DString  ds;
    int         cmd, oldValue, newValue;

    static CONST char *cmds[] = {
        "bounce", "database", "default", "driver",
        "maxhandles", "maxrows", "maxidle", "maxopen", "maxqueries",
        "pools", "stats", "timeout", NULL
    };
    enum CmdIdx {
        CBounceCmd, CDatabaseCmd, CDefaultCmd, CDriverCmd,
        CMaxHandlesCmd, CMaxRowsCmd, CMaxIdleCmd, CMaxOpenCmd, CMaxQueriesCmd,
        CPoolsCmd, CStatsCmd, CTimeoutCmd,
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
        if (Dbi_ListPools(&ds, server) != NS_OK) {
            return TCL_ERROR;
        }
        Tcl_DStringResult(interp, &ds);
        return TCL_OK;

    case CDefaultCmd:
        pool = Dbi_DefaultPool(server);
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

    case CMaxRowsCmd:
        oldValue = Dbi_Config(pool, DBI_CONFIG_MAXROWS, newValue);
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

    case CTimeoutCmd:
        oldValue = Dbi_Config(pool, DBI_CONFIG_TIMEOUT, newValue);
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
     Tcl_Obj *queryObj, Tcl_Obj *valuesObj, int maxRows, int dml,
     Dbi_Handle **handlePtrPtr)
{
    Tcl_Interp       *interp = idataPtr->interp;
    Dbi_Pool         *pool;
    Dbi_Handle       *handle;
    Dbi_Value         dbValues[DBI_MAX_BIND];
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
        Dbi_TclErrorResult(interp, handle);
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

    if (Dbi_TclBindVariables(interp, handle, dbValues, valuesObj) != TCL_OK) {
        goto error;
    }
    if (Dbi_Exec(handle, dbValues, maxRows) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        goto error;
    }

    return TCL_OK;

 error:
    PutHandle(idataPtr, handle);

    return TCL_ERROR;
}



/*
 *----------------------------------------------------------------------
 *
 * NextRow --
 *
 *      Fetch the next row of the result set.
 *
 * Results:
 *      TCL_OK/TCL_ERROR. *endPtr set to 0/1.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
NextRow(Tcl_Interp *interp, Dbi_Handle *handle, int *endPtr)
{
    if (Dbi_NextRow(handle, endPtr) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        return TCL_ERROR;
    }
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ColumnValue --
 *
 *      Get the value at the given column index as a new Tcl object.
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
ColumnValue(Tcl_Interp *interp, Dbi_Handle *handle, unsigned int index,
            Tcl_Obj **valueObjPtr)
{
    Tcl_Obj   *objPtr;
    char      *bytes;
    size_t     length;
    int        binary;

    if (Dbi_ColumnLength(handle, index, &length, &binary) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        return TCL_ERROR;
    }

    objPtr = Tcl_NewObj();

    if (binary) {
        bytes = (char *) Tcl_SetByteArrayLength(objPtr, (int) length);
    } else {
        Tcl_SetObjLength(objPtr, (int) length);
        bytes = objPtr->bytes;
    }
    if (Dbi_ColumnValue(handle, index, bytes, length) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        Tcl_DecrRefCount(objPtr);
        return TCL_ERROR;
    }
    *valueObjPtr = objPtr;

    return TCL_OK;
}
