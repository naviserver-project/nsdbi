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

#define MAX_NESTING_DEPTH 32


extern int
DbiTclSubstTemplate(Tcl_Interp *, Dbi_Handle *,
                    Tcl_Obj *templateObj, Tcl_Obj *defaultObj, int adp, Dbi_quotingLevel quote);


/*
 * The following struct maintains state for the currently
 * executing command.
 *
 * The handle cache may contain the same handle in more than
 * one entry as 'depth' is used as a form of reference counting.
 */

typedef struct InterpData {
    Tcl_Interp *interp;
    const char *server;
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
   /*ForeachObjCmd,*/
    CtlObjCmd,
    ConvertObjCmd;

static InterpData *GetInterpData(Tcl_Interp *interp);
static Tcl_InterpDeleteProc FreeInterpData;

static int RowCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[],
                  int *foundRowPtr);

static int Exec(InterpData *idataPtr, Tcl_Obj *poolObj, Ns_Time *timeoutPtr,
                Tcl_Obj *queryObj, Tcl_Obj *valuesObj, int maxRows, int dml,
                int autoNull, Dbi_Handle **handlePtrPtr);

static Dbi_Pool *GetPool(InterpData *, Tcl_Obj *poolObj);
static Dbi_Handle *GetHandle(InterpData *, Dbi_Pool *, Ns_Time *);
static void PutHandle(InterpData *idataPtr, Dbi_Handle *handle);

static int NextRow(Tcl_Interp *interp, Dbi_Handle *handle, int *endPtr);
static int ColumnValue(Tcl_Interp *interp, Dbi_Handle *handle, unsigned int index,
                       Tcl_Obj **valueObjPtr);


/*
 * Static variables defined in this file.
 */

static const Tcl_ObjType    *bytearrayTypePtr;

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
 * The following are the values that can be passed to the
 * dbi_rows '-quote' option.
 */

static Ns_ObjvTable quotingTypeStrings[] = {
    {"none", Dbi_QuoteNone},
    {"html", Dbi_QuoteHTML},
    {"js",   Dbi_QuoteJS},
    {NULL, 0}
};

/*
 * The following are the values that can be passed to the
 * dbi_rows '-result' option.
 */

static Ns_ObjvTable resultFormatStrings[] = {
    {"flatlist", Dbi_ResultFlatList},
    {"sets",     Dbi_ResultSets},
    {"dicts",    Dbi_ResultDicts},
    {"avlists",  Dbi_ResultAvLists},
    {"dict",     Dbi_ResultDict},
    {"lists",    Dbi_ResultLists},
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
DbiInitInterp(Tcl_Interp *interp, const void *UNUSED(arg))
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
        const char     *name;
        Tcl_ObjCmdProc *proc;
    } cmds[] = {
        {"dbi_rows",        RowsObjCmd},
        {"dbi_0or1row",     ZeroOrOneRowObjCmd},
        {"dbi_1row",        OneRowObjCmd},
        {"dbi_dml",         DmlObjCmd},
        {"dbi_eval",        EvalObjCmd},
        /*{"dbi_foreach",     ForeachObjCmd},*/
        {"dbi_ctl",         CtlObjCmd},
        {"dbi_convert",     ConvertObjCmd}
    };

    idataPtr = GetInterpData(interp);

    for (i = 0; i < (int)(sizeof(cmds) / sizeof(cmds[0])); i++) {
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
 *      Allocates and initializes on first use by DbiInitInterps.
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
FreeInterpData(ClientData arg, Tcl_Interp *UNUSED(interp))
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

    if (poolObj != NULL) {
        static const char *poolType = "dbi:pool";

        if (Ns_TclGetOpaqueFromObj(poolObj, poolType, (void **) &pool)
                != TCL_OK) {
            pool = Dbi_GetPool(idataPtr->server, Tcl_GetString(poolObj));
            if (pool != NULL) {
                Ns_TclSetOpaqueObj(poolObj, poolType, pool);
            } else {
                Tcl_SetObjResult(interp,
                                 Tcl_NewStringObj("invalid db name or db not available to virtual server",
                                                  -1));
            }
        }
    } else if (idataPtr->depth != -1
               && idataPtr->handles[idataPtr->depth] != NULL) {
        pool = idataPtr->handles[idataPtr->depth]->pool;
    } else {
        pool = Dbi_DefaultPool(idataPtr->server);
        if (pool == NULL) {
            Tcl_SetObjResult(interp,
                             Tcl_NewStringObj("no db specified and no default configured", -1));
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
        /*break;*/
    case NS_TIMEOUT:
        Tcl_SetErrorCode(interp, "NS_TIMEOUT", (char *)0L);
        Tcl_SetObjResult(interp, Tcl_NewStringObj("wait for database handle timed out", -1));
        break;
    default:
        Tcl_SetObjResult(interp, Tcl_NewStringObj("handle allocation failed", -1));
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
 * ArrayExists --
 *
 *      Test for the existence of an array with a given name.
 *
 * Results:
 *      Boolean value indicating existence
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
ArrayExists(Tcl_Interp *interp, Tcl_Obj *arrayNameObj) {
    static Tcl_Obj *arrayObj = NULL, *existsObj = NULL;
    Tcl_Obj *oldResultObj, *objv[3];
    int arrayExists = 0;

    if (arrayObj == NULL) {
        arrayObj  = Tcl_NewStringObj("array", 5);
        existsObj = Tcl_NewStringObj("exists", 6);
    }
    objv[0] = arrayObj;
    objv[1] = existsObj;
    objv[2] = arrayNameObj;

    oldResultObj = Tcl_GetObjResult(interp);
    Tcl_IncrRefCount(oldResultObj);

    if (Tcl_EvalObjv(interp, 3, objv, 0) == TCL_OK) {
        Tcl_GetIntFromObj(interp, Tcl_GetObjResult(interp), &arrayExists);
    }

    Tcl_SetObjResult(interp, oldResultObj);
    Tcl_DecrRefCount(oldResultObj);

    return arrayExists;
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
                     Dbi_Value *dbValues, Tcl_Obj *tclValues,
                     int autoNull)
{
    Ns_Set         *set;
    Tcl_Obj        *valueObj, *dictObj = NULL;
    const char     *key;
    char           *name;
    unsigned int    numVars, i;
    int             length = 0;

    numVars = Dbi_NumVariables(handle);
    if (numVars == 0) {
        return TCL_OK;
    }

    set = NULL;
    name = NULL;

    if (tclValues != NULL) {
        int valuesLength = 0;

        /*
         * If the provided value for bind has a valuesLength of 1, then it
         * is assumed that it is either an ns_set or the name of an
         * array.
         */

        Tcl_ListObjLength(interp, tclValues, &valuesLength);

        if (valuesLength == 1) {
            name = Tcl_GetString(tclValues);

            if ((name[0] == 'd' || name[0] == 't')
                && name[1] != '\0'
                && isdigit(UCHAR(name[1]))
                && Ns_TclGetSet2(interp, name, &set) != TCL_OK
                ) {
                return TCL_ERROR;
            }

            /*
             * If set == NULL then it must be an array or an error.
             */

            if (set == NULL && !ArrayExists(interp, tclValues)) {
                Ns_TclPrintfResult(interp, "dbi: array \"%s\" with bind values does not exist",
                                   name);
                return TCL_ERROR;
            }

        } else {
            /*
             * It must be a dict (or a list convertible to a dict);
             * therefore, it has to have an even length.
             */
            if ((valuesLength % 2) != 0) {
                Ns_TclPrintfResult(interp, "dbi: \"%s\" is not a valid dict with bind variables",
                                   Tcl_GetString(tclValues));
                return TCL_ERROR;
            }

            dictObj = tclValues;
        }
    }

    for (i = 0; i < numVars; i++) {
        int         binary = 0;
        const char *data;

        if (Dbi_VariableName(handle, i, &key) != NS_OK) {
            Dbi_TclErrorResult(interp, handle);
            return TCL_ERROR;
        }

        data = NULL;

        if (set != NULL) {
            if ((data = Ns_SetGet(set, key)) != NULL) {
                length = (int)strlen(data);
            }

        } else {
            /*
             * Set the valueObj either from the dict or from local variables
             */
            if (dictObj != NULL) {
                Tcl_Obj *keyObj = Tcl_NewStringObj(key, -1);

                if (Tcl_DictObjGet(interp, dictObj, keyObj, &valueObj) != TCL_OK) {
                    Tcl_DecrRefCount(keyObj);
                    return TCL_ERROR;
                }
                Tcl_DecrRefCount(keyObj);

            } else {
                /*
                 * NB: handle both array and variable lookup here.
                 */

                valueObj = Tcl_GetVar2Ex(interp,
                                         name ? name : key,
                                         name ? key : NULL,
                                         0 /*TCL_LEAVE_ERR_MSG*/);
            }

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
            length = 0;

            if (!autoNull) {
                const char *source;

                if (set != NULL) {
                    source = "in ns set";
                } else if (dictObj != NULL) {
                    source = "in dict";
                } else if (name != NULL) {
                    source = "in array";
                } else {
                    source = "as local variable";
                }
                Ns_TclPrintfResult(interp, "dbi: bind variable \"%s\" not found %s",
                                   key, source);
                return TCL_ERROR;
            }
        }

        dbValues[i].data = length ? data : NULL; /* Coerce the empty string to null. */
        dbValues[i].length = (size_t)length;
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
    Tcl_SetErrorCode(interp, Dbi_ExceptionCode(handle), (char *)0L);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_ExceptionMsg(handle), -1));
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
RowsObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    Tcl_Obj      *resObj, *valueObj, *colListObj = NULL, *queryObj, **colV = NULL, **templateV = NULL;
    Tcl_Obj      *poolObj = NULL, *valuesObj = NULL, *colsNameObj = NULL, *rowObj = NULL;
    Tcl_Obj      *templateObj = NULL, *defaultObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    int           end, status, maxRows = -1, adp = 0, autoNull = 0;
    Dbi_quotingLevel quote = Dbi_QuoteNone;
    Dbi_resultFormat resultFormat = Dbi_ResultFlatList;

    Ns_ObjvSpec opts[] = {
        {"-db",        Ns_ObjvObj,    &poolObj,       NULL},
        {"-autonull",  Ns_ObjvBool,   &autoNull,      (void *) NS_TRUE},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr,    NULL},
        {"-bind",      Ns_ObjvObj,    &valuesObj,     NULL},
        {"-columns",   Ns_ObjvObj,    &colsNameObj,   NULL},
        {"-max",       Ns_ObjvInt,    &maxRows,       NULL},
        {"-result",    Ns_ObjvIndex,  &resultFormat,  resultFormatStrings},
        {"-append",    Ns_ObjvBool,   &adp,           (void *) NS_TRUE},
        {"-quote",     Ns_ObjvIndex,  &quote,         quotingTypeStrings},
        {"--",         Ns_ObjvBreak,  NULL,           NULL},
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

    if (templateObj == NULL && quote != Dbi_QuoteNone) {
        Tcl_SetObjResult(interp,
                         Tcl_NewStringObj("dbi: '-quote' is only allowed when template is given", -1));
        return TCL_ERROR;
    }

    if (templateObj != NULL && resultFormat != Dbi_ResultFlatList) {
        Tcl_SetObjResult(interp,
                         Tcl_NewStringObj("dbi: '-result' option is only allowed when no template is given",
                                          -1));
        return TCL_ERROR;
    }

    /*
     * Get a handle, prepare, bind, and run the query.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, queryObj, valuesObj, maxRows, 0, autoNull,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Successful result is a flat list of all values (or an empty list)
     */
    if (templateObj != NULL) {
        status = DbiTclSubstTemplate(interp, handle,
                                     templateObj, defaultObj, adp, quote);
    } else {
        long rowNum = 0;
        const char   *colName;
        unsigned int  colIdx, numCols;

        numCols = Dbi_NumColumns(handle);

        /*
         * Report the column names of the result set, or compute the
         * nameObjs if needed later,
         */

        if (colsNameObj != NULL
            || (resultFormat != Dbi_ResultFlatList && resultFormat != Dbi_ResultLists )) {
            int nrElements;


            colListObj = Tcl_NewListObj((int)numCols, NULL);

            for (colIdx = 0; colIdx < numCols; colIdx++) {

                if (Dbi_ColumnName(handle, colIdx, &colName) != NS_OK) {
                    Dbi_TclErrorResult(interp, handle);
                    goto error;
                }
                valueObj = Tcl_NewStringObj(colName, -1);
                if (Tcl_ListObjAppendElement(interp, colListObj, valueObj) != TCL_OK) {
                    Tcl_DecrRefCount(valueObj);
                    goto error;
                }

            }

            if (colsNameObj != NULL) {
                if (Tcl_ObjSetVar2(interp, colsNameObj, NULL,
                                   colListObj, TCL_LEAVE_ERR_MSG) == NULL) {
                    goto error;
                }
            }

            if ((Tcl_ListObjGetElements(interp, colListObj, &nrElements, &colV) != TCL_OK)) {
                goto error;
            }
        }

        status = TCL_OK;

        if (resultFormat == Dbi_ResultAvLists) {
            templateV = ns_calloc(numCols * 2, sizeof(Tcl_Obj*));
            for (colIdx = 0; colIdx < numCols; colIdx++) {
                templateV[colIdx * 2] = colV[colIdx];
            }
        } else if (resultFormat == Dbi_ResultDict) {
            Tcl_SetObjResult(interp, Tcl_NewDictObj());
        }
        resObj = Tcl_GetObjResult(interp);

        while ((status = NextRow(interp, handle, &end)) == TCL_OK && !end) {
            Ns_Set *set = NULL;

            /*
             * Begin of row
             */

            switch (resultFormat) {
            case Dbi_ResultFlatList:
            case Dbi_ResultAvLists:
                break;
            case Dbi_ResultLists:
                rowObj = Tcl_NewListObj((int)numCols, NULL);
                break;
            case Dbi_ResultDict:
            case Dbi_ResultDicts:
                rowObj = Tcl_NewDictObj();
                break;
            case Dbi_ResultSets:
                set = Ns_SetCreate("r");
                break;
            }

            /*
             * Columns
             */
            for (colIdx = 0; colIdx < numCols; colIdx++) {
                if (ColumnValue(interp, handle, colIdx, &valueObj) != TCL_OK) {
                    goto error;
                }

                if (resultFormat == Dbi_ResultFlatList) {
                    if (Tcl_ListObjAppendElement(interp, resObj, valueObj) != TCL_OK) {
                        Tcl_DecrRefCount(valueObj);
                        goto error;
                    }
                } else if (resultFormat == Dbi_ResultLists) {
                    if (Tcl_ListObjAppendElement(interp, rowObj, valueObj) != TCL_OK) {
                        Tcl_DecrRefCount(valueObj);
                        goto error;
                    }
                } else {
                    assert(colV[colIdx] != NULL);

                    if (resultFormat == Dbi_ResultAvLists) {

                        templateV[colIdx*2 + 1] = valueObj;

                    } else if (resultFormat == Dbi_ResultDicts || resultFormat == Dbi_ResultDict) {

                        if (Tcl_DictObjPut(interp, rowObj, colV[colIdx], valueObj) != TCL_OK) {
                            Tcl_DecrRefCount(valueObj);
                            goto error;
                        }
                    } else if (resultFormat == Dbi_ResultSets) {
                        int coldIdxLength, valueLength;
                        const char *coldIdxString = Tcl_GetStringFromObj(colV[colIdx], &coldIdxLength);
                        const char *valueString = Tcl_GetStringFromObj(valueObj, &valueLength);
                        Ns_SetPutSz(set, coldIdxString, coldIdxLength, valueString, valueLength);
                    }
                }
            }

            /*
             * End of row
             */

            switch (resultFormat) {
            case Dbi_ResultFlatList: break;
            case Dbi_ResultSets: Ns_TclEnterSet(interp, set, 0); break;
            case Dbi_ResultDict:
                {
                    Tcl_Obj *idxObj = Tcl_NewLongObj(++rowNum);
                    if (Tcl_DictObjPut(interp, resObj, idxObj, rowObj) != TCL_OK) {
                        Tcl_DecrRefCount(idxObj);
                        goto error;
                    }
                }
                break;
            case Dbi_ResultAvLists:
                rowObj = Tcl_NewListObj((int)numCols * 2, templateV);
                if (Tcl_ListObjAppendElement(interp, resObj, rowObj) != TCL_OK) {
                    Tcl_DecrRefCount(rowObj);
                    goto error;
                }
                break;
            case Dbi_ResultLists:
            case Dbi_ResultDicts:
                if (Tcl_ListObjAppendElement(interp, resObj, rowObj) != TCL_OK) {
                    Tcl_DecrRefCount(rowObj);
                    goto error;
                }
                break;
            }

        }
    }


 done:
    PutHandle(idataPtr, handle);

    if (colListObj != NULL && colsNameObj == NULL) {
        /*
         * We have the object just for the names in the result
         */
        Tcl_DecrRefCount(colListObj);
    }
    if (templateV != NULL) {
        ns_free(templateV);
    }

    return status;

 error:
    if (rowObj != NULL) {
        Tcl_DecrRefCount(rowObj);
    }

    status = TCL_ERROR;
    goto done;
}

/*
 *----------------------------------------------------------------------
 *
 * RowsObjCmd --
 *
 *      Implements dbi_convert.
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
ConvertObjCmd(ClientData UNUSED(clientData), Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    int              nrColumns, nrElements;
    Tcl_Obj         *resObj, *colsObj, *listObj, **colV, **elemV, **templateV = NULL;
    int              colNum, status, rowNum;
    Dbi_resultFormat resultFormat = Dbi_ResultLists;

    Ns_ObjvSpec opts[] = {
        {"-result",    Ns_ObjvIndex,  &resultFormat,  resultFormatStrings},
        {"--",         Ns_ObjvBreak,  NULL,           NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"columns",    Ns_ObjvObj, &colsObj,    NULL},
        {"list",       Ns_ObjvObj, &listObj,    NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    if ((Tcl_ListObjGetElements(interp, colsObj, &nrColumns, &colV) != TCL_OK)) {
        return TCL_ERROR;
    }
    if ((Tcl_ListObjGetElements(interp, listObj, &nrElements, &elemV) != TCL_OK)) {
        return TCL_ERROR;
    }
    if (nrColumns == 0 || nrColumns > nrElements || nrElements % nrColumns != 0) {
        Tcl_SetObjResult(interp,
                         Tcl_NewStringObj("dbi: number of elements in the list must be a multiple of the columns", -1));
        return TCL_ERROR;
    }

    resObj = Tcl_GetObjResult(interp);
    status = TCL_OK;

    if (resultFormat == Dbi_ResultAvLists) {
        templateV = ns_calloc((size_t)(nrColumns * 2), sizeof(Tcl_Obj*));
        for (colNum = 0; colNum < nrColumns; colNum++) {
            templateV[colNum * 2] = colV[colNum];
        }
    }

    for (rowNum = 0; rowNum * nrColumns < nrElements; rowNum ++) {
        Ns_Set *set = NULL;

        switch (resultFormat) {

        case Dbi_ResultDicts:
            {
                Tcl_Obj *dictObj = Tcl_NewDictObj();

                for (colNum = 0; colNum < nrColumns; colNum++) {
                    if (Tcl_DictObjPut(interp, dictObj,
                                       colV[colNum],
                                       elemV[rowNum * nrColumns + colNum]) != TCL_OK) {
                        Tcl_DecrRefCount(dictObj);
                        goto error;
                    }
                }
                if (Tcl_ListObjAppendElement(interp, resObj, dictObj) != TCL_OK) {
                    Tcl_DecrRefCount(dictObj);
                    goto error;
                }
            }
            break;

        case Dbi_ResultAvLists:
            {
                Tcl_Obj *rowObj;

                for (colNum = 0; colNum < nrColumns; colNum++) {
                    templateV[colNum*2 + 1] = elemV[rowNum * nrColumns + colNum];
                }
                rowObj = Tcl_NewListObj(nrColumns * 2, templateV);
                if (Tcl_ListObjAppendElement(interp, resObj, rowObj) != TCL_OK) {
                    Tcl_DecrRefCount(rowObj);
                    goto error;
                }
            }
            break;

        case Dbi_ResultDict:
            {   Tcl_Obj* idxObj = Tcl_NewLongObj(rowNum), *rowObj;

                rowObj = Tcl_NewDictObj();
                for (colNum = 0; colNum < nrColumns; colNum++) {
                    if (Tcl_DictObjPut(interp, rowObj,
                                       colV[colNum],
                                       elemV[rowNum * nrColumns + colNum]) != TCL_OK) {
                        Tcl_DecrRefCount(rowObj);
                        goto error;
                    }
                }
                if (Tcl_DictObjPut(interp, resObj, idxObj, rowObj) != TCL_OK) {
                    Tcl_DecrRefCount(idxObj);
                    Tcl_DecrRefCount(rowObj);
                }

                break;
            }

        case Dbi_ResultLists:
            {
                Tcl_Obj *rowsObj = Tcl_NewListObj(nrColumns, &elemV[rowNum * nrColumns]);
                if (Tcl_ListObjAppendElement(interp, resObj, rowsObj) != TCL_OK) {
                    Tcl_DecrRefCount(rowsObj);
                    goto error;
                }
            }
            break;

        case Dbi_ResultSets:
            {
                set = Ns_SetCreate("r");
                for (colNum = 0; colNum < nrColumns; colNum++) {
                    int        valueLength, attrLength;
                    const char *attrString = Tcl_GetStringFromObj(elemV[colNum], &attrLength);
                    const char *valueString = Tcl_GetStringFromObj(elemV[rowNum * nrColumns + colNum],
                                                                   &valueLength);
                    Ns_SetPutSz(set,
                                attrString, attrLength,
                                valueString, valueLength);
                }
                Ns_TclEnterSet(interp, set, 0);
            }
            break;

        case Dbi_ResultFlatList: break;
        }

    }


    status = TCL_OK;

 done:
    if (templateV != NULL) {
        ns_free(templateV);
    }
    return status;

 error:

    status = TCL_ERROR;
    goto done;
}

#if 0

/*
 *----------------------------------------------------------------------
 *
 * ForeachObjCmd --
 *
 *      Implements dbi_foreach.
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
ForeachObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    unsigned int  colIdx, numCols;
    const char   *colName;
    Tcl_Obj      *valueObj, *queryObj, *bodyObj;
    Tcl_Obj      *poolObj = NULL, *valuesObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    int           end, status, maxRows = -1, autoNull = 0;

    Ns_ObjvSpec opts[] = {
        {"-db",        Ns_ObjvObj,    &poolObj,       NULL},
        {"-autonull",  Ns_ObjvBool,   &autoNull,      (void *) NS_TRUE},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr,    NULL},
        {"-bind",      Ns_ObjvObj,    &valuesObj,     NULL},
        {"-max",       Ns_ObjvInt,    &maxRows,       NULL},
        {"--",         Ns_ObjvBreak,  NULL,           NULL},
        {NULL, NULL, NULL, NULL}
    };
    Ns_ObjvSpec args[] = {
        {"query",      Ns_ObjvObj, &queryObj,    NULL},
        {"body",       Ns_ObjvObj, &bodyObj,    NULL},
        {NULL, NULL, NULL, NULL}
    };
    if (Ns_ParseObjv(opts, args, interp, 1, objc, objv) != NS_OK) {
        return TCL_ERROR;
    }

    /*
     * Get a handle, prepare, bind, and run the query.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, queryObj, valuesObj, maxRows, 0, autoNull,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    status = TCL_OK;
    numCols = Dbi_NumColumns(handle);

    while ((status = NextRow(interp, handle, &end)) == TCL_OK && !end) {

        for (colIdx = 0; colIdx < numCols; colIdx++) {
            if (ColumnValue(interp, handle, colIdx, &valueObj) != TCL_OK) {
                goto error;
            }

            if (Dbi_ColumnName(handle, colIdx, &colName) != NS_OK) {
                Dbi_TclErrorResult(interp, handle);
                Tcl_DecrRefCount(valueObj);
                goto error;
            }

            Tcl_SetVar2Ex(interp, colName, NULL, valueObj, 0);
        }

        status = Tcl_EvalObjEx(interp, bodyObj, 0);

        if (status != TCL_OK) {
            if (status == TCL_BREAK) {
                status = TCL_OK;
            } else if (status == TCL_CONTINUE) {
                continue;
            } else {
                break;
            }
        }
    }

 done:
    PutHandle(idataPtr, handle);

    return status;

 error:

    status = TCL_ERROR;
    goto done;
}
#endif


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
DmlObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    Tcl_Obj      *queryObj, *poolObj = NULL, *valuesObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    int           autoNull = 0;

    Ns_ObjvSpec opts[] = {
        {"-db",        Ns_ObjvObj,    &poolObj,       NULL},
        {"-autonull",  Ns_ObjvBool,   &autoNull,      (void *) NS_TRUE},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr,    NULL},
        {"-bind",      Ns_ObjvObj,    &valuesObj,     NULL},
        {"--",         Ns_ObjvBreak,  NULL,           NULL},
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
     * Get a handle, prepare, bind, and run the query.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, queryObj, valuesObj, -1, 1, autoNull,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    /*
     * Set the result to number of rows affected in case the driver
     * reports it back.
     */

    if (handle->numRowsHint != DBI_NUM_ROWS_UNKNOWN) {
        Tcl_SetObjResult(interp, Tcl_NewLongObj(handle->numRowsHint));
    }

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
ZeroOrOneRowObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    int foundRow;

    if (RowCmd(arg, interp, objc, objv, &foundRow) != TCL_OK) {
        return TCL_ERROR;
    }
    Tcl_SetObjResult(interp, Tcl_NewBooleanObj(foundRow));

    return TCL_OK;
}

static int
OneRowObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    int foundRow;

    if (RowCmd(arg, interp, objc, objv, &foundRow) != TCL_OK) {
        return TCL_ERROR;
    }
    if (!foundRow) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj("query was not a statement returning rows",-1));
        return TCL_ERROR;
    }
    return TCL_OK;
}

static int
RowCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[],
       int *foundRowPtr)
{
    InterpData   *idataPtr = arg;
    Dbi_Handle   *handle;
    unsigned int  colIdx, numCols;
    Tcl_Obj      *valueObj, *queryObj;
    Tcl_Obj      *poolObj = NULL, *valuesObj = NULL;
    Ns_Time      *timeoutPtr = NULL;
    const char   *column, *varName1, *varName2, *arrayName = NULL;
    int           found, end, status, autoNull = 0;

    Ns_ObjvSpec opts[] = {
        {"-db",        Ns_ObjvObj,    &poolObj,    NULL},
        {"-autonull",  Ns_ObjvBool,   &autoNull,   (void *) NS_TRUE},
        {"-timeout",   Ns_ObjvTime,   &timeoutPtr, NULL},
        {"-bind",      Ns_ObjvObj,    &valuesObj,  NULL},
        {"-array",     Ns_ObjvString, &arrayName,  NULL},
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
     * Get handle, then prepare, bind, and run the query.
     */

    if (Exec(idataPtr, poolObj, timeoutPtr, queryObj, valuesObj, 1, 0, autoNull,
             &handle) != TCL_OK) {
        return TCL_ERROR;
    }

    status = TCL_ERROR;
    found = 0;

    /*
     * Fetch the one and only row.
     */

    if (NextRow(interp, handle, &end) != TCL_OK) {
        goto cleanup;
    }
    if (end) {
        goto done;
    }

    /*
     * Set column-value variable in the callers stack frame, or in the
     * specified array.
     */

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
        if (arrayName != NULL) {
            varName1 = arrayName;
            varName2 = column;
        } else {
            varName1 = column;
            varName2 = NULL;
        }
        if (Tcl_SetVar2Ex(interp, varName1, varName2, valueObj,
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
EvalObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    InterpData      *idataPtr = arg;
    Dbi_Pool        *pool;
    Dbi_Handle      *handle;
    Ns_Time         *timeoutPtr = NULL;
    Tcl_Obj         *scriptObj, *poolObj = NULL;
    int              isolation = -1;
    int              status = TCL_ERROR;

    Ns_ObjvSpec opts[] = {
        {"-db",          Ns_ObjvObj,   &poolObj,    NULL},
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
        && Dbi_Begin(handle, (Dbi_Isolation)isolation) != NS_OK) {
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
CtlObjCmd(ClientData arg, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
{
    InterpData  *idataPtr = arg;
    const char  *server = idataPtr->server;
    Dbi_Pool    *pool;
    Ns_DString   ds;
    int          cmd, newIntValue, result = TCL_OK;
    Tcl_Obj      *resultObj;
    Ns_Time      newTimeValue = {0, 0}, oldTimeValue, *newTimeValuePtr = NULL;
    static const Ns_ObjvTimeRange posTimeRange0 = {{0, 0}, {LONG_MAX, 0}};

    static const char *cmds[] = {
        "bounce", "database", "dblist", "default", "driver",
        "maxhandles", "maxrows", "maxidle", "maxopen", "maxqueries",
        "stats", "timeout", NULL
    };
    enum CmdIdx {
        CBounceCmd, CDatabaseCmd, CDBListCmd, CDefaultCmd, CDriverCmd,
        CMaxHandlesCmd, CMaxRowsCmd, CMaxIdleCmd, CMaxOpenCmd, CMaxQueriesCmd,
        CStatsCmd, CTimeoutCmd
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
    case CDBListCmd:
        Ns_DStringInit(&ds);
        if (Dbi_ListPools(&ds, server) != NS_OK) {
            return TCL_ERROR;
        }
        Tcl_DStringResult(interp, &ds);
        return TCL_OK;

    case CDefaultCmd:
        pool = Dbi_DefaultPool(server);
        if (pool != NULL) {
            Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolName(pool), -1));
        }
        if (objc == 3) {

        }
        return TCL_OK;
    }

    /*
     * All other commands require a db to opperate on.
     */

    if (objc != 3 && objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "db ?args?");
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
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_DriverName(pool), -1));
        return TCL_OK;

    case CDatabaseCmd:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_DatabaseName(pool), -1));
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
      switch (cmd) {
      case CMaxHandlesCmd:
      case CMaxRowsCmd:
      case CMaxQueriesCmd:
          if (Tcl_GetIntFromObj(interp, objv[3], &newIntValue) != TCL_OK) {
              return TCL_ERROR;
          }
          break;

      case CMaxIdleCmd:
      case CMaxOpenCmd:
      case CTimeoutCmd:
          if (Ns_GetTimeFromString(interp, Tcl_GetString(objv[3]), &newTimeValue) != TCL_OK) {
              return TCL_ERROR;
          }
          if (Ns_CheckTimeRange(interp, Tcl_GetString(objv[3]), &posTimeRange0, &newTimeValue) != TCL_OK) {
              return TCL_ERROR;
          }
          newTimeValuePtr = &newTimeValue;
          break;
      }
    } else {
        newIntValue = -1;
        newTimeValuePtr = NULL;
    }

    switch (cmd) {
    case CMaxHandlesCmd:
        resultObj = Tcl_NewIntObj(Dbi_ConfigInt(pool, DBI_CONFIG_MAXHANDLES, newIntValue));
        break;

    case CMaxRowsCmd:
        resultObj = Tcl_NewIntObj(Dbi_ConfigInt(pool, DBI_CONFIG_MAXROWS, newIntValue));
        break;

    case CMaxQueriesCmd:
        resultObj = Tcl_NewIntObj(Dbi_ConfigInt(pool, DBI_CONFIG_MAXQUERIES, newIntValue));
        break;


    case CMaxIdleCmd:
        Dbi_ConfigTime(pool, DBI_CONFIG_MAXIDLE, newTimeValuePtr, &oldTimeValue);
        resultObj = Ns_TclNewTimeObj(&oldTimeValue);
        break;

    case CMaxOpenCmd:
        Dbi_ConfigTime(pool, DBI_CONFIG_MAXOPEN, newTimeValuePtr, &oldTimeValue);
        resultObj = Ns_TclNewTimeObj(&oldTimeValue);
        break;

    case CTimeoutCmd:
        Dbi_ConfigTime(pool, DBI_CONFIG_TIMEOUT, newTimeValuePtr, &oldTimeValue);
        resultObj = Ns_TclNewTimeObj(&oldTimeValue);
        break;

    default:
        resultObj = Tcl_NewStringObj("INVALID CMD", -1);
        result = TCL_ERROR;
        break;
    }
    Tcl_SetObjResult(interp, resultObj);

    return result;
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
 *      on successful return.
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
     int autoNull, Dbi_Handle **handlePtrPtr)
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
        Tcl_SetObjResult(interp, Tcl_NewStringObj("query was not a DML or DDL command",-1));
        goto error;
    } else if (!dml && numCols == 0) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj("query was not a statement returning rows", -1));
        goto error;
    }

    /*
     * Bind values to variable as required and execute the statement.
     */

    if (Dbi_TclBindVariables(interp, handle, dbValues, valuesObj, autoNull) != TCL_OK) {
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

/*
 * Local Variables:
 * mode: c
 * c-basic-offset: 4
 * fill-column: 72
 * indent-tabs-mode: nil
 * End:
 */
