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
    char          *server;
    int            cleanup;
    Tcl_HashTable  dbs;
} InterpData;

/*
 * Static functions defined in this file
 */

static int  DbiFail(Tcl_Interp *interp, Dbi_Handle *handle, char *cmd);
static void EnterDbHandle(InterpData *idataPtr, Tcl_Interp *interp, Dbi_Handle *handle);
static int  DbiGetHandle(InterpData *idataPtr, Tcl_Interp *interp, char *handleId,
                         Dbi_Handle **handlePtrPtr, Tcl_HashEntry **hashPtrPtr);
static int  DbiGetFreshHandle(InterpData *idataPtr, Tcl_Interp *interp, char *handleId,
                              Dbi_Handle **handlePtrPtr, Tcl_HashEntry **hashPtrPtr);
static Dbi_Handle* DbiGetHandle2(InterpData *idataPtr, Tcl_Interp *interp, char *pool);
static Tcl_InterpDeleteProc FreeData;
static Ns_TclDeferProc ReleaseHandles;

static Tcl_ObjCmdProc Tcl0or1rowCmd, Tcl1rowCmd, TclBindrowCmd, TclBouncepoolCmd,
    TclCancelCmd, TclDisconnectCmd, TclDmlCmd, TclErrorcodeCmd, TclErrormsgCmd,
    TclExceptionCmd, TclExecCmd, TclFlushCmd, TclGethandleCmd, TclGetrowCmd,
    TclPoolCmd, TclPoolsCmd, TclReleasehandleCmd, TclResethandleCmd, TclRowsCmd,
    TclSelectCmd, TclSetexceptionCmd, TclVerboseCmd;

/*
 * Static variables defined in this file.
 */

static char *datakey = "nsdbi:data";

static struct Cmd {
    char           *name;
    Tcl_ObjCmdProc *objProc;
} cmds[] = {
    {"dbi_0or1row",       Tcl0or1rowCmd},
    {"dbi_1row",          Tcl1rowCmd},
    {"dbi_bindrow",       TclBindrowCmd},
    {"dbi_bouncepool",    TclBouncepoolCmd},
    {"dbi_cancel",        TclCancelCmd},
    {"dbi_disconnect",    TclDisconnectCmd},
    {"dbi_dml",           TclDmlCmd},
    {"dbi_errorcode",     TclErrorcodeCmd},
    {"dbi_errormsg",      TclErrormsgCmd},
    {"dbi_exception",     TclExceptionCmd},
    {"dbi_exec",          TclExecCmd},
    {"dbi_flush",         TclFlushCmd},
    {"dbi_gethandle",     TclGethandleCmd},
    {"dbi_getrow",        TclGetrowCmd},
    {"dbi_pool",          TclPoolCmd},
    {"dbi_pools",         TclPoolsCmd},
    {"dbi_releasehandle", TclReleasehandleCmd},
    {"dbi_resethandle",   TclResethandleCmd},
    {"dbi_rows",          TclRowsCmd},
    {"dbi_select",        TclSelectCmd},
    {"dbi_setexception",  TclSetexceptionCmd},
    {"dbi_verbose",       TclVerboseCmd},
    {NULL, NULL}
};


/*
 *----------------------------------------------------------------------
 *
 * Dbi_TclGetHandle --
 *
 *      Get database handle from its handle id.
 *
 * Results:
 *      See DbiGetHandle().
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_TclGetHandle(Tcl_Interp *interp, char *id, Dbi_Handle **handlePtr)
{
    InterpData *idataPtr;

    idataPtr = Tcl_GetAssocData(interp, datakey, NULL);
    if (idataPtr == NULL) {
        return TCL_ERROR;
    }
    return DbiGetHandle(idataPtr, interp, id, handlePtr, NULL);
}


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
    int         i;

    /*
     * Initialize the per-interp data.
     */

    idataPtr = ns_malloc(sizeof(InterpData));
    idataPtr->server = arg;
    idataPtr->cleanup = 0;
    Tcl_InitHashTable(&idataPtr->dbs, TCL_STRING_KEYS);
    Tcl_SetAssocData(interp, datakey, FreeData, idataPtr);

    for (i = 0; cmds[i].name != NULL; ++i) {
        Tcl_CreateObjCommand(interp, cmds[i].name, cmds[i].objProc, idataPtr, NULL);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiGetHandle --
 *
 *      Get database handle from its handle id.
 *
 * Results:
 *      Return TCL_OK if handle is found or TCL_ERROR otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
DbiGetHandle(InterpData *idataPtr, Tcl_Interp *interp, char *id, Dbi_Handle **handlePtrPtr,
             Tcl_HashEntry **hashPtrPtr)
{
    Tcl_HashEntry  *hashPtr;

    hashPtr = Tcl_FindHashEntry(&idataPtr->dbs, id);
    if (hashPtr == NULL) {
        Tcl_AppendResult(interp, "invalid database id:  \"", id, "\"", NULL);
        return TCL_ERROR;
    }
    *handlePtrPtr = (Dbi_Handle *) Tcl_GetHashValue(hashPtr);
    if (hashPtrPtr != NULL) {
        *hashPtrPtr = hashPtr;
    }
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiGetFreshHandle --
 *
 *      Get database handle from its handle id and reset error code, msg.
 *
 * Results:
 *      Return TCL_OK if handle is found or TCL_ERROR otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
DbiGetFreshHandle(InterpData *idataPtr, Tcl_Interp *interp, char *id, Dbi_Handle **handlePtrPtr,
                  Tcl_HashEntry **hashPtrPtr)
{
    Dbi_Handle *handlePtr;

    if (DbiGetHandle(idataPtr, interp, id, &handlePtr, hashPtrPtr) != TCL_OK) {
        return TCL_ERROR;
    }
    Ns_DStringFree(&handlePtr->dsExceptionMsg);
    handlePtr->cExceptionCode[0] = '\0';
    *handlePtrPtr = handlePtr;

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 * DbiGetHandle2 --
 *
 *      Get database handle from pool.  Use server default pool if none
 *      specified.
 *
 * Results:
 *      Return TCL_OK if handle is found or TCL_ERROR otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Handle*
DbiGetHandle2(InterpData *idataPtr, Tcl_Interp *interp, char *pool)
{
    Dbi_Handle *handle;

    if (!Dbi_PoolAllowable(idataPtr->server, pool)) {
        Tcl_AppendResult(interp, "pool '", pool, "' not available to server '",
                           idataPtr->server, "'.", NULL);
        return NULL;
    }
    if ((handle = Dbi_PoolGetHandle(idataPtr->server, pool)) == NULL) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj("handle allocation failed", -1));
        return NULL;
    }

    return handle;
}


/*
 *----------------------------------------------------------------------
 *
 * EnterDbHandle --
 *
 *      Enter a database handle and create its handle id.
 *
 * Results:
 *      The database handle id is returned as a Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
EnterDbHandle(InterpData *idataPtr, Tcl_Interp *interp, Dbi_Handle *handle)
{
    Tcl_HashEntry *hPtr;
    int            new, next;
    char           buf[100];

    if (!idataPtr->cleanup) {
        Ns_TclRegisterDeferred(interp, ReleaseHandles, idataPtr);
        idataPtr->cleanup = 1;
    }
    next = idataPtr->dbs.numEntries;
    do {
        snprintf(buf, sizeof(buf), "dbi%x", next++);
        hPtr = Tcl_CreateHashEntry(&idataPtr->dbs, buf, &new);
    } while (!new);
    Tcl_AppendElement(interp, buf);
    Tcl_SetHashValue(hPtr, handle);
}


/*
 *----------------------------------------------------------------------
 *
 * DbiFail --
 *
 *      Common routine that creates database failure message.
 *
 * Results:
 *      Return TCL_ERROR and set database failure message as Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
DbiFail(Tcl_Interp *interp, Dbi_Handle *handle, char *cmd)
{
    Tcl_AppendResult(interp, "Database operation \"", cmd, "\" failed", NULL);
    if (handle->cExceptionCode[0] != '\0') {
        Tcl_AppendResult(interp, " (exception ", handle->cExceptionCode,
                         NULL);
        if (handle->dsExceptionMsg.length > 0) {
            Tcl_AppendResult(interp, ", \"", handle->dsExceptionMsg.string,
                             "\"", NULL);
        }
        Tcl_AppendResult(interp, ")", NULL);
    }
    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
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

    Tcl_DeleteHashTable(&idataPtr->dbs);
    ns_free(idataPtr);
}


/*
 *----------------------------------------------------------------------
 *
 * ReleaseHandles --
 *
 *      Release any database handles still held.
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
ReleaseHandles(Tcl_Interp *interp, void *arg)
{
    Dbi_Handle *handlePtr;
    Tcl_HashEntry  *hPtr;
    Tcl_HashSearch  search;
    InterpData *idataPtr = arg;

    hPtr = Tcl_FirstHashEntry(&idataPtr->dbs, &search);
    while (hPtr != NULL) {
        handlePtr = Tcl_GetHashValue(hPtr);
        Dbi_PoolPutHandle(handlePtr);
        hPtr = Tcl_NextHashEntry(&search);
    }
    Tcl_DeleteHashTable(&idataPtr->dbs);
    Tcl_InitHashTable(&idataPtr->dbs, TCL_STRING_KEYS);
    idataPtr->cleanup = 0;
}


/*
 *----------------------------------------------------------------------
 *
 * Tcl0or1rowCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
Tcl0or1rowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    Ns_Set     *row;
    int         nrows;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle sql");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    row = Dbi_0or1Row(handle, Tcl_GetString(objv[2]), &nrows);
    if (row == NULL) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }
    if (nrows == 0) {
        Ns_SetFree(row);
    } else {
        Ns_TclEnterSet(interp, row, NS_TCL_SET_DYNAMIC);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Tcl1rowCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
Tcl1rowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    Ns_Set     *row;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle sql");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    row = Dbi_1Row(handle, Tcl_GetString(objv[2]));
    if (row == NULL) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }
    Ns_TclEnterSet(interp, row, NS_TCL_SET_DYNAMIC);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclBindrowCmd --
 *
 *      ???
 *
 * Results: 
*      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclBindrowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    Ns_Set     *row;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    row = Dbi_BindRow(handle);
    if (row == NULL) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }
    Ns_TclEnterSet(interp, row, NS_TCL_SET_STATIC);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclBouncepoolCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclBouncepoolCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    char *pool;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "pool");
        return TCL_ERROR;
    }

    pool = Tcl_GetString(objv[1]);
    if (Dbi_BouncePool(pool) == NS_ERROR) {
        Tcl_AppendResult(interp, "could not bounce: ", pool, (char *) NULL);
        return TCL_ERROR;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclCancelCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclCancelCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    if (Dbi_Cancel(handle) != NS_OK) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclDisconnectCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclDisconnectCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    DbiDisconnect(handle);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclDmlCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclDmlCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle sql");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    if (Dbi_DML(handle, Tcl_GetString(objv[2])) != NS_OK) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclErrorcodeCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclErrorcodeCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = (InterpData *) clientData;
    Dbi_Handle *handle;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }
    Tcl_SetResult(interp, handle->cExceptionCode, TCL_VOLATILE);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclErrormsgCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclErrormsgCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = (InterpData *) clientData;
    Dbi_Handle *handle;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }
    Tcl_SetResult(interp, handle->dsExceptionMsg.string, TCL_VOLATILE);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclExceptionCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclExceptionCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    Tcl_Obj    *result;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiId");
        return TCL_ERROR;
    }

    if (DbiGetHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }
    result = Tcl_GetObjResult(interp);
    Tcl_ListObjAppendElement(interp, result,
        Tcl_NewStringObj(handle->cExceptionCode, -1));
    Tcl_ListObjAppendElement(interp, result,
        Tcl_NewStringObj(handle->dsExceptionMsg.string, -1));
    Tcl_SetObjResult(interp, result);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclExecCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclExecCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle sql");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    switch (Dbi_Exec(handle, Tcl_GetString(objv[2]))) {
    case DBI_DML:
        Tcl_SetResult(interp, "DBI_DML", TCL_STATIC);
        break;
    case DBI_ROWS:
        Tcl_SetResult(interp, "DBI_ROWS", TCL_STATIC);
        break;
    default:
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
        break;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclFlushCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclFlushCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    if (Dbi_Flush(handle) != NS_OK) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclGethandleCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclGethandleCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData  *idataPtr = clientData;
    Dbi_Handle  *handlePtr;
    Dbi_Handle **handlesPtrPtr;
    int          timeout, nhandles, result;
    char        *pool;
    int          argi = objc;

    timeout = -1;
    if (objc >= 3) {
        if (STREQ(Tcl_GetString(objv[1]), "-timeout")) {
            if (Tcl_GetIntFromObj(interp, objv[2], &timeout) != TCL_OK) {
                return TCL_ERROR;
            }
            argi -= 2;
        } else if (objc > 3) {
            Tcl_WrongNumArgs(interp, 1, objv, "?-timeout timeout? ?pool? ?nhandles?");
            return TCL_ERROR;
        }
    }

    /*
     * Determine the requested number of handles.
     */

    if (argi == 3) {
        if (Tcl_GetIntFromObj(interp, objv[objc-1], &nhandles) != TCL_OK) {
            return TCL_ERROR;
        }
        if (nhandles <= 0) {
            Tcl_SetObjResult(interp, Tcl_NewStringObj("invalid nhandles, should be >= 1", -1));
            return TCL_ERROR;
        }
    } else {
        nhandles = 1;
    }

    /*
     * Determine the pool.
     */
    pool = NULL;
    if (argi >= 2) {
        pool = Tcl_GetString(objv[objc - (argi == 2 ? 1 : 2)]);
    }
    if (pool == NULL) {
        pool = Dbi_PoolDefault(idataPtr->server);
        if (pool == NULL) {
            Tcl_SetObjResult(interp, Tcl_NewStringObj("no defaultpool configured", -1));
            return TCL_ERROR;
        }
    }

    /*
     * Allocate handles and enter them into Tcl.
     */

    if (nhandles == 1) {
        handlesPtrPtr = &handlePtr;
    } else {
        handlesPtrPtr = ns_malloc(nhandles * sizeof(Dbi_Handle *));
    }
    result = Dbi_PoolTimedGetMultipleHandles(handlesPtrPtr, idataPtr->server, pool,
                                             nhandles, timeout);
    if (result == NS_OK) {
        int i;
            
        for (i = 0; i < nhandles; ++i) {
            EnterDbHandle(idataPtr, interp, handlesPtrPtr[i]);
        }
    }
    if (handlesPtrPtr != &handlePtr) {
        ns_free(handlesPtrPtr);
    }
    if (result != NS_TIMEOUT && result != NS_OK) {
        Tcl_AppendStringsToObj(Tcl_GetObjResult(interp), "could not allocate handle",
                               nhandles > 1 ? "s" : "", " from ", pool, NULL);
        return TCL_ERROR;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclGetrowCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclGetrowCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    Ns_Set     *row;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle row");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    if (Ns_TclGetSet2(interp, Tcl_GetString(objv[2]), &row) != TCL_OK) {
        return TCL_ERROR;
    }
    switch (Dbi_GetRow(handle, row)) {
    case NS_OK:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(1));
        break;
    case DBI_END_DATA:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(0));
        break;
    default:
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
        break;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclPoolCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclPoolCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    char       *pool;

    static CONST char *opts[] = {
        "datasource", "dbtype", "default", "description",
        "driver", "nhandles", "password", "user", NULL
    };

    enum IPoolIdx {
        IDatasourceIdx, IDbtypeIdx, IDefaultIdx, IDescriptionIdx,
        IDriverIdx, INhandlesIdx, IPasswordIdx, IUserIdx
    } opt;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "option pool");
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[1], opts, "option", 0, (int *) &opt)
        != TCL_OK) {
        return TCL_ERROR;
    }
    pool = Tcl_GetString(objv[2]);

    switch (opt) {

    case IDatasourceIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolDataSource(pool), -1));
        break;

    case IDbtypeIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolDbType(pool), -1));
        break;

    case IDefaultIdx:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(Dbi_PoolDefault(idata->server) ? 1 : 0));
        break;

    case IDescriptionIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolDescription(pool), -1));
        break;

    case IDriverIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolDriverName(pool), -1));
        break;

    case INhandlesIdx:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(Dbi_PoolNHandles(pool)));
        break;

    case IPasswordIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolPassword(pool), -1));
        break;

    case IUserIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolUser(pool), -1));
        break;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclPoolsCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclPoolsCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idataPtr = clientData;
    Tcl_Obj    *result;
    char       *pool;

    pool = Dbi_PoolList(idataPtr->server);

    if (pool != NULL) {
        result = Tcl_GetObjResult(interp);
        while (*pool != '\0') {
            Tcl_ListObjAppendElement(interp, result, Tcl_NewStringObj(pool, -1));
            pool = pool + strlen(pool) + 1;
        }
        Tcl_SetObjResult(interp, result);
    }
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclReleasehandleCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclReleasehandleCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData    *idata = clientData;
    Dbi_Handle    *handle;
    Tcl_HashEntry *hPtr;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, &hPtr) != TCL_OK) {
        return TCL_ERROR;
    }

    Tcl_DeleteHashEntry(hPtr);
    Dbi_PoolPutHandle(handle);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclResethandleCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclResethandleCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData    *idata = clientData;
    Dbi_Handle    *handle;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    if (Dbi_ResetHandle(handle) != NS_OK) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(NS_OK));

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclRowsCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclRowsCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData  *idata = clientData;
    char        *pool;
    Dbi_Handle  *handle;
    Ns_Set      *columns;
    Ns_Set       row;
    int          status;
    Tcl_Obj     *value, *result;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "sql");
        return TCL_ERROR;
    }
    pool = NULL;
    if ((handle = DbiGetHandle2(idata, interp, pool)) == NULL) {
        return TCL_ERROR;
    }
    columns = Dbi_Select(handle, Tcl_GetString(objv[1]));
    if (columns == NULL) {
        DbiFail(interp, handle, Tcl_GetString(objv[0]));
        Dbi_PoolPutHandle(handle);
        return TCL_ERROR;
    }
    result = Tcl_GetObjResult(interp);
    while ((status = Dbi_GetRow(handle, &row)) == NS_OK) {
        int i;
        for (i = 0; i < Ns_SetSize(&row); ++i) {
            value = Tcl_NewStringObj(Ns_SetValue(&row, i), -1);
            Tcl_ListObjAppendElement(interp, result, value);
        }
    }
    if (status != DBI_END_DATA) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj("failed retrieving rows", -1));
        Dbi_PoolPutHandle(handle);
        return TCL_ERROR;
    }
    Dbi_PoolPutHandle(handle);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclSelectCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclSelectCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    Ns_Set     *row;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle sql");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }

    row = Dbi_Select(handle, Tcl_GetString(objv[2]));
    if (row == NULL) {
        return DbiFail(interp, handle, Tcl_GetString(objv[0]));
    }
    Ns_TclEnterSet(interp, row, NS_TCL_SET_STATIC);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclSetexceptionCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclSetexceptionCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    char       *code;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle code message");
        return TCL_ERROR;
    }
    code = Tcl_GetString(objv[2]);
    if (strlen(code) > 5) {
        Tcl_AppendResult(interp, "code \"", code, "\" more than 5 characters", NULL);
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }
    Dbi_SetException(handle, code, Tcl_GetString(objv[3]));

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclVerboseCmd --
 *
 *      ???
 *
 * Results:
 *      TCL_OK/TCL_ERROR.
 *
 * Side effects:
 *      ???
 *
 *----------------------------------------------------------------------
 */

static int
TclVerboseCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    InterpData *idata = clientData;
    Dbi_Handle *handle;
    int         verbose;

    if (objc != 3 && objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "dbiHandle ?on|off?");
        return TCL_ERROR;
    }
    if (DbiGetFreshHandle(idata, interp, Tcl_GetString(objv[1]), &handle, NULL) != TCL_OK) {
        return TCL_ERROR;
    }
    if (objc == 4) {
        if (Tcl_GetIntFromObj(interp, objv[3], &verbose) != TCL_OK) {
            return TCL_ERROR;
        }
        handle->verbose = verbose;
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(handle->verbose));
    
    return TCL_OK;
}
