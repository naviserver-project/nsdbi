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

static Dbi_Pool* GetPool(char *server, Tcl_Interp *interp, char *pool);
static Dbi_Handle *GetHandle(char *server, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);
static int Exception(Tcl_Interp *interp, char *info, char *fmt, ...);
static int DbiException(Tcl_Interp *interp, Dbi_Handle *handle);

static Tcl_ObjCmdProc
    Tcl0or1rowCmd, Tcl1rowCmd, TclRowsCmd, TclDmlCmd, TclPoolCmd,
    TclPoolsCmd, TclDefaultpoolCmd, TclBouncepoolCmd;

/*
 * Static variables defined in this file.
 */

static struct Cmd {
    char           *name;
    Tcl_ObjCmdProc *objProc;
} cmds[] = {
    {"dbi_0or1row",       Tcl0or1rowCmd},
    {"dbi_1row",          Tcl1rowCmd},
    {"dbi_rows",          TclRowsCmd},
    {"dbi_dml",           TclDmlCmd},
    {"dbi_pool",          TclPoolCmd},
    {"dbi_pools",         TclPoolsCmd},
    {"dbi_defaultpool",   TclDefaultpoolCmd},
    {"dbi_bouncepool",    TclBouncepoolCmd},
    {NULL, NULL}
};


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
    char *server = (char *) arg;
    int   i;

    for (i = 0; cmds[i].name != NULL; ++i) {
        Tcl_CreateObjCommand(interp, cmds[i].name, cmds[i].objProc, server, NULL);
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 * GetHandle --
 *
 *      Common function to get a database handle for commands which need
 *      one.  Use server default pool if none specified.
 *
 * Results:
 *      Return pointer to handle or NULL if timeout or other error.
 *
 * Side effects:
 *      Tcl error result may be set on error.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Handle*
GetHandle(char *server, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    Dbi_Handle *handlePtr;
    Dbi_Pool   *poolPtr;
    char       *pool;
    int         timeout, i;

    static CONST char *opts[] = {"pool", "timeout", NULL};
    enum IHandleOptsIdx {IPoolIdx, ITimeoutIdx} opt;

    timeout = -1;
    pool = NULL;

    for (i = 1; i < objc - 1; i += 2) {
        if (Tcl_GetIndexFromObj(interp, objv[i], opts, "option", 0, (int *) &opt) != TCL_OK) {
            Exception(interp, "ERROR", NULL);
            return NULL;
        }
        switch (opt) {
        case IPoolIdx:
            pool = Tcl_GetString(objv[i+1]);
            break;
        case ITimeoutIdx:
            if (Tcl_GetIntFromObj(interp, objv[i+1], &timeout) != TCL_OK) {
                Exception(interp, "ERROR", "invalid timeout value");
                return NULL;
            }
            break;
        }
    }

    poolPtr = GetPool(server, interp, pool);
    if (poolPtr == NULL) {
        return NULL;
    }

    handlePtr = NULL;
    switch (Dbi_PoolTimedGetHandle(&handlePtr, server, poolPtr, timeout)) {
    case NS_TIMEOUT:
        Exception(interp, "TIMEOUT", "wait for database handle timed out");
        break;
    case NS_ERROR:
        Exception(interp, "ERROR", "database handle allocation failed");
        break;
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
 *      Tcl error result may be in iterp.
 *
 *----------------------------------------------------------------------
 */

static Dbi_Pool*
GetPool(char *server, Tcl_Interp *interp, char *pool)
{
    Dbi_Pool *poolPtr;

    if (pool == NULL || *pool == '\0') {
        poolPtr = Dbi_PoolDefault(server);
        if (poolPtr == NULL) {
            Exception(interp, "ERROR", "no pool specified and no default configured");
            return NULL;
        }
    } else {
        poolPtr = Dbi_GetPool(pool);
        if (poolPtr == NULL) {
            Exception(interp, "ERROR", "pool '%s' not valid", pool);
            return NULL;
        }
        if (!Dbi_PoolAllowable(server, poolPtr)) {
            Exception(interp, "ERROR", "pool '%s' not available to server '%s'", pool, server);
            return NULL;
        }
    }

    return poolPtr;
}


/*
 *----------------------------------------------------------------------
 * Exception --
 *
 *      Set a Tcl exception with optional code, info and message.
 *
 * Results:
 *      Return TCL_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Exception(Tcl_Interp *interp, char *info, char *fmt, ...)
{
    Ns_DString ds;
    va_list ap;

    Tcl_SetObjErrorCode(interp, Tcl_NewStringObj("DBI", 3));
    if (info) {
        Tcl_AddObjErrorInfo(interp, info, -1);
    }
    if (fmt) {
        Ns_DStringInit(&ds);
        va_start(ap, fmt);
        Ns_DStringVPrintf(&ds, fmt, ap);
        va_end(ap);
        Tcl_AppendResult(interp, Ns_DStringValue(&ds));
        Ns_DStringFree(&ds);
    }
    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 * DbiException --
 *
 *      Set current handle exception code and message as Tcl exception.
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
DbiException(Tcl_Interp *interp, Dbi_Handle *handle)
{
    Exception(interp,
              handle->cExceptionCode[0] != '\0' ? handle->cExceptionCode : "ERROR",
              "database operation failed: %s",
              handle->dsExceptionMsg.length > 0 ? handle->dsExceptionMsg.string : "ERROR");
    return TCL_ERROR;
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
    char       *server = (char *) clientData;
    Dbi_Handle *handle;
    Ns_Set     *row;
    int         nrows;

    if (objc < 2 || objc > 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? ?-timeout timeout? sql");
        return TCL_ERROR;
    }
    handle = GetHandle(server, interp, objc, objv);
    if (handle == NULL) {
        return TCL_ERROR;
    }

    row = Dbi_0or1Row(handle, Tcl_GetString(objv[objc - 1]), &nrows);
    if (row == NULL) {
        Dbi_PoolPutHandle(handle);
        return DbiException(interp, handle);
    }
    if (nrows == 0) {
        Ns_SetFree(row);
    } else {
        Ns_TclEnterSet(interp, row, NS_TCL_SET_DYNAMIC);
    }
    Dbi_PoolPutHandle(handle);

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
    char       *server = (char *) clientData;
    Dbi_Handle *handle;
    Ns_Set     *row;

    if (objc < 2 || objc > 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? ?-timeout timeout? sql");
        return TCL_ERROR;
    }
    handle = GetHandle(server, interp, objc, objv);
    if (handle == NULL) {
        return TCL_ERROR;
    }

    row = Dbi_1Row(handle, Tcl_GetString(objv[objc - 1]));
    if (row == NULL) {
        Dbi_PoolPutHandle(handle);
        return DbiException(interp, handle);
    }
    Ns_TclEnterSet(interp, row, NS_TCL_SET_DYNAMIC);
    Dbi_PoolPutHandle(handle);

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
    char     *server = clientData;
    char     *pool;
    Dbi_Pool *poolPtr;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "pool");
        return TCL_ERROR;
    }

    pool = Tcl_GetString(objv[1]);
    if ((poolPtr = GetPool(server, interp, pool)) == NULL) {
        return TCL_ERROR;
    }
    if (Dbi_BouncePool(poolPtr) == NS_ERROR) {
        return Exception(interp, "ERROR", "could not bounce pool: %s", pool);
    }

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
    char       *server = (char *) clientData;
    Dbi_Handle *handle;

    if (objc < 2 || objc > 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? ?-timeout timeout? sql");
        return TCL_ERROR;
    }
    handle = GetHandle(server, interp, objc, objv);
    if (handle == NULL) {
        return TCL_ERROR;
    }

    if (Dbi_DML(handle, Tcl_GetString(objv[objc - 1])) != NS_OK) {
        Dbi_PoolPutHandle(handle);
        return DbiException(interp, handle);
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
    char     *server = clientData;
    char     *pool;
    Dbi_Pool *poolPtr;

    static CONST char *opts[] = {
        "datasource", "dbtype", "description", "driver",
        "nhandles", "password", "user", NULL
    };

    enum IPoolIdx {
        IDatasourceIdx, IDbtypeIdx, IDescriptionIdx, IDriverIdx,
        INhandlesIdx, IPasswordIdx, IUserIdx
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
    if ((poolPtr = GetPool(server, interp, pool)) == NULL) {
        return TCL_ERROR;
    }

    switch (opt) {

    case IDatasourceIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->datasource, -1));
        break;

    case IDbtypeIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Dbi_PoolDbType(poolPtr), -1));
        break;

    case IDescriptionIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->description, -1));
        break;

    case IDriverIdx:
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->driver, -1));
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
    char       *server = clientData;
    Ns_DString  ds;

    Ns_DStringInit(&ds);
    if (Dbi_PoolList(&ds, server)) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj(Ns_DStringValue(&ds),
                                                  Ns_DStringLength(&ds)));
    }
    Ns_DStringFree(&ds);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * TclDefaultpoolCmd --
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
TclDefaultpoolCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    char     *server = clientData;
    Dbi_Pool *poolPtr;

    poolPtr = Dbi_PoolDefault(server);
    if (poolPtr != NULL) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj(poolPtr->name, -1));
    }

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
    char        *server = (char *) clientData;
    Dbi_Handle  *handle;
    Ns_Set      *columns;
    Ns_Set       row;
    int          status;
    Tcl_Obj     *value, *result;

    if (objc < 2 || objc > 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "?-pool pool? -timeout timeout? sql");
        return TCL_ERROR;
    }
    handle = GetHandle(server, interp, objc, objv);
    if (handle == NULL) {
        return TCL_ERROR;
    }

    columns = Dbi_Select(handle, Tcl_GetString(objv[objc - 1]));
    if (columns == NULL) {
        Dbi_PoolPutHandle(handle);
        return DbiException(interp, handle);
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
        Dbi_PoolPutHandle(handle);
        return Exception(interp, "ERROR", "failed retrieving rows");
    }
    Dbi_PoolPutHandle(handle);

    return TCL_OK;
}
