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
 * drv.c --
 *
 *      Routines for handling the loadable db driver interface.
 */

#include "dbi.h"


/*
 * Static variables defined in this file
 */

static Tcl_HashTable driversTable;



/*
 *----------------------------------------------------------------------
 *
 * Dbi_RegisterDriver --
 *
 *      Register dbi procs for a driver.  This routine is called by
 *      driver modules when loaded.
 *
 * Results:
 *      NS_OK if procs registered, NS_ERROR otherwise.
 *
 * Side effects:
 *      Driver structure is stashed in the driversTable.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_RegisterDriver(Dbi_Driver *driver)
{
    Tcl_HashEntry *hPtr;
    int new;

    hPtr = Tcl_CreateHashEntry(&driversTable, driver->name, &new);
    if (!new) {
        Ns_Log(Error, "nsdbi: a driver is already registered as '%s'", driver->name);
        return NS_ERROR;
    }
    Tcl_SetHashValue(hPtr, driver);

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DriverName --
 *
 *      Return the string name of the driver.
 *
 * Results:
 *      String name.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_DriverName(Dbi_Handle *handle)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    char *name = NULL;

    if (driverPtr->nameProc != NULL) {
        name = (*driverPtr->nameProc)(handle);
    }

    return name;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DriverDbType --
 *
 *      Return the string name of the database type (e.g., "sybase").
 *
 * Results:
 *      String name.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_DriverDbType(Dbi_Handle *handle)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    char *type = NULL;

    if (driverPtr->typeProc != NULL && handle->connected) {
        type = (*driverPtr->typeProc)(handle);;
    }

    return type;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DML --
 *
 *      Execute an SQL statement which is expected to be DML.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_DML(Dbi_Handle *handle, char *sql, int *nrows, int *ncols)
{
    int status;

    status = Dbi_Exec(handle, sql, nrows, ncols);
    if (status == DBI_DML) {
        status = NS_OK;
    } else {
        if (status == DBI_ROWS) {
            Dbi_SetException(handle, "DBI",
                "Query was not a DML or DDL command.");
            Dbi_Flush(handle);
        }
        status = NS_ERROR;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Select --
 *
 *      Execute an SQL statement which is expected to return rows.
 *
 * Results:
 *      NS_OK/NS_ERROR.
 *
 * Side effects:
 *      SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Select(Dbi_Handle *handle, char *sql, int *nrows, int *ncols)
{
    Handle *handlePtr = (Handle *) handle;

    if (Dbi_Exec(handle, sql, nrows, ncols) != DBI_ROWS) {
        if (Ns_DStringLength(&handlePtr->dsExceptionMsg) == 0) {
            Dbi_SetException(handle, "DBI",
                "Query was not a statement returning rows.");
        }
        return NS_ERROR;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Exec --
 *
 *      Execute an SQL statement.
 *
 * Results:
 *      DBI_DML, DBI_ROWS, or NS_ERROR.
 *
 * Side effects:
 *      SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Exec(Dbi_Handle *handle, char *sql, int *nrows, int *ncols)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;
    int         status    = NS_ERROR;

    handlePtr->cExceptionCode[0] = '\0';
    Ns_DStringTrunc(&handlePtr->dsExceptionMsg, 0);

    if (driverPtr->execProc != NULL && handlePtr->connected) {

        status = (*driverPtr->execProc)(handle, sql,
                                        &handlePtr->numRows, &handlePtr->numCols);
        if (nrows != NULL) {
            *nrows = handlePtr->numRows;
        }
        if (ncols != NULL) {
            *ncols = handlePtr->numCols;
        }
        DbiLogSql(handle, sql);

        if (status == DBI_ROWS) {
            handlePtr->fetchingRows = NS_TRUE;
            handlePtr->currentRow = 0;
            handlePtr->currentCol = 0;
        }
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_NextValue --
 *
 *      Fetch the result from the next column index of the next row. If
 *      column is not null, set the column name also.
 *      This routine is normally called repeatedly after a Dbi_Exec or
 *      a series of Dbi_GetColumn calls.
 *
 * Results:
 *      NS_OK:        the next result value was successfully retrieved
 *      DBI_LAST_COL: the result of the last column in a row
 *      DBI_END_DATA: the result of the last column in the last row
 *      NS_ERROR:     an error occurred retrieving the result
 *
 * Side effects:
 *      The given handles currentCol and currentRow are maintained.
 *
 *----------------------------------------------------------------------
 */
int
Dbi_NextValue(Dbi_Handle *handle, char **value, int *vLen, char **column, int *cLen)
{
    Handle *handlePtr = (Handle *) handle;
    int     status;

    if (!handlePtr->fetchingRows || !handlePtr->connected
        || handlePtr->numCols == 0 || handlePtr->numRows == 0) {

        Dbi_SetException(handle, "DBI", "No waiting rows.");
        return NS_ERROR;
    }
    if (handlePtr->currentCol == handlePtr->numCols) {
        handlePtr->currentCol = 0;
    }

    if ((status = DbiValue(handle, value, vLen)) == NS_ERROR) {
        return NS_ERROR;
    }
    if (column != NULL) {
        if (DbiColumn(handle, column, cLen) == NS_ERROR) {
            return NS_ERROR;
        }
    }
    handlePtr->currentCol++;
    if (handlePtr->currentCol == handlePtr->numCols) {
        if (handlePtr->currentRow == (handlePtr->numRows - 1)) {
            handlePtr->fetchingRows = NS_FALSE;
            status = DBI_END_DATA;
        } else {
            handlePtr->currentRow++;
            status = DBI_LAST_COL;
        }
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_CurrentColumn --
 *
 *      Fetch the name of the current column in the result set. This
 *      routine is normally called while iterating with Dbi_NextValue.
 *
 * Results:
 *      NS_OK/NS_ERROR
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_CurrentColumn(Dbi_Handle *handle, char **column, int *len)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;

    if (!handlePtr->fetchingRows) {
        Dbi_SetException(handle, "DBI", "No rows waiting to bind.");
        return NS_ERROR;
    }
    if (driverPtr->columnProc != NULL && handlePtr->connected) {
        return (*driverPtr->columnProc)(handle, handlePtr->currentCol, column, len);
    }

    return NS_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Cancel --
 *
 *      Cancel the execution of a select and dump pending rows.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Depending on the driver, a running select call which executes
 *      as rows are fetched may be interrupted.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Cancel(Dbi_Handle *handle)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;
    int         status = NS_ERROR;

    if (driverPtr->cancelProc != NULL && handlePtr->connected) {

        status = (*driverPtr->cancelProc)(handle);

        handlePtr->fetchingRows = NS_FALSE;
        handlePtr->numRows = handlePtr->numCols = 0;
        handlePtr->currentRow = handlePtr->currentCol = 0;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Flush --
 *
 *      Flush rows pending in a result set.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Rows waiting in the result set are dumped, perhaps by simply
 *      fetching them over one by one.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Flush(Dbi_Handle *handle)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;
    int         status = NS_ERROR;

    if (driverPtr->flushProc != NULL
        && handlePtr->connected
        && handlePtr->fetchingRows) {

        status = (*driverPtr->flushProc)(handle);

        handlePtr->fetchingRows = NS_FALSE;
        handlePtr->numRows = handlePtr->numCols = 0;
        handlePtr->currentRow = handlePtr->currentCol = 0;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ResetHandle --
 *
 *      Reset a handle after a cancel operation.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Handle is available for new commands.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ResetHandle (Dbi_Handle *handle)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;
    int         status = NS_ERROR;

    if (driverPtr->resetProc != NULL && handlePtr->connected) {

        status = (*driverPtr->resetProc)(handle);

        handlePtr->fetchingRows = NS_FALSE;
        handlePtr->numRows = handlePtr->numCols = 0;
        handlePtr->currentRow = handlePtr->currentCol = 0;

        handlePtr->cExceptionCode[0] = '\0';
        Ns_DStringTrunc(&handlePtr->dsExceptionMsg, 0);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiLoadDriver --
 *
 *      Load a database driver for one or more pools.
 *
 * Results:
 *      Pointer to driver structure or NULL on error.
 *
 * Side effects:
 *      Driver module file may be mapped into the process.
 *
 *----------------------------------------------------------------------
 */

struct Dbi_Driver *
DbiLoadDriver(char *name)
{
    Tcl_HashEntry  *hPtr;
    Dbi_Driver     *driverPtr;
    char           *module, *path;
    static int      initialized = NS_FALSE;

    if (initialized == NS_FALSE) {
        Tcl_InitHashTable(&driversTable, TCL_STRING_KEYS);
        initialized = NS_TRUE;
    }

    /*
     * Check if driver was already loaded.
     */

    hPtr = Tcl_FindHashEntry(&driversTable, name);
    if (hPtr != NULL) {
        driverPtr = (Dbi_Driver *) Tcl_GetHashValue(hPtr);
        return driverPtr;
    }

    /*
     * Load new driver.
     */

    module = Ns_ConfigGetValue("ns/dbi/drivers", name);
    if (module == NULL) {
        Ns_Log(Error, "nsdbi: no such driver '%s'", name);
        return NULL;
    }
    path = Ns_ConfigGetPath(NULL, NULL, "dbi", "driver", name, NULL);
    if (Ns_ModuleLoad(name, path, module, "Dbi_DriverInit") != NS_OK) {
        Ns_Log(Error, "nsdbi: failed to load driver '%s'", name);
        return NULL;
    }
    hPtr = Tcl_FindHashEntry(&driversTable, name);
    driverPtr = (Dbi_Driver *) Tcl_GetHashValue(hPtr);

    return driverPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiDriverInit --
 *
 *      Invoke driver provided server init proc (e.g., to add driver
 *      specific Tcl commands).
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
DbiDriverInit(char *server, Dbi_Driver *driverPtr)
{
    if (driverPtr->initProc != NULL &&
        ((*driverPtr->initProc) (server, "dbi", driverPtr->name)) != NS_OK) {

        Ns_Log(Warning, "nsdbi: init proc failed for driver '%s'",
               driverPtr->name);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * DbiOpen --
 *
 *      Open a connection to the database.  This routine is called
 *      from the pool routines in dbiinit.c.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Database may be connected by driver specific routine.
 *
 *----------------------------------------------------------------------
 */

int
DbiOpen(Dbi_Handle *handle)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;

    if (driverPtr->openProc == NULL ||
        (*driverPtr->openProc) (handle) != NS_OK) {

        Ns_Log(Error, "nsdbi: failed to open handle in pool '%s': %s",
               handlePtr->poolPtr->name,
               Ns_DStringValue(&handlePtr->dsExceptionMsg));
        return NS_ERROR;
    } else {
        Ns_Log(Notice, "nsdbi: opened handle in pool '%s'", handlePtr->poolPtr->name);
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiClose --
 *
 *      Close a connection to the database.  This routine is called
 *      from the pool routines in dbiinit.c
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
DbiClose(Dbi_Handle *handle)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);

    Ns_Log(Notice, "nsdbi: closing handle in pool '%s'", handle->poolPtr->name);
    if (driverPtr->closeProc != NULL && handle->connected) {
        (*driverPtr->closeProc)(handle);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * DbiValue --
 *
 *      Fetch the current result value identified by the given handles
 *      currentRow and currentCol.
 *
 * Results:
 *      NS_OK/NS_ERROR
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */
int
DbiValue(Dbi_Handle *handle, char **value, int *len)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;
    int         status = NS_ERROR;

    if (driverPtr->valueProc != NULL) {
        status = (*driverPtr->valueProc)(handle, handlePtr->currentRow,
                                         handlePtr->currentCol, value, len);
    }
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiColumn --
 *
 *      Fetch the current column identified by the given handles
 *      currentCol.
 *
 * Results:
 *      NS_OK/NS_ERROR
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */
int
DbiColumn(Dbi_Handle *handle, char **column, int *len)
{
    Dbi_Driver *driverPtr = DbiGetDriver(handle);
    Handle     *handlePtr = (Handle *) handle;
    int         status = NS_ERROR;

    if (driverPtr->columnProc != NULL) {
        status = (*driverPtr->columnProc)(handle, handlePtr->currentCol, column, len);
    }
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiGetDriver --
 *
 *      Return a pointer to the driver structure for a handle.
 *
 * Results:
 *      Pointer to driver.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

Dbi_Driver *
DbiGetDriver(Dbi_Handle *handle)
{
    Pool *poolPtr = (Pool *) handle->poolPtr;

    return poolPtr->driverPtr;
}
