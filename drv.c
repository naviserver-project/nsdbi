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

NS_RCSID("@(#) $Header$");


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

CONST char *
Dbi_DriverName(Dbi_Handle *handle)
{
    Dbi_Driver *driver = handle->pool->driver;
    CONST char *name   = NULL;

    if (driver->nameProc != NULL) {
        name = (*driver->nameProc)(handle);
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

CONST char *
Dbi_DriverDbType(Dbi_Handle *handle)
{
    Dbi_Driver *driver = handle->pool->driver;
    CONST char *type   = NULL;

    if (driver->typeProc != NULL && handle->connected) {
        type = (*driver->typeProc)(handle);
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
Dbi_DML(Dbi_Handle *handle, Dbi_Statement *stmt, int *nrows, int *ncols)
{
    int status;

    status = Dbi_Exec(handle, stmt, nrows, ncols);
    if (status == DBI_DML) {
        status = NS_OK;
    } else {
        if (status == DBI_ROWS) {
            Dbi_SetException(handle, "DBI",
                "Query was not a DML or DDL command.");
            Dbi_Flush(stmt);
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
Dbi_Select(Dbi_Handle *handle, Dbi_Statement *stmt, int *nrows, int *ncols)
{
    if (Dbi_Exec(handle, stmt, nrows, ncols) != DBI_ROWS) {
        if (Dbi_ExceptionPending(handle) == NS_FALSE) {
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
Dbi_Exec(Dbi_Handle *handle, Dbi_Statement *stmt, int *nrows, int *ncols)
{
    Statement  *stmtPtr = (Statement *) stmt;
    Dbi_Driver *driver  = stmt->pool->driver;
    int         status  = NS_ERROR;

    Dbi_ResetException(handle);
    stmtPtr->handlePtr = (Handle *) handle;

    if (driver->execProc != NULL && handle->connected) {

        status = (*driver->execProc)(handle, stmt, &stmtPtr->numRows, &stmtPtr->numCols);

        stmtPtr->handlePtr->stats.opps++;
        if (nrows != NULL) {
            *nrows = stmtPtr->numRows;
        }
        if (ncols != NULL) {
            *ncols = stmtPtr->numCols;
        }
        DbiLogSql(stmt);
        if (status == DBI_ROWS) {
            stmt->fetchingRows = NS_TRUE;
            stmtPtr->currentRow = 0;
            stmtPtr->currentCol = 0;
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
Dbi_NextValue(Dbi_Statement *stmt, CONST char **value, int *vLen, CONST char **column, int *cLen)
{
    Statement  *stmtPtr = (Statement *) stmt;
    Dbi_Handle *handle  = (Dbi_Handle *) stmtPtr->handlePtr;
    Dbi_Driver *driver  = stmt->pool->driver;
    char       *msg     = "Cannot fetch next value; no waiting rows.";
    int         status;

    if (!stmt->fetchingRows) {
        if (stmtPtr->currentCol == stmtPtr->numCols) {
            return DBI_END_DATA;
        } else if (!handle) {
            Ns_Log(Error, "nsdbi: %s", msg);
        } else if (!handle->connected) {
            Dbi_SetException(handle, "DBI", msg);
        }
        return NS_ERROR;
    } else {
        if (stmtPtr->currentCol == stmtPtr->numCols) {
            stmtPtr->currentCol = 0;
        }
    }

    status = (*driver->valueProc)(handle, stmt, stmtPtr->currentRow,
                                  stmtPtr->currentCol, value, vLen);
    if (status == NS_ERROR) {
        return NS_ERROR;
    }
    if (column != NULL && cLen != NULL) {
        status = (*driver->columnProc)(handle, stmt, stmtPtr->currentCol, column, cLen);
        if (status == NS_ERROR) {
            return NS_ERROR;
        }
    }
    stmtPtr->currentCol++;
    if (stmtPtr->currentCol == stmtPtr->numCols) {
        if (stmtPtr->currentRow == (stmtPtr->numRows - 1)) {
            stmt->fetchingRows = NS_FALSE;
            status = DBI_END_DATA;
        } else {
            stmtPtr->currentRow++;
            status = DBI_LAST_COL;
        }
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

void
Dbi_Flush(Dbi_Statement *stmt)
{
    Statement  *stmtPtr = (Statement *) stmt;
    Dbi_Driver *driver  = stmt->pool->driver;

    if (driver->flushProc != NULL) {
        (*driver->flushProc)(stmt);
    }
    stmt->arg = NULL;
    stmt->fetchingRows = NS_FALSE;
    stmtPtr->numRows = stmtPtr->numCols = 0;
    stmtPtr->currentRow = stmtPtr->currentCol = 0;
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
 *      Current statement is flushed.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ResetHandle(Dbi_Handle *handle)
{
    Handle     *handlePtr = (Handle *) handle;
    Dbi_Driver *driver    = handle->pool->driver;
    int         status    = NS_ERROR;

    if (handlePtr->stmtPtr != NULL) {
        Dbi_Flush((Dbi_Statement *) handlePtr->stmtPtr);
    }
    if (driver->resetProc != NULL && handle->connected) {
        status = (*driver->resetProc)(handle);
    }
    Dbi_ResetException(handle);

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
    Dbi_Driver     *driver;
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
        driver = (Dbi_Driver *) Tcl_GetHashValue(hPtr);
        return driver;
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
    driver = (Dbi_Driver *) Tcl_GetHashValue(hPtr);

    return driver;
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
DbiDriverInit(char *server, Dbi_Driver *driver)
{
    if (driver->initProc != NULL &&
        ((*driver->initProc) (server, "dbi", driver->name)) != NS_OK) {

        Ns_Log(Error, "nsdbi: init proc failed for driver '%s'",
               driver->name);
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
    Dbi_Driver *driver = handle->pool->driver;

    if (driver->openProc == NULL ||
        (*driver->openProc) (handle) != NS_OK) {

        Ns_Log(Error, "nsdbi: failed to open handle in pool '%s': code: '%s' msg: %s",
               handle->pool->name,
               Dbi_ExceptionCode(handle), Dbi_ExceptionMsg(handle));
        return NS_ERROR;
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
    Dbi_Driver *driver = handle->pool->driver;

    if (driver->closeProc != NULL && handle->connected) {
        (*driver->closeProc)(handle);
        handle->connected = NS_FALSE;
    }
}
