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
 * The following structure specifies the driver-specific functions
 * to call for each Dbi_ routine.
 */

typedef struct DbiDriver {
    char                 *name;
    int                   registered;
    Dbi_InitProc         *initProc;
    Dbi_NameProc         *nameProc;
    Dbi_DbTypeProc       *typeProc;
    Dbi_OpenProc         *openProc;
    Dbi_CloseProc        *closeProc;
    Dbi_ExecProc         *execProc;
    Dbi_ValueProc        *valueProc;
    Dbi_ColumnProc       *columnProc;
    Dbi_CancelProc       *cancelProc;
    Dbi_FlushProc        *flushProc;
    Dbi_ResetProc        *resetProc;
    Dbi_TableListProc    *tableListProc;
    Dbi_GetTableInfoProc *tableInfoProc;
    Dbi_BestRowProc      *bestRowProc;
} DbiDriver;

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
 *      Driver structure is allocated and function pointers are set
 *      to the given array of procs.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_RegisterDriver(char *driver, Dbi_Proc *procs)
{
    Tcl_HashEntry *hPtr;
    DbiDriver *driverPtr = NULL;

    hPtr = Tcl_FindHashEntry(&driversTable, driver);
    if (hPtr == NULL) {
        Ns_Log(Error, "dbidrv: no such driver '%s'", driver);
        return NS_ERROR;
    }
    driverPtr = (DbiDriver *) Tcl_GetHashValue(hPtr);
    if (driverPtr->registered) {
        Ns_Log(Error, "dbidrv: a driver is already registered as '%s'",
               driver);
        return NS_ERROR;
    }
    driverPtr->registered = 1;
    
    while (procs->func != NULL) {
        switch (procs->id) {
            case Dbi_InitId:
                driverPtr->initProc = (Dbi_InitProc *) procs->func;
                break;
            case Dbi_NameId:
                driverPtr->nameProc = (Dbi_NameProc *) procs->func;
                break;
            case Dbi_DbTypeId:
                driverPtr->typeProc = (Dbi_DbTypeProc *) procs->func;
                break;
            case Dbi_OpenId:
                driverPtr->openProc = (Dbi_OpenProc *) procs->func;
                break;
            case Dbi_CloseId:
                driverPtr->closeProc = (Dbi_CloseProc *) procs->func;
                break;
            case Dbi_ExecId:
                driverPtr->execProc = (Dbi_ExecProc *) procs->func;
                break;
            case Dbi_ValueId:
                driverPtr->valueProc = (Dbi_ValueProc *) procs->func;
                break;
            case Dbi_ColumnId:
                driverPtr->columnProc = (Dbi_ColumnProc *) procs->func;
                break;
            case Dbi_CancelId:
                driverPtr->cancelProc = (Dbi_CancelProc *) procs->func;
                break;
            case Dbi_FlushId:
                driverPtr->flushProc = (Dbi_FlushProc *) procs->func;
                break;
            case Dbi_ResetId:
                driverPtr->resetProc = (Dbi_ResetProc *) procs->func;
                break;
            case Dbi_TableListId:
                driverPtr->tableListProc = (Dbi_TableListProc *) procs->func;
                break;
            case Dbi_GetTableInfoId:
                driverPtr->tableInfoProc = (Dbi_GetTableInfoProc *) procs->func;
                break;
            case Dbi_BestRowId:
                driverPtr->bestRowProc = (Dbi_BestRowProc *) procs->func;
                break;
                
            default:
                Ns_Log(Error, "dbidrv: unknown driver id '%d'", procs->id);
                return NS_ERROR;
                break;
        }
        ++procs;
    }

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
    DbiDriver *driverPtr = DbiGetDriver(handle);
    char *name = NULL;

    if (driverPtr != NULL && driverPtr->nameProc != NULL) {
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
    DbiDriver *driverPtr = DbiGetDriver(handle);

    if (driverPtr == NULL ||
        driverPtr->typeProc == NULL ||
        handle->connected == NS_FALSE) {

        return NULL;
    }

    return (*driverPtr->typeProc)(handle);
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
Dbi_DML(Dbi_Handle *handle, char *sql)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (driverPtr != NULL
        && driverPtr->execProc != NULL
        && handle->connected) {

        status = Dbi_Exec(handle, sql);

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
Dbi_Select(Dbi_Handle *handle, char *sql, int *nrows)
{
    if (Dbi_Exec(handle, sql) != DBI_ROWS) {
        if (handle->dsExceptionMsg.length == 0) {
            Dbi_SetException(handle, "DBI",
                "Query was not a statement returning rows.");
        }
        return NS_ERROR;
    }
    if (nrows != NULL) {
        *nrows = handle->numRows;
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
Dbi_Exec(Dbi_Handle *handle, char *sql)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    handle->cExceptionCode[0] = '\0';
    Ns_DStringTrunc(&handle->dsExceptionMsg, 0);

    if (driverPtr->execProc != NULL
        && handle->connected) {

        status = (*driverPtr->execProc)(handle, sql);
        DbiLogSql(handle, sql);

        if (status == DBI_ROWS) {
            handle->fetchingRows = NS_TRUE;
            handle->currentRow = 0;
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
    int status;

    if (!handle->fetchingRows || !handle->connected
        || handle->numCols == 0 || handle->numRows == 0) {

        Dbi_SetException(handle, "ERROR", "No waiting rows.");
        return NS_ERROR;
    }
    if (handle->currentCol == handle->numCols) {
        handle->currentCol = 0;
    }
    if ((status = DbiValue(handle, value, vLen)) == NS_ERROR) {
        return NS_ERROR;
    }
    if (column != NULL) {
        if (DbiColumn(handle, column, cLen) == NS_ERROR) {
            return NS_ERROR;
        }
    }
    handle->currentCol++;
    if (handle->currentCol == handle->numCols) {
        if (handle->currentRow == (handle->numRows - 1)) {
            handle->fetchingRows = NS_FALSE;
            status = DBI_END_DATA;
        } else {
            handle->currentRow++;
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
    DbiDriver *driverPtr = DbiGetDriver(handle);

    if (!handle->fetchingRows) {
        Dbi_SetException(handle, "ERROR", "No rows waiting to bind.");
        return NS_ERROR;
    }
    if (driverPtr->columnProc != NULL && handle->connected) {
        return (*driverPtr->columnProc)(handle, handle->currentCol, column, len);
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
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (driverPtr != NULL
        && driverPtr->cancelProc != NULL
        && handle->connected) {

        status = (*driverPtr->cancelProc)(handle);

        handle->fetchingRows = NS_FALSE;
        handle->numRows = handle->numCols = 0;
        handle->currentRow = handle->currentCol = 0;
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
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (driverPtr != NULL
        && driverPtr->flushProc != NULL
        && handle->connected) {

        status = (*driverPtr->flushProc)(handle);

        handle->fetchingRows = NS_FALSE;
        handle->numRows = handle->numCols = 0;
        handle->currentRow = handle->currentCol = 0;
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
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (driverPtr != NULL
        && driverPtr->resetProc != NULL
        && handle->connected) {

        status = (*driverPtr->resetProc)(handle);

        handle->cExceptionCode[0] = '\0';
        Ns_DStringTrunc(&handle->dsExceptionMsg, 0);
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

struct DbiDriver *
DbiLoadDriver(char *driver)
{
    Tcl_HashEntry  *hPtr;
    char           *module, *path;
    int             new;
    DbiDriver      *driverPtr;
    static int      initialized = NS_FALSE;

    if (initialized == NS_FALSE) {
        Tcl_InitHashTable(&driversTable, TCL_STRING_KEYS);
        initialized = NS_TRUE;
    }

    hPtr = Tcl_CreateHashEntry(&driversTable, driver, &new);
    if (new == 0) {
        driverPtr = (DbiDriver *) Tcl_GetHashValue(hPtr);
    } else {
        driverPtr = ns_malloc(sizeof(DbiDriver));
        memset(driverPtr, 0, sizeof(DbiDriver));
        driverPtr->name = Tcl_GetHashKey(&driversTable, hPtr);
        Tcl_SetHashValue(hPtr, driverPtr);
        module = Ns_ConfigGetValue("ns/dbi/drivers", driver);
        if (module == NULL) {
            Ns_Log(Error, "dbidrv: no such driver '%s'", driver);
        } else {
            path = Ns_ConfigGetPath(NULL, NULL, "dbi", "driver", driver, NULL);
            if (Ns_ModuleLoad(driver, path, module, "Dbi_DriverInit")
                    != NS_OK) {
                Ns_Log(Error, "dbidrv: failed to load driver '%s'",
                       driver);
            }
        }
    }
    if (driverPtr->registered == 0) {
        return NULL;
    }

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
DbiDriverInit(char *server, DbiDriver *driverPtr)
{
    if (driverPtr->initProc != NULL &&
        ((*driverPtr->initProc) (server, "dbi", driverPtr->name)) != NS_OK) {

        Ns_Log(Warning, "dbidrv: init proc failed for driver '%s'",
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
    DbiDriver *driverPtr = DbiGetDriver(handle);

    if (driverPtr->openProc == NULL ||
        (*driverPtr->openProc) (handle) != NS_OK) {

        Ns_Log(Error, "nsdbi: failed to open handle in pool '%s': %s",
               handle->poolPtr->name, handle->dsExceptionMsg);
        return NS_ERROR;
    } else {
        Ns_Log(Notice, "nsdbi: opened handle in pool '%s'", handle->poolPtr->name);
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
    DbiDriver *driverPtr = DbiGetDriver(handle);

    Ns_Log(Notice, "nsdbi: closing handle in pool '%s'", handle->poolPtr->name);
    if (handle->connected && driverPtr->closeProc != NULL) {
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
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (driverPtr->valueProc != NULL) {
        status = (*driverPtr->valueProc)(handle, handle->currentRow,
                                         handle->currentCol, value, len);
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
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (driverPtr->columnProc != NULL) {
        status = (*driverPtr->columnProc)(handle, handle->currentCol, column, len);
    }
    return status;
}
