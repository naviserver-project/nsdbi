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
    Dbi_BindRowProc      *bindProc;
    Dbi_GetRowProc       *getProc;
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
            case Dbi_BindRowId:
                driverPtr->bindProc = (Dbi_BindRowProc *) procs->func;
                break;
            case Dbi_GetRowId:
                driverPtr->getProc = (Dbi_GetRowProc *) procs->func;
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
    char      *name = NULL;

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
 *      Pointer to Ns_Set of selected columns or NULL on error.
 *
 * Side effects:
 *      SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

Ns_Set *
Dbi_Select(Dbi_Handle *handle, char *sql)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    Ns_Set *setPtr = NULL;

    if (!handle->fetchingRows) {
        Ns_Log(Error, "%s[%s]: no rows waiting to bind",
               handle->poolPtr->driver, handle->poolPtr->name);
        return setPtr;
    }

    if (driverPtr != NULL
        && driverPtr->execProc != NULL
        && handle->connected) {

        if (Dbi_Exec(handle, sql) == DBI_ROWS) {
            setPtr = Dbi_BindRow(handle);
        } else {
            if (handle->dsExceptionMsg.length == 0) {
                Dbi_SetException(handle, "DBI",
                    "Query was not a statement returning rows.");
            }
        }
    }

    return setPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Exec --
 *
 *       Execute an SQL statement.
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
    int        status = NS_ERROR;

    if (driverPtr != NULL
        && driverPtr->execProc != NULL
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
 * Dbi_BindRow --
 *
 *      Bind the column names from a pending result set.  This routine
 *      is normally called right after an Dbi_Exec if the result
 *      was DBI_ROWS.
 *
 * Results:
 *      Pointer to Ns_Set.
 *
 * Side effects:
 *      Column names of result rows are set in the Ns_Set. 
 *
 *----------------------------------------------------------------------
 */

Ns_Set *
Dbi_BindRow(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    Ns_Set    *setPtr = NULL;

    if (driverPtr != NULL
        && driverPtr->bindProc != NULL
        && handle->connected) {

        Ns_SetTrunc(handle->row, 0);
        setPtr = (*driverPtr->bindProc)(handle);
    }

    return setPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetRow --
 *
 *      Fetch the next row waiting in a result set.  This routine
 *      is normally called repeatedly after an Dbi_Select or
 *      an Dbi_Exec and Dbi_BindRow.
 *
 * Results:
 *      DBI_END_DATA if there are no more rows, NS_OK or NS_ERROR
 *      otherwise.
 *
 * Side effects:
 *      The values of the given set are filled in with those of the
 *      next row.
 *
 *----------------------------------------------------------------------
 */
int
Dbi_GetRow(Dbi_Handle *handle, Ns_Set *row)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    Dbi_Pool  *poolPtr   = handle->poolPtr;
    int        status = NS_ERROR;

    if (!handle->fetchingRows) {
        Ns_Log(Error, "%s[%s]: no waiting rows", poolPtr->driver, poolPtr->name);
        return status;
    }
    if (handle->currentRow == handle->numRows) {
        return DBI_END_DATA;
    }

    if (driverPtr != NULL
        && driverPtr->getProc != NULL
        && handle->connected) {

        status = (*driverPtr->getProc)(handle, row);
        handle->currentRow++;

        if (status == DBI_END_DATA) {
            handle->fetchingRows = NS_FALSE;
        }
    }

    return status;
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
        handle->currentRow = 0;
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
        handle->currentRow = 0;
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
    int        status = NS_ERROR;

    if (driverPtr != NULL
        && driverPtr->resetProc != NULL
        && handle->connected) {

        status = (*driverPtr->resetProc)(handle);
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
 * DbiServerInit --
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
        handle->connected = NS_FALSE;
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
