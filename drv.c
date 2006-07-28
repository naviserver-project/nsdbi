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
 *      Routines for calling driver callbacks.
 */

#include "dbi.h"

NS_RCSID("@(#) $Header$");



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
Dbi_DriverName(Dbi_Pool *pool)
{
    return DbiDriverForPool(pool)->name;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DatabaseName --
 *
 *      Return the string name of the database type (e.g. "oracle").
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
Dbi_DatabaseName(Dbi_Pool *pool)
{
    return DbiDriverForPool(pool)->database;
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
Dbi_Exec(Dbi_Query *query)
{
    Handle        *handlePtr;
    Dbi_Driver    *driver;
    int            retries = 1, status  = NS_ERROR;

    if (query->handle == NULL || query->stmt == NULL) {
        Ns_Log(Error, "nsdbi: Dbi_Exec: null handle or statement for query.");
        return NS_ERROR;
    }
    handlePtr = (Handle *) query->handle;
    driver    = DbiDriverForHandle(query->handle);

    Dbi_ResetException(query->handle);

    if (driver->execProc != NULL) {

    exec:
        status = (*driver->execProc)(query, driver->arg);
        handlePtr->stats.queries++;

        if (status == DBI_ROWS) {
            if (!query->result.numCols) {
                Dbi_SetException(query->handle, "DBI",
                                 "bug: driver failed to set number of columns");
                return NS_ERROR;
            }
            query->result.fetchingRows = NS_TRUE;
            query->result.currentRow = 0;
            query->result.currentCol = 0;

        } else if (status == NS_ERROR && !DbiConnected(query->handle) && retries--) {
            /*
             * Retry once on a broken connection.
             */
            if (DbiOpen(query->handle) == NS_OK) {
                goto exec;
            }
        }

        DbiLog(query->handle, Debug, "%s",
               Dbi_StatementBoundSQL(query->stmt, NULL));
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
Dbi_NextValue(Dbi_Query *query, CONST char **value, int *vLen, CONST char **column, int *cLen)
{
    Dbi_Driver    *driver;
    int            status;

    if (query->result.fetchingRows == NS_FALSE) {
        return DBI_END_DATA;
    }
    if (!DbiConnected(query->handle)) {
        Dbi_SetException(query->handle, "DBI",
                         "Cannot fetch next value; handle disconnected.");
        return NS_ERROR;
    }

    if (query->result.currentCol == query->result.numCols) {
        query->result.currentCol = 0; /* Reset for next row. */
    }

    driver = DbiDriverForHandle(query->handle);
    status = (*driver->valueProc)(query, value, vLen, driver->arg);
    if (status == NS_ERROR) {
        return NS_ERROR;
    }
    if (column != NULL && cLen != NULL) {
        status = (*driver->columnProc)(query, column, cLen, driver->arg);
        if (status == NS_ERROR) {
            return NS_ERROR;
        }
    }
    query->result.currentCol++;
    if (query->result.currentCol == query->result.numCols) {
        if (query->result.currentRow == (query->result.numRows - 1)) {
            query->result.fetchingRows = NS_FALSE;
            status = DBI_END_DATA;
        } else {
            query->result.currentRow++;
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
 *      fetching them over one by one -- depends on driver.
 *
 *----------------------------------------------------------------------
 */

void
Dbi_Flush(Dbi_Query  *query)
{
    Dbi_Driver *driver = DbiDriverForHandle(query->handle);

    if (driver->flushProc != NULL) {
        (*driver->flushProc)(query, driver->arg);
    }
    query->arg = NULL;
    memset(&query->result, 0, sizeof(query->result));
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
 *      Depends on driver.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ResetHandle(Dbi_Handle *handle)
{
    Dbi_Driver *driver = DbiDriverForHandle(handle);
    int         status = NS_ERROR;

    if (driver->resetProc != NULL) {
        status = (*driver->resetProc)(handle, driver->arg);
        assert(status == NS_OK || handle->arg == NULL);
    }
    Dbi_ResetException(handle);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiConnected --
 *
 *      Is the given database handle currently connected?
 *
 * Results:
 *      NS_TRUE or NS_FALSE.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
DbiConnected(Dbi_Handle *handle)
{
    Dbi_Driver *driver = DbiDriverForHandle(handle);

    if (driver->connectedProc != NULL && handle->arg != NULL) {
        return (*driver->connectedProc)(handle, driver->arg);
    }
    return NS_FALSE;
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
    Dbi_Driver *driver = DbiDriverForHandle(handle);

    if (driver->openProc == NULL ||
        (*driver->openProc)(handle, driver->arg) != NS_OK) {

        DbiLog(handle, Error, "failed to open handle: code: '%s' msg:: %s",
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
    Dbi_Driver *driver = DbiDriverForHandle(handle);

    if (driver->closeProc != NULL && DbiConnected(handle)) {
        (*driver->closeProc)(handle, driver->arg);
    }
    handle->arg = NULL;
}
