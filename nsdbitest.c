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
 * nsdbitest.c --
 *
 *      Implements a mock DBI database driver for testing.
 */

#include "nsdbi.h"

NS_RCSID("@(#) $Header$");


NS_EXPORT int Ns_ModuleVersion = 1;


/*
 * Static functions defined in this file.
 */

static Dbi_OpenProc         Open;
static Dbi_CloseProc        Close;
static Dbi_ConnectedProc    Connected;
static Dbi_BindVarProc      Bind;
static Dbi_ExecProc         Exec;
static Dbi_ValueProc        Value;
static Dbi_ColumnProc       Column;
static Dbi_FlushProc        Flush;
static Dbi_ResetProc        Reset;



/*
 *----------------------------------------------------------------------
 *
 * Ns_ModuleInit --
 *
 *      Register the driver procs.
 *
 * Results:
 *      NS_OK / NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

NS_EXPORT int
Ns_ModuleInit(CONST char *server, CONST char *module)
{
    Dbi_Driver *driver;
    CONST char *name     = "test";
    CONST char *database = "db";

    driver = ns_calloc(1, sizeof(Dbi_Driver));
    driver->name          = name;
    driver->database      = database;
    driver->openProc      = Open;
    driver->closeProc     = Close;
    driver->connectedProc = Connected;
    driver->bindVarProc   = Bind;
    driver->execProc      = Exec;
    driver->valueProc     = Value;
    driver->columnProc    = Column;
    driver->flushProc     = Flush;
    driver->resetProc     = Reset;

    return Dbi_RegisterDriver(server, module, driver, sizeof(Dbi_Driver));
}


/*
 *----------------------------------------------------------------------
 *
 * Open --
 *
 *      Open a connection to the configured database.
 *
 * Results:
 *      NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Open(Dbi_Handle *handle, void *arg)
{
    handle->arg = (void *) NS_TRUE;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Close --
 *
 *      Close a database connection.
 *
 * Results:
 *      NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
Close(Dbi_Handle *handle, void *arg)
{
    handle->arg = NULL;
}


/*
 *----------------------------------------------------------------------
 *
 * Connected --
 *
 *      Is the gien handle currently connected?
 *
 * Results:
 *      NS_TRUE.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Connected(Dbi_Handle *handle, void *arg)
{
    return handle->arg ? NS_TRUE : NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * Bind --
 *
 *      Append a bind variable place holder to the given dstring.
 *
 * Results:
 *      NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
Bind(Ns_DString *ds, CONST char *name, int bindIdx, void *arg)
{
    Ns_DStringPrintf(ds, "%d:%s", bindIdx, name);
}


/*
 *----------------------------------------------------------------------
 *
 * Exec --
 *
 *      Execute a query.
 *
 * Results:
 *      DBI_ROWS, DBI_DML or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Exec(Dbi_Query *query, void *arg)
{
    Ns_DString *dsPtr;
    CONST char *sql, *value;
    char        cmd[64];
    int         n, i, rest = 0, status = NS_ERROR;

    assert(query->arg == NULL);

    sql = Dbi_StatementBoundSQL(query->stmt, NULL);
    n = sscanf(sql, "%s %d %d %n", cmd,
               &query->result.numRows, &query->result.numCols, &rest);

    dsPtr = ns_malloc(sizeof(Ns_DString));
    Ns_DStringInit(dsPtr);

    if (n >= 1) {
        if (STREQ(cmd, "DML")) {
            status = DBI_DML;
        } else if (STREQ(cmd, "ROWS")) {
            i = 0;
            while (Dbi_QueryGetBindValue(query, i++, &value, NULL) == NS_OK) {
                Tcl_DStringAppendElement(dsPtr, value);
            }
            status = DBI_ROWS;
        } else if (STREQ(cmd, "ERROR")) {
            Dbi_SetException(query->handle, "TEST", "driver error");
        } else {
            goto error;
        }
    } else {
    error:
        Dbi_SetException(query->handle, "TEST", "nsdbitest query syntax error");
    }

    Tcl_DStringAppendElement(dsPtr, sql + rest);
    query->arg = dsPtr;

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Value --
 *
 *      Fetch the value of the current row and column.
 *
 *      For testing, all values are "v", except the first which is
 *      the original SQL statement with driver specific bind
 *      variable notation substituted.
 *
 * Results:
 *      Always NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Value(Dbi_Query *query, CONST char **value, int *len, void *arg)
{
    Ns_DString *dsPtr = query->arg;

    if (query->result.currentRow == 0
        && query->result.currentCol == 0) {

        *value = Ns_DStringValue(dsPtr);
        *len = Ns_DStringLength(dsPtr);
    } else {
        *value = "v";
        *len = 1;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Column --
 *
 *      Fetch the name of the current column.
 *
 *      For testing, all columns are named "c".
 *
 * Results:
 *      Always NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Column(Dbi_Query *query, CONST char **column, int *len, void *arg)
{
    *column = "c";
    *len = 1;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Flush --
 *
 *      Clear the current query result, which should discards any
 *      pending rows.
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
Flush(Dbi_Query *query, void *arg)
{
    Ns_DString *dsPtr = query->arg;

    if (dsPtr != NULL) {
        Ns_DStringFree(dsPtr);
        ns_free(dsPtr);
        query->arg = NULL;
    }
}


/*
 *----------------------------------------------------------------------
 *
 * Reset --
 *
 *      Currently does nothing. What should it do?
 *
 * Results:
 *      Always NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Reset(Dbi_Handle *handle, void *arg)
{
    return NS_OK;
}
