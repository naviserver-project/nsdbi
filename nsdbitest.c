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
static Dbi_BindVarProc      Bind;
static Dbi_ExecProc         Exec;
static Dbi_ValueProc        Value;
static Dbi_ColumnProc       Column;
static Dbi_FlushProc        Flush;
static Dbi_ResetProc        Reset;



/*
 *----------------------------------------------------------------------
 *
 * Dbi_ModuleInit --
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
    CONST char *name     = "drivername";
    CONST char *database = "dbname";

    driver = ns_calloc(1, sizeof(Dbi_Driver));
    driver->name          = name;
    driver->database      = database;
    driver->openProc      = Open;
    driver->closeProc     = Close;
    driver->bindVarProc   = Bind;
    driver->execProc      = Exec;
    driver->valueProc     = Value;
    driver->columnProc    = Column;
    driver->flushProc     = Flush;
    driver->resetProc     = Reset;

    return Dbi_RegisterDriver(server, module, driver);
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

static int
Bind(Ns_DString *ds, int bindIdx, void *arg)
{
    Ns_DStringPrintf(ds, "@%d@", bindIdx);

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Exec --
 *
 *      Execute a statement using the given handle.
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
Exec(Dbi_Handle *handle, Dbi_Statement *stmt, int *nrows, int *ncols, void *arg)
{
    char *sql;
    int   status;

    sql = stmt->dsBoundSql.string;

    if (STREQ(sql, "DML")) {
        status = DBI_DML;
    } else if (sscanf(sql, "%d %d", nrows, ncols) == 2) {
        status = DBI_ROWS;
    } else {
        Dbi_SetException(handle, "TEST", "nsdbitest error");
        status = NS_ERROR;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Value --
 *
 *      Fetch the value of the given row and column. For testing, all
 *      columns are named "v".
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
Value(Dbi_Handle *handle, Dbi_Statement *stmt, int rowIdx, int colIdx,
      CONST char **value, int *len, void *arg)
{
    *value = "v";
    *len = 1;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Column --
 *
 *      Fetch the name of the given column. For testing, all columns
 *      are named "c".
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
Column(Dbi_Handle *handle, Dbi_Statement *stmt, int colIdx, CONST char **column,
       int *len, void *arg)
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
 *      Clear the current result, which discards any pending rows.
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
Flush(Dbi_Statement *stmt, void *arg)
{
    stmt->arg = NULL;
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
