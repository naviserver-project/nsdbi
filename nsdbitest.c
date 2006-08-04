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
 * The following struct manages a handle connection and
 * single result.
 */

typedef struct Connection {
    int        connected;
    Ns_DString ds;
} Connection;


/*
 * Static functions defined in this file.
 */

static Dbi_OpenProc         Open;
static Dbi_CloseProc        Close;
static Dbi_ConnectedProc    Connected;
static Dbi_BindVarProc      Bind;
static Dbi_PrepareProc      Prepare;
static Dbi_PrepareCloseProc PrepareClose;
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
    driver->arg              = "driver context";
    driver->name             = name;
    driver->database         = database;
    driver->openProc         = Open;
    driver->closeProc        = Close;
    driver->connectedProc    = Connected;
    driver->bindVarProc      = Bind;
    driver->prepareProc      = Prepare;
    driver->prepareCloseProc = PrepareClose;
    driver->execProc         = Exec;
    driver->valueProc        = Value;
    driver->columnProc       = Column;
    driver->flushProc        = Flush;
    driver->resetProc        = Reset;

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
    Connection *conn;

    assert(STREQ((char *) arg, "driver context"));
    assert(handle->arg == NULL);

    conn = ns_malloc(sizeof(Connection));
    conn->connected = NS_TRUE;
    Ns_DStringInit(&conn->ds);
    handle->arg = conn;

    Dbi_SetException(handle, "TEST", "extra driver connection info");

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
    Connection *conn = handle->arg;

    assert(STREQ((char *) arg, "driver context"));
    assert(conn != NULL);

    Ns_DStringFree(&conn->ds);
    ns_free(conn);

    /* handle->arg = NULL;  Let dbi do this so we can check. */
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
    Connection *conn = handle->arg;

    assert(STREQ((char *) arg, "driver context"));

    return conn ? conn->connected : NS_FALSE;
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
    assert(STREQ((char *) arg, "driver context"));

    assert(ds != NULL);
    assert(*name != '\0');
    assert(bindIdx >= 0);
    assert(bindIdx <= DBI_MAX_BIND);

    Ns_DStringPrintf(ds, "%d:%s", bindIdx, name);
}


/*
 *----------------------------------------------------------------------
 *
 * Prepare --
 *
 *      Prepare the given chunk of SQL for execution.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Prepare(Dbi_Handle *handle, CONST char *sql, int length,
        unsigned int id, unsigned int nqueries,
        void **stmtArg, void *driverArg)
{
    assert(*stmtArg == NULL);

    if (STREQ(sql, "NOPREPARE")) {
        Dbi_SetException(handle, "TEST", "test: prepare failure");
        return NS_ERROR;
    }
    if (nqueries > 0) {
        *stmtArg = (void*) NS_TRUE;
    }
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * PrepareClose --
 *
 *      Cleanup a prepared statement.
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
PrepareClose(void *stmtArg, void *driverArg)
{
    Ns_Log(Debug, "nsdbitest: PrepareClose: stmtArg: %d",
           (int) stmtArg);
}


/*
 *----------------------------------------------------------------------
 *
 * Exec --
 *
 *      Execute a query.
 *
 * Results:
 *      DBI_ROWS, DBI_DML or DBI_EXEC_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static DBI_EXEC_STATUS
Exec(Dbi_Handle *handle, CONST char *sql, int length,
     CONST char **values, unsigned int *lengths, int nvalues,
     int *ncolsPtr, int *nrowsPtr,
     void *stmtArg, void *driverArg)
{
    Connection      *conn = handle->arg;
    char             cmd[64];
    DBI_EXEC_STATUS  dbistat = DBI_EXEC_ERROR;
    int              n, i, rest = 0;

    assert(STREQ((char *) driverArg, "driver context"));

    assert(conn != NULL);
    assert(conn->connected == NS_TRUE);
    assert(Ns_DStringLength(&conn->ds) == 0);

    assert(nvalues <= DBI_MAX_BIND);
    assert((nvalues == 0 && values == NULL && lengths == NULL)
           || (nvalues > 0 && values != NULL && lengths != NULL));

    assert(ncolsPtr != NULL);
    assert(nrowsPtr != NULL);

    n = sscanf(sql, "%s %d %d %n", cmd, ncolsPtr, nrowsPtr, &rest);

    if (n >= 1) {
        if (STREQ(cmd, "DML")) {
            dbistat = DBI_EXEC_DML;
        } else if (STREQ(cmd, "ROWS")) {
            for (i = 0; i < nvalues; i++) {
                Tcl_DStringAppendElement(&conn->ds, values[i]);
            }
            dbistat = DBI_EXEC_ROWS;
        } else if (STREQ(cmd, "SLEEP")) {
            sleep(*ncolsPtr);
            dbistat = DBI_EXEC_ROWS;
        } else if (STREQ(cmd, "ERROR")) {
            Dbi_SetException(handle, "TEST", "driver error");
        } else {
            goto error;
        }
    } else {
    error:
        Dbi_SetException(handle, "TEST", "nsdbitest query syntax error");
    }
    Tcl_DStringAppendElement(&conn->ds, sql + rest);

    return dbistat;
}


/*
 *----------------------------------------------------------------------
 *
 * Value --
 *
 *      Fetch the value of the given row and column.
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
Value(Dbi_Handle *handle, int col, int row,
      CONST char **valuePtr, int *lengthPtr, void *arg)
{
    Connection *conn = handle->arg;

    assert(STREQ((char *) arg, "driver context"));

    assert(conn != NULL);
    assert(conn->connected == NS_TRUE);
    assert(Ns_DStringLength(&conn->ds) > 0);

    assert(valuePtr != NULL);
    assert(lengthPtr != NULL);

    if (row == 0 && col == 0) {
        *valuePtr  = Ns_DStringValue(&conn->ds);
        *lengthPtr = Ns_DStringLength(&conn->ds);
    } else {
        *valuePtr  = "v";
        *lengthPtr = 1;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Column --
 *
 *      Fetch the name of the given column.
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
Column(Dbi_Handle *handle, int col,
       CONST char **columnPtr, int *lengthPtr, void *arg)
{
    Connection *conn = handle->arg;

    assert(STREQ((char *) arg, "driver context"));

    assert(conn != NULL);
    assert(conn->connected == NS_TRUE);
    assert(Ns_DStringLength(&conn->ds) > 0);

    assert(columnPtr != NULL);
    assert(lengthPtr != NULL);

    *columnPtr = "c";
    *lengthPtr = 1;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Flush --
 *
 *      Clear the current result, which should discard any
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
Flush(Dbi_Handle *handle, void *arg)
{
    Connection *conn = handle->arg;

    assert(STREQ((char *) arg, "driver context"));
    assert(conn != NULL);

    Ns_DStringTrunc(&conn->ds, 0);
}


/*
 *----------------------------------------------------------------------
 *
 * Reset --
 *
 *      Reset the db handle. For testing, ensure the handle arg, which
 *      we use here only to store the current result, is NULL.
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
    Connection *conn = handle->arg;

    assert(STREQ((char *) arg, "driver context"));
    assert(conn != NULL);
    assert(Ns_DStringLength(&conn->ds) == 0);

    return NS_OK;
}
