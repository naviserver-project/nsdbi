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
    CONST char *configData;
    int         connected;
    char        columnBuf[32];
    Ns_DString  ds;
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
 * Local variables defined in this file.
 */

static Dbi_DriverProc procs[] = {
    {Dbi_OpenProcId,         Open},
    {Dbi_CloseProcId,        Close},
    {Dbi_ConnectedProcId,    Connected},
    {Dbi_BindVarProcId,      Bind},
    {Dbi_PrepareProcId,      Prepare},
    {Dbi_PrepareCloseProcId, PrepareClose},
    {Dbi_ExecProcId,         Exec},
    {Dbi_ValueProcId,        Value},
    {Dbi_ColumnProcId,       Column},
    {Dbi_FlushProcId,        Flush},
    {Dbi_ResetProcId,        Reset},
    {0, NULL}
};


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
    CONST char *name      = "test";
    CONST char *database  = "db";
    char *configData      = "driver config data";

    return Dbi_RegisterDriver(server, module, name, database,
                              procs, configData);
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
Open(ClientData configData, Dbi_Handle *handle)
{
    Connection *conn = handle->driverData;

    assert(STREQ((char *) configData, "driver config data"));
    assert(handle->driverData == NULL);

    if (conn == NULL) {
        conn = ns_malloc(sizeof(Connection));
        conn->configData = configData;
        Ns_DStringInit(&conn->ds);
        handle->driverData = conn;
    } else {
        assert(conn->connected == NS_FALSE);
    }
    conn->connected = NS_TRUE;

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
Close(Dbi_Handle *handle)
{
    Connection *conn = handle->driverData;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

    assert(conn->connected = NS_TRUE);
    assert(Ns_DStringLength(&conn->ds) == 0);

    conn->connected = NS_FALSE; /* Simulate close connection. */
}


/*
 *----------------------------------------------------------------------
 *
 * Connected --
 *
 *      Is the given handle currently connected?
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
Connected(Dbi_Handle *handle)
{
    Connection *conn = handle->driverData;

    return conn ? conn->connected : NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * Bind --
 *
 *      Append the name and index so that both can be verified in tests.
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
Bind(Ns_DString *ds, CONST char *name, int bindIdx)
{
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
Prepare(Dbi_Handle *handle, Dbi_Statement *stmt)
{
    Connection *conn = handle->driverData;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

    /*
     * Simulate prepare-failure.
     */

    if (STREQ(stmt->sql, "NOPREPARE")) {
        Dbi_SetException(handle, "TEST", "test: prepare failure");
        return NS_ERROR;
    }

    /*
     * Simulate preparing the statement on the second request.
     */

    if (stmt->nqueries > 0) {
        stmt->driverData = (void *) NS_TRUE;
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
PrepareClose(Dbi_Handle *handle, Dbi_Statement *stmt)
{
    Connection *conn = handle->driverData;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

    assert(stmt->driverData == (void *) NS_TRUE);

    stmt->driverData = (void *) NS_FALSE;
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
Exec(Dbi_Handle *handle, Dbi_Statement *stmt,
     CONST char **values, unsigned int *lengths, unsigned int nvalues,
     int *ncolsPtr, int *nrowsPtr)
{
    Connection      *conn = handle->driverData;
    char             cmd[64];
    DBI_EXEC_STATUS  dbistat = DBI_EXEC_ERROR;
    int              n, i, rest = 0;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

    assert(conn->connected == NS_TRUE);
    assert(Ns_DStringLength(&conn->ds) == 0);

    assert((stmt->nqueries == 0 && stmt->driverData == (void *) NS_FALSE)
           || stmt->nqueries > 0);

    assert(nvalues <= DBI_MAX_BIND);
    assert(nvalues == 0 || (nvalues > 0 && values != NULL && lengths != NULL));

    assert(ncolsPtr != NULL);
    assert(nrowsPtr != NULL);

    n = sscanf(stmt->sql, "%s %d %d %n", cmd, ncolsPtr, nrowsPtr, &rest);

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
        } else if (STREQ(stmt->sql, "begin transaction")
                   || STREQ(stmt->sql, "commit")
                   || STREQ(stmt->sql, "rollback")) {
            dbistat = DBI_EXEC_ROWS;
        } else {
            goto error;
        }
    } else {
    error:
        Dbi_SetException(handle, "TEST", "nsdbitest query syntax error");
    }
    Tcl_DStringAppendElement(&conn->ds, stmt->sql + rest);

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
      CONST char **valuePtr, int *lengthPtr)
{
    Connection *conn = handle->driverData;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

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
 *      For testing, all columns are named after their index position.
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
       CONST char **columnPtr, int *lengthPtr)
{
    Connection *conn = handle->driverData;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

    assert(conn->connected == NS_TRUE);
    assert(Ns_DStringLength(&conn->ds) > 0);

    assert(columnPtr != NULL);
    assert(lengthPtr != NULL);

    sprintf(conn->columnBuf, "%d", col);
    *columnPtr = conn->columnBuf;
    *lengthPtr = strlen(conn->columnBuf);

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
Flush(Dbi_Handle *handle)
{
    Connection *conn = handle->driverData;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

    Ns_DStringTrunc(&conn->ds, 0);
}


/*
 *----------------------------------------------------------------------
 *
 * Reset --
 *
 *      Reset the db handle. For testing, ensure the result dstring
 *      has zero length, as set by the flush proc previously.
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
Reset(Dbi_Handle *handle)
{
    Connection *conn = handle->driverData;

    assert(conn != NULL);
    assert(STREQ(conn->configData, "driver config data"));

    assert(Ns_DStringLength(&conn->ds) == 0);

    return NS_OK;
}
