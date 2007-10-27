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

#include "nsdbidrv.h"

NS_RCSID("@(#) $Header$");


NS_EXPORT int Ns_ModuleVersion = 1;


/*
 * The following struct manages a per-handle connection to our
 * imaginary dtabase and a single result.
 */

typedef struct Connection {

    CONST char   *configData;     /* Pointer to per-pool config data. */
    int           connected;      /* Is the handle currently connected to the db? */

    unsigned int  numCols;        /* Total number of columns in statement/result. */
    unsigned int  numRows;        /* Total number of rows to return in result. */

    char          cmd[64];        /* Buffer for test commands. */
    char          columnBuf[32];  /* Scratch buffer for column names. */
    Ns_DString    ds;             /* Scratch buffer for first result value. */
    char         *rest;           /* The tail of the query. */

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
static Dbi_NextValueProc    NextValue;
static Dbi_ColumnNameProc   ColumnName;
static Dbi_TransactionProc  Transaction;
static Dbi_FlushProc        Flush;
static Dbi_ResetProc        Reset;


/*
 * Local variables defined in this file.
 */

static CONST Dbi_DriverProc procs[] = {
    {Dbi_OpenProcId,         Open},
    {Dbi_CloseProcId,        Close},
    {Dbi_ConnectedProcId,    Connected},
    {Dbi_BindVarProcId,      Bind},
    {Dbi_PrepareProcId,      Prepare},
    {Dbi_PrepareCloseProcId, PrepareClose},
    {Dbi_ExecProcId,         Exec},
    {Dbi_NextValueProcId,    NextValue},
    {Dbi_ColumnNameProcId,   ColumnName},
    {Dbi_TransactionProcId,  Transaction},
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

    Dbi_LibInit();
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
    Connection *conn;

    assert(handle);
    assert(handle->driverData == NULL);

    assert(STREQ((char *) configData, "driver config data"));


    conn = ns_calloc(1, sizeof(Connection));
    Ns_DStringInit(&conn->ds);
    conn->connected = NS_TRUE;
    conn->configData = configData;

    handle->driverData = conn;

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

    assert(handle);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));

    assert(conn->connected = NS_TRUE);
    assert(Ns_DStringLength(&conn->ds) == 0);
    assert(conn->rest == NULL);

    assert(conn->cmd[0] == '\0');

    assert(conn->numCols == 0);
    assert(conn->numRows == 0);

    Ns_DStringFree(&conn->ds);
    ns_free(conn);
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

    assert(handle);

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
    assert(ds);
    assert(name);
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
Prepare(Dbi_Handle *handle, Dbi_Statement *stmt,
        unsigned int *numVarsPtr, unsigned int *numColsPtr)
{
    Connection *conn = handle->driverData;
    int         n, numCols, numRows, rest = 0;

    assert(handle);
    assert(stmt);
    assert(numVarsPtr);
    assert(numColsPtr);

    assert((stmt->nqueries <= 1 && !stmt->driverData)
           || stmt->nqueries > 1);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));

    assert(conn->numCols == 0);
    assert(conn->numRows == 0);

    /*
     * Scan the qeuery for our test specific syntax.
     *
     * We're looking for a command, the number of columns to
     * return, the number of rows to return, and the tail which is
     * used for testing bind variable syntax.
     */

    n = sscanf(stmt->sql, "%s %d %d %n",
               conn->cmd, &numCols, &numRows, &rest);

    if (n < 1 || (STREQ(conn->cmd, "DML") && conn->numCols > 0)) {
        Dbi_SetException(handle, "TEST", "nsdbitest: test query syntax error");
        return NS_ERROR;
    }

    /*
     * Simulate a failed prepare opperation.
     */

    if (STREQ(conn->cmd, "PREPERR")) {
        Dbi_SetException(handle, "TEST", "test: prepare failure");
        return NS_ERROR;
    }
    conn->numCols = numCols;
    conn->numRows = numRows;

    if (rest) {
        conn->rest = ns_strdup(stmt->sql + rest);
    }

    /*
     * Simulate preparing on the second run.
     * (no need to create a prepared statement for single-use queries).
     */

    if (stmt->nqueries > 0) {
        stmt->driverData = (void *) NS_TRUE;
    }

    *numColsPtr = conn->numCols;
    *numVarsPtr = Dbi_NumVariables(handle);

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

    assert(handle);
    assert(stmt);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));

    assert(stmt->driverData == (void *) NS_TRUE);


    stmt->driverData = NULL;
}


/*
 *----------------------------------------------------------------------
 *
 * Exec --
 *
 *      Execute a query.
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
Exec(Dbi_Handle *handle, Dbi_Statement *stmt,
     Dbi_Value *values, unsigned int numValues)
{
    Connection *conn = handle->driverData;
    int         i, j;

    assert(stmt);

    assert(numValues <= DBI_MAX_BIND);
    assert(numValues == 0 || (numValues > 0 && values != NULL));

     assert((stmt->nqueries == 0 && !stmt->driverData)
           || stmt->nqueries > 0);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));
    assert(conn->connected == NS_TRUE);


    /*
     * Execute the test commands.
     */

    if (STREQ(conn->cmd, "DML")
        || STREQ(conn->cmd, "ROWS")) {

        /*
         * Record bound values, which we report as the first column
         * of the first row during fetch.
         *
         * For binary values we record the length in bytes.
         */

        for (i = 0; i < numValues; i++) {

            /* check nulls and lengths */
            assert((values[i].length && values[i].data)
                   || (!values[i].length && !values[i].data));

            if (values[i].binary) {
                Ns_DStringPrintf(&conn->ds, " %u", values[i].length);
            } else {
                Tcl_DStringAppendElement(&conn->ds, values[i].data);
            }
        }
        return NS_OK;

    } else if (STREQ(conn->cmd, "BINARY")) {

        /*
         * Ensure all bind variables are marked binary and contain
         * nothing but 0 bytes.
         */

        for (i = 0; i < numValues; i++) {
            if (!values[i].binary) {
                Ns_Fatal("BINARY: values[%d].binary not 1", i);
            }
            for (j = 0; j < values[i].length; j++) {
                if (values[i].data[j] != '\0') {
                    Ns_Fatal("BINARY: values[%d].data[%d] not '\\0'", i, j);
                }
            }
        }
        return NS_OK;

    } else if (STREQ(conn->cmd, "SLEEP")) {

        /* For testing handle timeouts. */

        sleep(conn->numCols);
        return NS_OK;
    
    } else if (STREQ(conn->cmd, "EXECERR")) {

        /* A simulated execution error. */

        Dbi_SetException(handle, "TEST", "driver error");
        return NS_ERROR;

    } else if (STREQ(conn->cmd, "PREPERR")) {

        /* This should not have been propagated after preparation. */

        Dbi_SetException(handle, "TEST", "nsdbitest: PREPERR caught in Exec.");
        return NS_ERROR;

    } else if (STREQ(conn->cmd, "NEXTERR")) {

        /* Pass through to NextValue . */

        return NS_OK;

    }

    /* Faulty test suite... */

    Dbi_SetException(handle, "TEST", "nsdbitest: test query syntax error");

    return NS_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * NextValue --
 *
 *      Fetch the next value from the pending result set of the handle.
 *
 *      For testing, all values are "v", except the first which is
 *      the original SQL statement with driver specific bind
 *      variable notation substituted.
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
NextValue(Dbi_Handle *handle, Dbi_Statement *stmt, Dbi_Value *value, int *endPtr)
{
    Connection        *conn = handle->driverData;
    static CONST char  binary[8];

    assert(stmt);
    assert(value);
    assert(endPtr);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));
    assert(conn->connected == NS_TRUE);


    /*
     * Simulate a NextValue and connection failure.
     */

    if (STREQ(conn->cmd, "NEXTERR")) {
        conn->connected = NS_FALSE;
        return NS_ERROR;
    }

    /*
     * Check to see if we've already returned the last value.
     */

    if (value->rowIdx >= conn->numRows) {
        *endPtr = 1;
        return NS_OK;
    }

    /*
     * Return the original SQL statement as the first value.
     * This allows us to check that bind variables were substituted
     * correctly.
     */

    if (value->colIdx == 0 && value->rowIdx == 0) {
        if (conn->rest) {
            Tcl_DStringAppendElement(&conn->ds, conn->rest);
        }
        value->data   = Ns_DStringValue(&conn->ds);
        value->length = Ns_DStringLength(&conn->ds);
        value->binary = 0;
    } else {
        if (STREQ(conn->cmd, "BINARY")) {
            value->data   = binary;
            value->length = sizeof(binary);
            value->binary = 1;
        } else {
            value->data   = "v";
            value->length = 1;
            value->binary = 0;
        }
    }
    *endPtr = 0;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ColumnName --
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
ColumnName(Dbi_Handle *handle, Dbi_Statement *stmt,
           unsigned int index, CONST char **columnPtr)
{
    Connection *conn = handle->driverData;

    assert(stmt);
    assert(columnPtr);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));
    assert(conn->connected == NS_TRUE);
    /* assert(Ns_DStringLength(&conn->ds) > 0); */


    sprintf(conn->columnBuf, "%u", index);
    *columnPtr = conn->columnBuf;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Transaction --
 *
 *      Simulate transaction begin, commit, rollback and savepoints.
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
Transaction(Dbi_Handle *handle, unsigned int depth,
            Dbi_TransactionCmd cmd, Dbi_Isolation isolation)
{
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
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Flush(Dbi_Handle *handle, Dbi_Statement *stmt)
{
    Connection *conn = handle->driverData;

    assert(stmt);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));


    Ns_DStringTrunc(&conn->ds, 0);
    conn->numCols = conn->numRows = 0;

    return NS_OK;
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

    if (conn->rest) {
        ns_free(conn->rest);
        conn->rest = NULL;
    }
    conn->cmd[0] = '\0';

    return NS_OK;
}
