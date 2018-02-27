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

NS_EXPORT int Ns_ModuleVersion = 1;


/*
 * The following structure manages a per-handle connection to our
 * imaginary dtabase, and a single result.
 */

typedef struct Connection {

    const char   *configData;     /* Pointer to per-pool config data. */
    int           connected;      /* Is the handle currently connected to the db? */

    unsigned int  numCols;        /* Total number of columns in statement/result. */
    unsigned int  numRows;        /* Total number of rows to return in result. */

    int           exec;           /* Exec was called. */
    int           nextrow;        /* NextRow was called. */

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
static Dbi_PrepareProc      Prepare;
static Dbi_PrepareCloseProc PrepareClose;
static Dbi_BindVarProc      Bind;
static Dbi_ExecProc         Exec;
static Dbi_NextRowProc      NextRow;
static Dbi_ColumnLengthProc ColumnLength;
static Dbi_ColumnValueProc  ColumnValue;
static Dbi_ColumnNameProc   ColumnName;
static Dbi_TransactionProc  Transaction;
static Dbi_FlushProc        Flush;
static Dbi_ResetProc        Reset;


/*
 * Local variables defined in this file.
 */

static const Dbi_DriverProc procs[] = {
  {Dbi_OpenProcId,           (Ns_Callback *)Open},
    {Dbi_CloseProcId,        (Ns_Callback *)Close},
    {Dbi_ConnectedProcId,    (Ns_Callback *)Connected},
    {Dbi_BindVarProcId,      (Ns_Callback *)Bind},
    {Dbi_PrepareProcId,      (Ns_Callback *)Prepare},
    {Dbi_PrepareCloseProcId, (Ns_Callback *)PrepareClose},
    {Dbi_ExecProcId,         (Ns_Callback *)Exec},
    {Dbi_NextRowProcId,      (Ns_Callback *)NextRow},
    {Dbi_ColumnLengthProcId, (Ns_Callback *)ColumnLength},
    {Dbi_ColumnValueProcId,  (Ns_Callback *)ColumnValue},
    {Dbi_ColumnNameProcId,   (Ns_Callback *)ColumnName},
    {Dbi_TransactionProcId,  (Ns_Callback *)Transaction},
    {Dbi_FlushProcId,        (Ns_Callback *)Flush},
    {Dbi_ResetProcId,        (Ns_Callback *)Reset},
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
Ns_ModuleInit(const char *server, const char *module)
{
    const char *name      = "test";
    const char *database  = "db";
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
 *      NS_OK or NS_ERROR.
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

    if (STREQ(Dbi_PoolName(handle->pool), "OPENERR")
            || STREQ(Dbi_PoolName(handle->pool), "OPENERR0")) {
        Dbi_SetException(handle, "00000", "simulate failed open");
        return NS_ERROR;
    }

    if (handle->driverData == NULL) {
        conn = ns_calloc(1, sizeof(Connection));
        Ns_DStringInit(&conn->ds);
        conn->connected = NS_TRUE;
        conn->configData = configData;

        handle->driverData = conn;
    } else {
        conn = handle->driverData;
        conn->connected = NS_TRUE;
    }

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

    assert(conn->connected == NS_TRUE);
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
Bind(Ns_DString *ds, const char *name, int bindIdx)
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

    n = sscanf(stmt->sql, "%64s %d %d %n",
               conn->cmd, &numCols, &numRows, &rest);

    if (n < 1 || (STREQ(conn->cmd, "DML") && conn->numCols > 0)) {
        Dbi_SetException(handle, "TEST", "nsdbitest: test query syntax error");
        return NS_ERROR;
    }

    /*
     * Simulate a failed prepare operation.
     */

    if (STREQ(conn->cmd, "PREPERR")) {
        Dbi_SetException(handle, "TEST", "test: prepare failure");
        return NS_ERROR;
    }
    conn->numCols = numCols;
    conn->numRows = numRows;

    if (rest && rest < stmt->length) {
        conn->rest = ns_strdup(stmt->sql + rest);
    }

    /*
     * Simulate preparing on the second run.
     * (no need to create a prepared statement for single-use queries).
     */

    if (stmt->nqueries > 0) {
        stmt->driverData = (void *) NS_TRUE;
    }

    /*
     * Only need to report the number of bind variables we found
     * when first preparing a statement.
     */

    if (stmt->nqueries == 0) {
        *numVarsPtr = Dbi_NumVariables(handle);
    }

    *numColsPtr = conn->numCols;

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
#ifndef NDEBUG
    Connection *conn = handle->driverData;
#endif
    assert(handle);
    assert(stmt);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));

    assert((stmt->nqueries == 0 && !stmt->driverData)
           || (stmt->nqueries > 0 && stmt->driverData));

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
    int         i;

    assert(stmt);

    assert(numValues <= DBI_MAX_BIND);
    assert(numValues == 0 || (numValues > 0 && values != NULL));

    assert((stmt->nqueries == 0 && !stmt->driverData)
           || (stmt->nqueries > 0 && stmt->driverData));

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
                Ns_DStringPrintf(&conn->ds, " %" PRIdz, values[i].length);
            } else {
	        Tcl_DStringAppendElement(&conn->ds, values[i].length > 0 ? values[i].data : "");
            }
        }
        if (conn->rest) {
            Tcl_DStringAppendElement(&conn->ds, conn->rest);
        }

        conn->exec = 1;

        return NS_OK;

    } else if (STREQ(conn->cmd, "BINARY")) {

        /*
         * Ensure all bind variables are marked binary and contain
         * nothing but 0 bytes.
         */

        for (i = 0; i < numValues; i++) {
	    int j;

            if (!values[i].binary) {
                Ns_Fatal("BINARY: values[%d].binary not 1", i);
            }
            for (j = 0; j < values[i].length; j++) {
                if (values[i].data[j] != '\0') {
                    Ns_Fatal("BINARY: values[%d].data[%d] not '\\0'", i, j);
                }
            }
        }
        conn->exec = 1;

        return NS_OK;

    } else if (STREQ(conn->cmd, "SLEEP")) {

        /* For testing handle timeouts. */

        sleep(conn->numCols);
        conn->exec = 1;
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

        /* Pass through to NextRow . */

        conn->exec = 1;
        return NS_OK;

    }

    /* Faulty test suite... */

    Dbi_SetException(handle, "TEST", "nsdbitest: test query syntax error");

    return NS_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * NextRow --
 *
 *      Fetch the next row from the pending result set of the handle.
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
NextRow(Dbi_Handle *handle, Dbi_Statement *stmt, int *endPtr)
{
    Connection *conn = handle->driverData;

    assert(stmt);
    assert(endPtr);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));
    assert(conn->connected == NS_TRUE);

    assert(conn->exec == 1);

    /*
     * Simulate a NextRow and connection failure.
     */

    if (STREQ(conn->cmd, "NEXTERR")) {
        conn->connected = NS_FALSE;
        return NS_ERROR;
    }

    conn->nextrow = 1;

    /*
     * Check to see if we've already returned the last row.
     */

    assert(handle->rowIdx <= conn->numRows);

    if (handle->rowIdx == conn->numRows) {
        *endPtr = 1;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ColumnLength --
 *
 *      Return the length of the column value in bytes, and whether or
 *      not it is binary or text.
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
ColumnLength(Dbi_Handle *handle, Dbi_Statement *stmt, unsigned int index,
             size_t *lengthPtr, int *binaryPtr)
{
    Connection *conn = handle->driverData;

    assert(stmt);
    assert(lengthPtr);
    assert(binaryPtr);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));
    assert(conn->connected == NS_TRUE);

    assert(conn->exec == 1);
    assert(conn->nextrow == 1);

    assert(index < conn->numCols);

    if (handle->rowIdx == 0 && index == 0
            && conn->rest) {

        *lengthPtr = Ns_DStringLength(&conn->ds);
        *binaryPtr = 0;

    } else if (STREQ(conn->cmd, "BINARY")) {

        *lengthPtr = 8;
        *binaryPtr = 1;

    } else {
        Ns_DStringSetLength(&conn->ds, 0);
        Ns_DStringPrintf(&conn->ds, "%u.%u",
                         handle->rowIdx, index);
        *lengthPtr = Ns_DStringLength(&conn->ds);
        *binaryPtr = 0;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ColumnValue --
 *
 *      Fetch the indicated value from the current row.
 *
 *      For testing, values are the column and row index, except the
 *      first which is the original SQL statement with driver specific
 *      bind variable notation substituted.
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
ColumnValue(Dbi_Handle *handle, Dbi_Statement *stmt, unsigned int index,
            char *value, size_t length)
{
    Connection        *conn = handle->driverData;
    static const char  binaryValue[8];

    assert(stmt);
    assert(value);

    assert(conn);
    assert(STREQ(conn->configData, "driver config data"));
    assert(conn->connected == NS_TRUE);

    assert(conn->exec == 1);
    assert(conn->nextrow == 1);

    assert(index < conn->numCols);

    /*
     * Return the original SQL statement as the first value.
     * This allows us to check that bind variables were substituted
     * correctly.
     */

    if (handle->rowIdx == 0 && index == 0
            && conn->rest) {

        assert(length <= Ns_DStringLength(&conn->ds));
        memcpy(value, Ns_DStringValue(&conn->ds), length);

    } else if (STREQ(conn->cmd, "BINARY")) {

        assert(length <= sizeof(binaryValue));
        memcpy(value, binaryValue, length);

    } else {
        Ns_DStringSetLength(&conn->ds, 0);
        Ns_DStringPrintf(&conn->ds, "%u.%u",
                         handle->rowIdx, index);

        assert(length <= Ns_DStringLength(&conn->ds));
        memcpy(value, Ns_DStringValue(&conn->ds), length);
    }

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
           unsigned int index, const char **columnPtr)
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


    Ns_DStringSetLength(&conn->ds, 0);
    conn->exec = 0;
    conn->numCols = conn->numRows = 0;
    conn->exec = conn->nextrow = 0;

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
