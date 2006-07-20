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
 * util.c --
 *
 *      Utility db routines.
 */

#include "dbi.h"

NS_RCSID("@(#) $Header$");


/*
 * The following constants are defined for this file.
 */

#define DBI_SQLERRORCODE "DBI" /* SQL error code for exceptions. */



/*
 *----------------------------------------------------------------------
 *
 * Dbi_Select --
 *
 *      Execute a query which is expected to return rows.
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
Dbi_Select(Dbi_Query *query)
{
    if (Dbi_Exec(query) != DBI_ROWS) {
        if (Dbi_ExceptionPending(query->handle) == NS_FALSE) {
            Dbi_SetException(query->handle, "DBI",
                "Query was not a statement returning rows.");
        }
        return NS_ERROR;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_0or1Row --
 *
 *      Execute a query which should return either no rows or
 *      exactly one row.
 *
 * Results:
 *      NS_OK/NS_ERROR.
 *
 * Side effects:
 *      Given nrows pointer is set to 0 or 1 to indicate if a row
 *      was actually returned.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_0or1Row(Dbi_Query *query)
{
    if (Dbi_Select(query) != NS_OK) {
        return NS_ERROR;
    }
    if (query->result.numRows > 1) {
        Dbi_SetException(query->handle, DBI_SQLERRORCODE,
            "Query returned more than one row.");
        Dbi_Flush(query);
        return NS_ERROR;
    }
 
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_1Row --
 *
 *      Execute a query which is expected to return exactly 1 row.
 *
 * Results:
 *      NS_OK/NS_ERROR
 *
 * Side effects:
 *      An exception may be set if zero rows returned or other error.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_1Row(Dbi_Query *query)
{
    if (Dbi_0or1Row(query) != NS_OK) {
        return NS_ERROR;
    }
    if (query->result.numRows == 0) {
        Dbi_SetException(query->handle, DBI_SQLERRORCODE,
            "Query was not a statement returning rows.");
        return NS_ERROR;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DML --
 *
 *      Execute a query which is expected to be DML.
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
Dbi_DML(Dbi_Query *query)
{
    int status;

    status = Dbi_Exec(query);
    if (status == DBI_DML) {
        status = NS_OK;
    } else {
        if (status == DBI_ROWS) {
            Dbi_SetException(query->handle, "DBI",
                "Query was not a DML or DDL command.");
            Dbi_Flush(query);
        }
        status = NS_ERROR;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SetException --
 *
 *      Set the stored SQL exception state and message for the
 *      given handle.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Status code and message may be updated.
*
 *----------------------------------------------------------------------
 */

void
Dbi_SetException(Dbi_Handle *handle, CONST char *sqlstate, CONST char *fmt, ...)
{
    Handle      *handlePtr = (Handle *) handle;
    Ns_DString  *ds = &handlePtr->dsExceptionMsg;
    va_list      ap;
    int          len;

    if (sqlstate != NULL) {
        strncpy(handlePtr->cExceptionCode, sqlstate, 6);
        handlePtr->cExceptionCode[5] = '\0';
    }
    if (fmt != NULL) {
        Ns_DStringTrunc(ds, 0);
        va_start(ap, fmt);
        Ns_DStringVPrintf(ds, (char *) fmt, ap);
        va_end(ap);
        len = Ns_DStringLength(ds);
        while (ds->string[len - 1] == '\n') {
            Ns_DStringTrunc(ds, len - 1);
        }
    }
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ResetException --
 *
 *      Clear any stored SQL exception state and message for the
 *      given handle.
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
Dbi_ResetException(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    handlePtr->cExceptionCode[0] = '\0';
    Ns_DStringTrunc(&handlePtr->dsExceptionMsg, 0);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ExceptionCode --
 *
 *      The current 5 character exception code for the given handle.
 *
 * Results:
 *      cstring.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_ExceptionCode(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    return handlePtr->cExceptionCode;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ExceptionMsg --
 *
 *      The current exception message for the given handle.
 *
 * Results:
 *      cstring.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_ExceptionMsg(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    return Ns_DStringValue(&handlePtr->dsExceptionMsg);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ExceptionPending --
 *
 *      Is an exception currently set on the given handle?
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
Dbi_ExceptionPending(Dbi_Handle *handle)
{
    Handle *handlePtr = (Handle *) handle;

    if (handlePtr->cExceptionCode[0] != '\0'
        || Ns_DStringLength(&handlePtr->dsExceptionMsg)) {
        return NS_TRUE;
    }
    return NS_FALSE;
}
