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
 * Dbi_QuoteValue --
 *
 *      Add single quotes around an SQL string value if necessary.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Copy of the string, modified if needed, is placed in the 
 *      given Ns_DString.
 *
 *----------------------------------------------------------------------
 */

void
Dbi_QuoteValue(Ns_DString *pds, char *string)
{
    while (*string != '\0') {
        if (*string == '\'') {
            Ns_DStringNAppend(pds, "'", 1);
        }
        Ns_DStringNAppend(pds, string, 1);
        ++string;
    }
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_0or1Row --
 *
 *      Send an SQL statement which should return either no rows or
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
Dbi_0or1Row(Dbi_Handle *handle, char *sql, int *nrows)
{
    if (Dbi_Select(handle, sql, nrows) != NS_OK) {
        return NS_ERROR;
    }
    if (handle->numRows > 1) {
        Dbi_SetException(handle, DBI_SQLERRORCODE,
            "Query returned more than one row.");
        Dbi_Flush(handle);
        return NS_ERROR;
    }
    *nrows = handle->numRows;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_1Row --
 *
 *      Send a SQL statement which is expected to return exactly 1 row.
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
Dbi_1Row(Dbi_Handle *handle, char *sql)
{
    int nrows;

    if (Dbi_0or1Row(handle, sql, &nrows) != NS_OK) {
        return NS_ERROR;
    }
    if (nrows != 1) {
        Dbi_SetException(handle, DBI_SQLERRORCODE,
            "Query did not return a row.");
        return NS_ERROR;
    }

    return NS_OK;
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
Dbi_SetException(Dbi_Handle *handle, char *sqlstate, char *fmt, ...)
{
    Ns_DString *ds = &handle->dsExceptionMsg;
    va_list ap;
    int len;

    if (sqlstate != NULL) {
        strncpy(handle->cExceptionCode, sqlstate, 6);
        handle->cExceptionCode[5] = '\0';
    }
    if (fmt != NULL) {
        Ns_DStringFree(ds);
        va_start(ap, fmt);
        Ns_DStringVPrintf(ds, fmt, ap);
        va_end(ap);
        len = Ns_DStringLength(ds);
        if (ds->string[len - 1] == '\n') {
            Ns_DStringTrunc(ds, len - 1);
        }
    }
}
