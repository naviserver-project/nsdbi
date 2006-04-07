/*
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://mozilla.org/.
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
 * stmt.c --
 *
 *      Routines for managing statements.
 */

#include "dbi.h"

NS_RCSID("@(#) $Header$");



/*
 * Static functions defined in this file
 */

static void ParseBindVars(Statement *stmtPtr)
    NS_GNUC_NONNULL(1);

static void DefineBindVar(Statement *stmtPtr, CONST char *name)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);



/*
 *----------------------------------------------------------------------
 *
 * Dbi_StatementAlloc --
 *
 *      Create a Dbi_Statement structure from the given SQL string.
 *
 * Results:
 *      Pointer to new Dbi_Statement.
 *
 * Side effects:
 *      Statement is parsed by driver and any bind variables
 *      are converted to driver specific notation.
 *
 *----------------------------------------------------------------------
 */

Dbi_Statement *
Dbi_StatementAlloc(CONST char *sql, int len)
{
    Statement *stmtPtr;

    stmtPtr = ns_calloc(1, sizeof(Statement));
    Ns_DStringInit(&stmtPtr->dsSql);
    Ns_DStringInit(&stmtPtr->dsBoundSql);
    Tcl_InitHashTable(&stmtPtr->bindVars, TCL_STRING_KEYS);
    Ns_DStringNAppend(&stmtPtr->dsSql, sql, len);
    ParseBindVars(stmtPtr);

    return (Dbi_Statement *) stmtPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_StatementFree --
 *
 *      Release memory associated with a Dbi_Statement.
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
Dbi_StatementFree(Dbi_Statement *stmt)
{
    Statement      *stmtPtr = (Statement *) stmt;
    Dbi_BindValue  *valuePtr;
    Tcl_HashSearch  search;
    Tcl_HashEntry  *hPtr;

    Dbi_Flush(stmt);
    hPtr = Tcl_FirstHashEntry(&stmtPtr->bindVars, &search);
    while (hPtr != NULL) {
        valuePtr = Tcl_GetHashValue(hPtr);
        ns_free(valuePtr);
        Tcl_NextHashEntry(&search);
    }
    Tcl_DeleteHashTable(&stmtPtr->bindVars);
    Ns_DStringFree(&stmtPtr->dsSql);
    Ns_DStringFree(&stmtPtr->dsBoundSql);
    ns_free(stmtPtr);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_StatementBindValue --
 *
 *      Set the value of the named bind variable.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_StatementBindValue(Dbi_Statement *stmt, char *name, char *value, int len)
{
    Statement      *stmtPtr = (Statement *) stmt;
    Dbi_BindValue  *valuePtr;
    Tcl_HashEntry  *hPtr;

    hPtr = Tcl_FindHashEntry(&stmtPtr->bindVars, name);
    if (hPtr == NULL) {
        return NS_ERROR;
    }
    valuePtr = ns_malloc(sizeof(Dbi_BindValue));
    valuePtr->data = value;
    valuePtr->len = len;
    Tcl_SetHashValue(hPtr, valuePtr);

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiStatementPrepare --
 *
 *      Prepare a statement for execution with the given handle.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Statement may be reparsed if statement was previously prepared
 *      for a handle from a different pool.
 *
 *----------------------------------------------------------------------
 */

int
DbiStatementPrepare(Dbi_Statement *stmt, Dbi_Handle *handle)
{
    Statement *stmtPtr = (Statement *) stmt;

    if (stmtPtr->poolPtr == NULL
        || stmtPtr->poolPtr != (Pool *) handle->pool) {

        stmtPtr->poolPtr = (Pool *) handle->pool;
        ParseBindVars(stmtPtr);
    }
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ParseBindVars --
 *
 *      Parse the given SQL string for bind variables of the form :name
 *      and call the driver for a replacement string.  Store the
 *      identified bind variables as hash table keys.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
ParseBindVars(Statement *stmtPtr)
{
    char       *sql, *p, *chunk, *bind, save;
    int         len, quote;

    if (stmtPtr->poolPtr == NULL
        || stmtPtr->poolPtr->driver->bindVarProc == NULL) {
        return;
    }

#define preveq(c) (p != sql && *(p-1) == (c))
#define nexteq(c) (*(p+1) == (c))

    Ns_DStringTrunc(&stmtPtr->dsBoundSql, 0);
    sql = Ns_DStringValue(&stmtPtr->dsSql);
    len = Ns_DStringLength(&stmtPtr->dsSql);
    quote = 0;
    for (p = chunk = sql, bind = NULL; len > 0; ++p, --len) {
        if (*p == ':' && !quote && !nexteq(':') && !preveq(':') && !preveq('\\')) {
            bind = p;
        } else if (*p == '\'') {
            if (p == sql || !preveq('\\')) {
                quote = !quote;
            }
        } else if (bind != NULL) {
            if (!(isalnum((int)*p) || *p == '_') && p > bind) {
                /* append everything up to the beginning of the bind variable */
                save = *bind, *bind = '\0';
                Ns_DStringNAppend(&stmtPtr->dsBoundSql, chunk, bind - chunk);
                *bind = save;
                chunk = p;
                /* save the bind variable */
                save = *p, *p = '\0';
                DefineBindVar(stmtPtr, ++bind);
                *p = save;
                bind = NULL;
            }
        }
    }
    /* append remaining chunk */
    Ns_DStringNAppend(&stmtPtr->dsBoundSql, chunk, bind ? bind - chunk : p - chunk);
    /* check for trailing bindvar */
    if (bind != NULL && p > bind) {
        DefineBindVar(stmtPtr, ++bind);
    }
}

static void
DefineBindVar(Statement *stmtPtr, CONST char *name)
{
    Dbi_Driver    *driver;
    Tcl_HashEntry *hPtr;
    int new;

    hPtr = Tcl_CreateHashEntry(&stmtPtr->bindVars, name, &new);
    Tcl_SetHashValue(hPtr, NULL);
    driver = stmtPtr->poolPtr->driver;
    driver->bindVarProc(&stmtPtr->dsBoundSql, stmtPtr->bindVars.numEntries,
                        driver->arg);
}
