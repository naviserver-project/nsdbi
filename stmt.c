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
 * The following structure defines an SQL statement.
 */

typedef struct Statement {

    /* Publicly visible in a Dbi_Statement. */

    char               *sql;        /* Pointer to bound SQL. */
    int                 length;     /* Length of bound SQL. */

    /* Private to a Statement. */

    Ns_DString          dsSql;      /* Original SQL statement. */
    Ns_DString          dsBoundSql; /* SQL with bind variable notation. */

    Dbi_Pool           *pool;       /* Pool that statement is prepared for. */

    struct {
        Tcl_HashTable   table;      /* Bind variables by name. */
        int             nbound;     /* Number of bind variables. */
        struct {
            CONST char *name;       /* (Hash table key) */
        } vars[DBI_MAX_BIND];       /* Bind variables by order. */

    } bind;

} Statement;


/*
 * Static functions defined in this file
 */

static int ParseBindVars(Statement *stmtPtr)
    NS_GNUC_NONNULL(1);

static int DefineBindVar(Statement *stmtPtr, CONST char *name)
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
 *      None.
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
    Tcl_InitHashTable(&stmtPtr->bind.table, TCL_STRING_KEYS);
    Ns_DStringNAppend(&stmtPtr->dsSql, sql, len);

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
    Statement *stmtPtr = (Statement *) stmt;

    Tcl_DeleteHashTable(&stmtPtr->bind.table);
    Ns_DStringFree(&stmtPtr->dsSql);
    Ns_DStringFree(&stmtPtr->dsBoundSql);
    ns_free(stmtPtr);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_StatementPrepare --
 *
 *      Prepare a statement for execution with the given handle.
 *
 * Results:
 *      NS_ERROR if max bind variables exceeded.
 *
 * Side effects:
 *      Statement is parsed by driver callback and any bind variables
 *      are converted to driver specific notation. Statement may
 *      be reparsed if previously prepared for a handle from
 *      a different pool.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_StatementPrepare(Dbi_Handle *handle, Dbi_Statement *stmt)
{
    Statement *stmtPtr = (Statement *) stmt;
    int        status  = NS_OK;

    if (stmtPtr->pool == NULL
        || stmtPtr->pool != handle->pool) {

        stmtPtr->pool = handle->pool;
        status = ParseBindVars(stmtPtr);

        if (status != NS_OK) {
            Dbi_SetException(handle, "DBI",
                             "max bind variables exceeded: %d",
                             DBI_MAX_BIND);
            stmtPtr->pool = NULL;
            Ns_DStringSetLength(&stmtPtr->dsBoundSql, 0);
            stmtPtr->sql = NULL;
            stmtPtr->length = 0;
        } else {
            stmtPtr->sql    = Ns_DStringValue(&stmtPtr->dsBoundSql);
            stmtPtr->length = Ns_DStringLength(&stmtPtr->dsBoundSql);
        }
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_StatementSQL --
 *
 *      Return the original SQL statement, pre bind variable
 *      substitution.
 *
 * Results:
 *      Pointer to cstring, length updated if not NULL.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

CONST char *
Dbi_StatementSQL(Dbi_Statement *stmt, int *length)
{
    Statement *stmtPtr = (Statement *) stmt;

    if (length != NULL) {
        *length = Ns_DStringLength(&stmtPtr->dsSql);
    }
    return (CONST char *) Ns_DStringValue(&stmtPtr->dsSql);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetBindVariable --
 *
 *      Get the name of the bind variable at the given index.
 *
 * Results:
 *      NS_OK if exists, NS_ERROR otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_GetBindVariable(Dbi_Statement *stmt, int idx, CONST char **namePtr)
{
    Statement *stmtPtr = (Statement *) stmt;

    if (idx < 0 || idx >= stmtPtr->bind.nbound) {
        return NS_ERROR;
    }
    *namePtr = stmtPtr->bind.vars[idx].name;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SetBindValue --
 *
 *      Set the value of the bind variable at the given index.
 *
 * Results:
 *      NS_OK if set, NS_ERROR if index out of bounds.
 *
 * Side effects:
 *      Old value, if any, is over written.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_SetBindValue(Dbi_Bind *bind, int idx, CONST char *value, int length)
{
    if (idx < 0 || idx >= DBI_MAX_BIND) {
        Ns_Log(Bug, "dbi: Dbi_SetBindValue: bad index: %d, nbound: %d",
               idx, bind->nbound);
        return NS_ERROR;
    }
    bind->vals[idx].value  = value;
    bind->vals[idx].length = length;
    bind->nbound++;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetBindValue --
 *
 *      Get the value of the bind variable at the given index.
 *
 * Results:
 *      NS_OK if exists, NS_ERROR otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_GetBindValue(Dbi_Bind *bind, int idx,
                 CONST char **valuePtr, int *lengthPtr)
{
    if (idx < 0 || idx >= bind->nbound) {
        return NS_ERROR;
    }
    if (valuePtr != NULL) {
        *valuePtr = bind->vals[idx].value;
    }
    if (lengthPtr != NULL) {
        *lengthPtr = bind->vals[idx].length;
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
 *      identified bind variables as a hash table of keys.
 *
 * Results:
 *      NS_ERROR if max bind variables exceeded.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
ParseBindVars(Statement *stmtPtr)
{
    char  *sql, *p, *chunk, *bind, save;
    int    len, quote;

#define preveq(c) (p != sql && *(p-1) == (c))
#define nexteq(c) (*(p+1) == (c))

    Ns_DStringTrunc(&stmtPtr->dsBoundSql, 0);
    stmtPtr->bind.nbound = 0;
    sql = Ns_DStringValue(&stmtPtr->dsSql);
    len = Ns_DStringLength(&stmtPtr->dsSql);
    quote = 0;
    for (p = chunk = sql, bind = NULL; len > 0; ++p, --len) {
        if (*p == ':' && !quote && !nexteq(':') && !preveq(':') && !preveq('\\')) {
            bind = p;
        } else if (*p == '\'' && bind == NULL) {
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
                if (DefineBindVar(stmtPtr, ++bind) != NS_OK) {
                    *p = save;
                    return NS_ERROR;
                }
                *p = save;
                bind = NULL;
            }
        }
    }
    /* append remaining chunk */
    Ns_DStringNAppend(&stmtPtr->dsBoundSql, chunk, bind ? bind - chunk : p - chunk);
    /* check for trailing bindvar */
    if (bind != NULL && p > bind) {
        if (DefineBindVar(stmtPtr, ++bind) != NS_OK) {
            return NS_ERROR;
        }
    }

    return NS_OK;
}

static int
DefineBindVar(Statement *stmtPtr, CONST char *name)
{
    Dbi_Driver    *driver = DbiPoolDriver(stmtPtr->pool);
    Tcl_HashEntry *hPtr;
    int            new, index;

    index = stmtPtr->bind.nbound;
    if (index >= DBI_MAX_BIND) {
        return NS_ERROR;
    }
    hPtr = Tcl_CreateHashEntry(&stmtPtr->bind.table, name, &new);
    if (new) {
        stmtPtr->bind.nbound++;
        Tcl_SetHashValue(hPtr, (void *) index);
        stmtPtr->bind.vars[index].name = Tcl_GetHashKey(&stmtPtr->bind.table, hPtr);
    } else {
        index = (int) Tcl_GetHashValue(hPtr);
    }
    (*driver->bindVarProc)(&stmtPtr->dsBoundSql, name, index, driver->arg);

    return NS_OK;
}
