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

static void ParseBindVars(Statement *stmtPtr, CONST char *sql, int len);
static void DefineBindVar(Statement *stmtPtr, CONST char *name);



/*
 *----------------------------------------------------------------------
 *
 * Dbi_StatementAlloc --
 *
 *      Initialise a Dbi_Statement structure with the given SQL.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Statement is parsed by driver and any bind variables
 *      are replaced.
 *
 *----------------------------------------------------------------------
 */

Dbi_Statement *
Dbi_StatementAlloc(Dbi_Pool *pool, CONST char *sql, int len)
{
    Statement *stmtPtr;

    stmtPtr = ns_calloc(1, sizeof(Statement));
    stmtPtr->poolPtr = (Pool *) pool;
    Ns_DStringInit(&stmtPtr->dsSql);
    Tcl_InitHashTable(&stmtPtr->bindVars, TCL_STRING_KEYS);
    ParseBindVars(stmtPtr, sql, len);

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
    for_each_hash_entry(hPtr, &stmtPtr->bindVars, &search) {
        valuePtr = Tcl_GetHashValue(hPtr);
        ns_free(valuePtr);
    }
    Tcl_DeleteHashTable(&stmtPtr->bindVars);
    Ns_DStringFree(&stmtPtr->dsSql);
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
 * ParseBindVars --
 *
 *      Parse the given SQL string for bind variables of the form :name
 *      and call the driver for a replacement string.  Store the
 *      identified bind 
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
ParseBindVars(Statement *stmtPtr, CONST char *sql, int len)
{
    Dbi_Driver *driver = stmtPtr->poolPtr->driver;
    Ns_DString  ds;
    char       *p, *chunk, *bind, save;
    int         quote = 0;

#define preveq(c) (p != ds.string && *(p-1) == (c))
#define nexteq(c) (*(p+1) == (c))

    if (driver->bindVarProc == NULL) {
        Ns_DStringNAppend(&stmtPtr->dsSql, sql, len);
        return;
    }
    Ns_DStringInit(&ds);
    Ns_DStringNAppend(&ds, sql, len);
    for (p = chunk = ds.string, bind = NULL; len > 0; ++p, --len) {
        if (*p == ':' && !quote && !nexteq(':') && !preveq(':') && !preveq('\\')) {
            bind = p;
        } else if (*p == '\'') {
            if (p == ds.string || !preveq('\\')) {
                quote = !quote;
            }
        } else if (bind != NULL) {
            if (!(isalnum((int)*p) || *p == '_') && p > bind) {
                /* append everything up to the beginning of the bind variable */
                save = *bind, *bind = '\0';
                Ns_DStringNAppend(&stmtPtr->dsSql, chunk, bind - chunk);
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
    Ns_DStringNAppend(&stmtPtr->dsSql, chunk, bind ? bind - chunk : p - chunk);
    /* check for trailing bindvar */
    if (bind != NULL && p > bind) {
        DefineBindVar(stmtPtr, ++bind);
    }
    Ns_DStringFree(&ds);
}

static void
DefineBindVar(Statement *stmtPtr, CONST char *name)
{
    Dbi_Driver    *driver = stmtPtr->poolPtr->driver;
    Tcl_HashEntry *hPtr;
    int new;

    hPtr = Tcl_CreateHashEntry(&stmtPtr->bindVars, name, &new);
    Tcl_SetHashValue(hPtr, NULL);
    (*driver->bindVarProc)(&stmtPtr->dsSql, stmtPtr->bindVars.numEntries);
}
