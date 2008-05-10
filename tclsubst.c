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
 * Copyright (C) 2008 Stephen Deasey <sdeasey@gmail.com>
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
 * tclsubst.c --
 *
 *      Implements Tcl subst templates.
 */

#include "nsdbi.h"



/*
 * The following describes the internal rep of a Template object
 * which is used to cache the parsed tokens.
 */

typedef struct Template {
    Tcl_Parse    parse;
    int          varColMap[DBI_MAX_BIND];
} Template;


int DbiTclSubstTemplate(Tcl_Interp *, Dbi_Handle *,
                        Tcl_Obj *templateObj, Tcl_Obj *defaultObj);


/*
 * Static functions defined in this file.
 */

static int GetTemplateFromObj(Tcl_Interp *interp, Dbi_Handle *,
                              Tcl_Obj *templateObj, Template **templatePtrPtr);
static int AppendValue(Tcl_Interp *interp, Dbi_Handle *handle, unsigned int index,
                       Tcl_Obj *resObj);
static int AppendTokenVariable(Tcl_Interp *interp, Tcl_Token *tokenPtr,
                               Tcl_Obj *resObj);
static void MapVariablesToColumns(Dbi_Handle *handle, Template *templatePtr);
static void NewTextToken(Tcl_Parse *parsePtr, char *string, int length);
static int NextRow(Tcl_Interp *interp, Dbi_Handle *handle, int *endPtr);

static void FreeTemplate(Template *);
static Tcl_FreeInternalRepProc FreeTemplateObj;


/*
 * Static variables defined in this file.
 */

static Tcl_ObjType templateType = {
    "dbi:template",
    FreeTemplateObj,
    (Tcl_DupInternalRepProc *) NULL,
    (Tcl_UpdateStringProc *) NULL,
    Ns_TclSetFromAnyError
};



/*
 *----------------------------------------------------------------------
 *
 * DbiTclSubstTemplate --
 *
 *      Substitute the template for each row of the pending db result and
 *      append to the Tcl result.
 *
 * Results:
 *      Standard Tcl result.
 *
 * Side effects:
 *      Template object may be converted to dbi:template type / an
 *      existing template object will have it varColMap recomputed.
 *
 *----------------------------------------------------------------------
 */

int
DbiTclSubstTemplate(Tcl_Interp *interp, Dbi_Handle *handle,
                    Tcl_Obj *templateObj, Tcl_Obj *defaultObj)
{
    Template      *templatePtr;
    Tcl_Parse     *parsePtr;
    Tcl_Token     *tokenPtr;
    Tcl_Obj       *resObj;
    int           *varColMap, end;
    unsigned int   tokIdx, varIdx, colIdx, numCols, numRows;

    /*
     * Convert the template into a stream of text + variable tokens.
     * Map the template variables to column indexes and Tcl variable names.
     */

    if (GetTemplateFromObj(interp, handle, templateObj, &templatePtr)
            != TCL_OK) {
        return TCL_ERROR;
    }
    parsePtr = &templatePtr->parse;
    varColMap = templatePtr->varColMap;

    /*
     * Step through the result rows, appending column values and Tcl
     * variables to the final result.
     */

    resObj = Tcl_GetObjResult(interp);
    numCols = Dbi_NumColumns(handle);
    numRows = 0;

    while (NextRow(interp, handle, &end) == TCL_OK
           && !end) {

        numRows++;

        for (tokIdx = 0, varIdx = 0;
             tokIdx < parsePtr->numTokens;
             tokIdx += tokenPtr->numComponents + 1) {

            tokenPtr = &parsePtr->tokenPtr[tokIdx];

            switch (tokenPtr->type) {

            case TCL_TOKEN_TEXT:
                Tcl_AppendToObj(resObj, tokenPtr->start, tokenPtr->size);
                break;

            case TCL_TOKEN_VARIABLE:

                colIdx = varColMap[varIdx++];

                if (colIdx == -1) {
                    if (AppendTokenVariable(interp, tokenPtr, resObj) != TCL_OK) {
                        return TCL_ERROR;
                    }
                } else {
                    if (AppendValue(interp, handle, colIdx, resObj) != TCL_OK) {
                        return TCL_ERROR;
                    }
                }
                break;

            default:
                Ns_Fatal("DbiTclSubstTemplate: unexpected token type: %d",
                         tokenPtr->type);
                break;
            }
        }
    }

    if (numRows == 0) {
        if (defaultObj != NULL) {
            Tcl_SetObjResult(interp, defaultObj);
        } else {
            Tcl_SetResult(interp, "query was not a statement returning rows",
                          TCL_STATIC);
            return TCL_ERROR;
        }
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * AppendValue --
 *
 *      Append the string value of the given result row index to the
 *      Tcl result.
 *
 * Results:
 *      TCL_ERROR if the database complains, TCL_OK otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
AppendValue(Tcl_Interp *interp, Dbi_Handle *handle, unsigned int index,
            Tcl_Obj *resObj)
{
    size_t  valueLength;
    int     resultLength, binary;
    char   *bytes;

    if (Dbi_ColumnLength(handle, index, &valueLength, &binary) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        return TCL_ERROR;
    }
    if (binary) {
        Tcl_SetResult(interp, "can't substitute binary value in template",
                      TCL_STATIC);
        return TCL_ERROR;
    }

    bytes = Tcl_GetStringFromObj(resObj, &resultLength);
    Tcl_SetObjLength(resObj, resultLength + valueLength);

    bytes = resObj->bytes + resultLength;

    if (Dbi_ColumnValue(handle, index, bytes, valueLength) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        return TCL_ERROR;
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * AppendTokenVariable --
 *
 *      Given a Tcl_Token of type TCL_TOKEN_VARIABLE, lookup the
 *      value and append it to the current result.
 *
 * Results:
 *      TCL_ERROR if variable name does not resolve, TCL_OK otherwise.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
AppendTokenVariable(Tcl_Interp *interp, Tcl_Token *tokenPtr, Tcl_Obj *resObj)
{
    Tcl_Obj *objPtr;
    char    *name, save;
    int      size;

    /* NB: Skip past leading '$' */
    name = (char *) tokenPtr->start + 1;
    size = tokenPtr->size - 1;

    save = name[size];
    name[size] = '\0';
    objPtr = Tcl_GetVar2Ex(interp, name, NULL, TCL_LEAVE_ERR_MSG);
    if (objPtr == NULL) {
        Tcl_ResetResult(interp);
        Tcl_AppendResult(interp, "can't read \"", name,
                         "\": no such column or variable", NULL);
        name[size] = save;

        return TCL_ERROR;
    }
    name[size] = save;

    Tcl_AppendObjToObj(resObj, objPtr);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * GetTemplateFromObj --
 *
 *      ???
 *
 * Results:
 *      Standard Tcl result.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
GetTemplateFromObj(Tcl_Interp *interp, Dbi_Handle *handle, Tcl_Obj *templateObj,
                   Template **templatePtrPtr)
{
    Template   *templatePtr;
    Tcl_Parse  *parsePtr;
    Tcl_Token  *tokenPtr;
    char       *string, *p;
    int         length, varIdx, numVarTokens;

    /*
     * Check for cached representation.
     */

    if (templateObj->typePtr == &templateType) {
        templatePtr = templateObj->internalRep.otherValuePtr;
        MapVariablesToColumns(handle, templatePtr);
        *templatePtrPtr = templatePtr;
        return TCL_OK;
    }

    string = p = Tcl_GetStringFromObj(templateObj, &length);

    /*
     * Initialise the parse struct as in Tcl generic/tclParse.c
     */

    templatePtr = ns_malloc(sizeof(Template));
    parsePtr = &templatePtr->parse;
    parsePtr->tokenPtr = parsePtr->staticTokens;
    parsePtr->numTokens = 0;
    parsePtr->tokensAvailable = NUM_STATIC_TOKENS;
    parsePtr->string = string;
    parsePtr->end = (string + length);
    parsePtr->interp = interp;
    parsePtr->errorType = TCL_PARSE_SUCCESS;

    /*
     * Scan the string for dollar substitutions. Variables are substituted
     * and added to the parse struct, everything else is added as text.
     */

    numVarTokens = 0;

    while (length) {

        if (*p == '$') {
            /*
             * First check for a pending run of text.
             */

            if (p != string) {
                NewTextToken(parsePtr, string, p-string);
                string = p;
            }

            /*
             * Check for a valid variable name.
             */

            varIdx = parsePtr->numTokens;
            if (Tcl_ParseVarName(interp, p, length, parsePtr, 1) != TCL_OK) {
                FreeTemplate(templatePtr);
                return TCL_ERROR;
            }

            tokenPtr = &parsePtr->tokenPtr[varIdx];
            if (tokenPtr->type == TCL_TOKEN_TEXT) {
                /*
                 * There isn't a variable name after all: the $ is
                 * just a $.
                 */
                parsePtr->numTokens--;
                p++; length--;
            }

            /*
             * Found a valid variable name.
             * Advance past all it's components.
             */

            numVarTokens++;

            p += tokenPtr->size;
            length -= tokenPtr->size;
            string = p;

        } else {
            p++; length--;
        }
    }

    /*
     * Add any remaining trailing text, and make sure we found at
     * least one variable.
     */

    if (p != string) {
        NewTextToken(parsePtr, string, p-string);
    }

    if (numVarTokens == 0) {
        Tcl_SetResult(interp, "template contains no variables", TCL_STATIC);
        FreeTemplate(templatePtr);
        return TCL_ERROR;
    }

    MapVariablesToColumns(handle, templatePtr);

    Ns_TclSetOtherValuePtr(templateObj, &templateType, templatePtr);
    *templatePtrPtr = templatePtr;

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * MapVariablesToColumns
 *
 *      Map variable tokens (only, skip text tokens) to result
 *      column indexes.
 *
 *      Variables without matching columns are marked '-1' and will
 *      be substituted as Tcl variables when the template is
 *      substitited.
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
MapVariablesToColumns(Dbi_Handle *handle, Template *templatePtr)
{
    Tcl_Parse     *parsePtr = &templatePtr->parse;
    int           *varColMap = templatePtr->varColMap;
    Tcl_Token     *tokenPtr;
    CONST char    *tokenString, *colName;
    int            tokenSize, tokIdx, varIdx, colIdx, numCols;

    numCols = Dbi_NumColumns(handle);

    for (tokIdx = 0, varIdx = 0;
         tokIdx < parsePtr->numTokens;
         tokIdx += tokenPtr->numComponents + 1) {

        tokenPtr = &parsePtr->tokenPtr[tokIdx];
        if (tokenPtr->type != TCL_TOKEN_VARIABLE) {
            continue;
        }

        /* Skip past leading '$' */
        tokenString = tokenPtr->start +1;
        tokenSize   = tokenPtr->size  -1;

        varColMap[varIdx] = -1;

        for (colIdx = 0; colIdx < numCols; colIdx++) {

            Dbi_ColumnName(handle, colIdx, &colName);

            if (strncmp(colName, tokenString, tokenSize) == 0) {
                varColMap[varIdx] = colIdx;
                break;
            }
        }
        varIdx++;
    }
}


/*
 *----------------------------------------------------------------------
 *
 * FreeTemplateObj, FreeTemplate --
 *
 *      Free the internal rep of a template object. Called by Tcl
 *      when the variable disappears.
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
FreeTemplateObj(Tcl_Obj *objPtr)
{
    Template  *templatePtr;

    templatePtr = objPtr->internalRep.otherValuePtr;
    FreeTemplate(templatePtr);
}

static void
FreeTemplate(Template *templatePtr)
{
    Tcl_FreeParse(&templatePtr->parse);
    ns_free(templatePtr);
}


/*
 *----------------------------------------------------------------------
 *
 * NewTextToken --
 *
 *      Add a run of text to the Tcl_Parse structure.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Memory may be allocated.
 *
 *----------------------------------------------------------------------
 */

static void
NewTextToken(Tcl_Parse *parsePtr, char *string, int length)
{
    Tcl_Token  *tokenPtr;
    int         newCount;

    if (parsePtr->numTokens == parsePtr->tokensAvailable) {
        /*
         * Expand the token array.
         */
        newCount = parsePtr->tokensAvailable * 2;
        tokenPtr = (Tcl_Token *) ckalloc((unsigned) (newCount * sizeof(Tcl_Token)));
        memcpy((void *) tokenPtr, (void *) parsePtr->tokenPtr,
               (size_t) (parsePtr->tokensAvailable * sizeof(Tcl_Token)));
        if (parsePtr->tokenPtr != parsePtr->staticTokens) {
            ckfree((char *) parsePtr->tokenPtr);
        }
        parsePtr->tokenPtr = tokenPtr;
        parsePtr->tokensAvailable = newCount;
    }

    tokenPtr = &parsePtr->tokenPtr[parsePtr->numTokens];
    tokenPtr->type = TCL_TOKEN_TEXT;
    tokenPtr->numComponents = 0;
    tokenPtr->start = string;
    tokenPtr->size = length;
    parsePtr->numTokens++;
}


/*
 *----------------------------------------------------------------------
 *
 * NextRow --
 *
 *      Fetch the next row of the result set.
 *
 * Results:
 *      TCL_OK/TCL_ERROR. *endPtr set to 0/1.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
NextRow(Tcl_Interp *interp, Dbi_Handle *handle, int *endPtr)
{
    if (Dbi_NextRow(handle, endPtr) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        return TCL_ERROR;
    }
    return TCL_OK;
}
