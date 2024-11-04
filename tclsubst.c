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
                        Tcl_Obj *templateObj, Tcl_Obj *defaultObj, int adp, Dbi_quotingLevel quote);


/*
 * Static functions defined in this file.
 */

static int GetTemplateFromObj(Tcl_Interp *interp, Dbi_Handle *,
                              Tcl_Obj *templateObj, Template **templatePtrPtr);
static int AppendValue(Tcl_Interp *interp, Dbi_Handle *handle, unsigned int index,
                       Tcl_Obj *resObj, Ns_DString *dsPtr, Dbi_quotingLevel quote);
static int AppendTokenVariable(Tcl_Interp *interp, Tcl_Token *tokenPtr,
                               Tcl_Obj *resObj, Ns_DString *dsPtr, Dbi_quotingLevel quote);
static void AppendInt(Tcl_Interp *, unsigned int rowint,
                      Tcl_Obj *resObj, Ns_DString *dsPtr);
static void MapVariablesToColumns(Dbi_Handle *handle, Template *templatePtr);
static void NewTextToken(Tcl_Parse *parsePtr, char *string, int length);
static int NextRow(Tcl_Interp *interp, Dbi_Handle *handle, int *endPtr);

static void FreeTemplate(Template *);
static Tcl_FreeInternalRepProc FreeTemplateObj;


/*
 * Static variables defined in this file.
 */

static CONST86 Tcl_ObjType templateType = {
    "dbi:template",
    FreeTemplateObj,
    (Tcl_DupInternalRepProc *) NULL,
    (Tcl_UpdateStringProc *) NULL,
    Ns_TclSetFromAnyError
#ifdef TCL_OBJTYPE_V0
   ,TCL_OBJTYPE_V0
#endif
};

/*
 * The following defines the variable types other than column names
 * which may appear in a template. Numbers >= 0 are column indexes.
 */

#define VARTYPE_TCL     -1  /* A Tcl variable. */
#define VARTYPE_ROWIDX  -2  /* The zero-based row number. */
#define VARTYPE_ROWNUM  -3  /* The one-based row number. */
#define VARTYPE_PARITY  -4  /* Whether the row is even or odd (zero-based). */

static struct {
    const char *varName;
    int         type;
} specials[] = {
    {"dbi(rowidx)", VARTYPE_ROWIDX},
    {"dbi(rownum)", VARTYPE_ROWNUM},
    {"dbi(parity)", VARTYPE_PARITY},
};



/*
 *----------------------------------------------------------------------
 *
 * QuoteJS --
 *
 *      Append valid JavaScript strings (between single quotes)
 *      to first argument
 *
 * Results:
 *      none
 *
 * Side effects:
 *      Properly escaped string is returned in the passed DString object.
 *
 *----------------------------------------------------------------------
 */
static void
QuoteJS(Ns_DString *dsPtr, char *string)
{
    Ns_DStringAppend(dsPtr, "'");
    while (likely(*string != '\0')) {
        switch (*string) {
        case '\'':
            Ns_DStringAppend(dsPtr, "\\'");
            break;

        default:
            Ns_DStringNAppend(dsPtr, string, 1);
            break;
        }
        ++string;
    }
    Ns_DStringAppend(dsPtr, "'");
}


/*
 *----------------------------------------------------------------------
 *
 * Quote --
 *
 *      Append a potentially quoted string to first argument.
 *
 * Results:
 *      None
 *
 * Side effects:
 *      Passed string is appended to first argument, potentially quoted.
 *
 *----------------------------------------------------------------------
 */
static void
Quote(Ns_DString *dsPtr, char *value, Dbi_quotingLevel quote)
{
    if (likely(quote == Dbi_QuoteHTML)) {
        Ns_QuoteHtml(dsPtr, value);
    } else if (quote == Dbi_QuoteJS) {
        QuoteJS(dsPtr, value);
    } else {
        Ns_DStringNAppend(dsPtr, value, -1);
    }
}



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
                    Tcl_Obj *templateObj, Tcl_Obj *defaultObj, int adp, Dbi_quotingLevel quote)
{
    Template      *templatePtr;
    Tcl_Parse     *parsePtr;
    Tcl_Token     *tokenPtr;
    Tcl_Obj       *resObj;
    Ns_DString    *dsPtr;
    const char    *parity;
    int           *varColMap, end;
    TCL_SIZE_T     len, tokIdx, varIdx;
    int            stream = 0, colIdx;
    size_t         maxBuffer = 0u;
    unsigned int   numRows;


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
     * Append to the Tcl result or directly to the ADP output buffer.
     */

    if (adp) {
        resObj = NULL;
        if (Ns_AdpGetOutput(interp, &dsPtr, &stream, &maxBuffer) != TCL_OK) {
            return TCL_ERROR;
        }
    } else {
        resObj = Tcl_GetObjResult(interp);
        dsPtr = NULL;
    }

    /*
     * Step through the result rows, appending column values and Tcl
     * variables to the final result.
     */

    numRows = 0;

    while (NextRow(interp, handle, &end) == TCL_OK
           && !end) {

        numRows++;

        for (tokIdx = 0, varIdx = 0;
             tokIdx < parsePtr->numTokens;
             tokIdx += (tokenPtr->numComponents + 1)) {

            tokenPtr = &parsePtr->tokenPtr[tokIdx];

            switch (tokenPtr->type) {

            case TCL_TOKEN_TEXT:
                if (adp) {
                    if (Ns_AdpAppend(interp, tokenPtr->start, tokenPtr->size)
                            != TCL_OK) {
                        return TCL_ERROR;
                    }
                } else {
                    Tcl_AppendToObj(resObj, tokenPtr->start, tokenPtr->size);
                }
                break;

            case TCL_TOKEN_VARIABLE:

                colIdx = varColMap[varIdx++];

                switch (colIdx) {
                case VARTYPE_TCL:
                    if (AppendTokenVariable(interp, tokenPtr, resObj, dsPtr, quote)
                            != TCL_OK) {
                        return TCL_ERROR;
                    }
                    break;

                case VARTYPE_ROWIDX:
                    AppendInt(interp, handle->rowIdx, resObj, dsPtr);
                    break;

                case VARTYPE_ROWNUM:
                    AppendInt(interp, handle->rowIdx +1, resObj, dsPtr);
                    break;

                case VARTYPE_PARITY:
                    parity = handle->rowIdx % 2 == 0 ? "even" : "odd";
                    if (dsPtr != NULL) {
                        Ns_DStringAppend(dsPtr, parity);
                    } else {
                        Tcl_AppendToObj(resObj, parity, -1);
                    }
                    break;

                default:
                    if (AppendValue(interp, handle, (unsigned int)colIdx, resObj, dsPtr, quote)
                            != TCL_OK) {
                        return TCL_ERROR;
                    }
                    break;
                }
                break;

            default:
                Ns_Fatal("DbiTclSubstTemplate: unexpected token type: %d",
                         tokenPtr->type);
                /*break;*/
            }
        }

        /*
         * Flush the ADP buffer after every row if we're in streaming
         * mode or the buffer grows too large.
         */

        if (dsPtr != NULL
            && (stream != 0 || (Ns_DStringLength(dsPtr) > (int)maxBuffer))
            && Ns_AdpFlush(interp, 1) != TCL_OK) {
            return TCL_ERROR;
        }

    }

    if (numRows == 0) {
        if (defaultObj != NULL) {
            if (adp) {
                char *def = Tcl_GetStringFromObj(defaultObj, &len);

                if (Ns_AdpAppend(interp, def, len) != TCL_OK) {
                    return TCL_ERROR;
                }
            } else {
                Tcl_SetObjResult(interp, defaultObj);
            }
        } else {
            Tcl_SetObjResult(interp, Tcl_NewStringObj("query was not a statement returning rows", -1));
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
            Tcl_Obj *resObj, Ns_DString *dsPtr, Dbi_quotingLevel quote)
{
    int         binary;
    size_t      valueLength;
    TCL_SIZE_T  resultLength;
    char       *bytes;

    if (Dbi_ColumnLength(handle, index, &valueLength, &binary) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        return TCL_ERROR;
    }
    if (binary) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj("can't substitute binary value in template", -1));
        return TCL_ERROR;
    }

    if (dsPtr) {
        resultLength = Ns_DStringLength(dsPtr);
        Ns_DStringSetLength(dsPtr, (resultLength + (int)valueLength));
        bytes = dsPtr->string + resultLength;
    } else {
        (void) Tcl_GetStringFromObj(resObj, &resultLength);
        Tcl_SetObjLength(resObj, (resultLength + (int)valueLength));
        bytes = resObj->bytes + resultLength;
    }

    if (Dbi_ColumnValue(handle, index, bytes, valueLength) != NS_OK) {
        Dbi_TclErrorResult(interp, handle);
        return TCL_ERROR;
    }


    if (quote != Dbi_QuoteNone) {
        Tcl_DString ds, *dsPtr2 = &ds;
        TCL_SIZE_T  quotedLength;

        Tcl_DStringInit(dsPtr2);
        Quote(dsPtr2, bytes, quote);
        quotedLength = Tcl_DStringLength(dsPtr2);

        /*
         * If nothing has to be quoted, the sizes are identical, no
         * need to copy result from quoteing
         */

        if (quotedLength != (int)valueLength) {
            if (dsPtr) {
                Ns_DStringSetLength(dsPtr, (resultLength + quotedLength));
                memcpy(dsPtr->string + resultLength, dsPtr2->string, quotedLength);
            } else {
                Tcl_SetObjLength(resObj, (resultLength + quotedLength));
                memcpy(resObj->bytes + resultLength, dsPtr2->string, quotedLength);
            }
        }
        Tcl_DStringFree(dsPtr2);
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
AppendTokenVariable(Tcl_Interp *interp, Tcl_Token *tokenPtr,
                    Tcl_Obj *resObj, Ns_DString *dsPtr, Dbi_quotingLevel quote)
{
    Tcl_Obj   *objPtr;
    char      *name, *value, save;
    TCL_SIZE_T size;

    /* NB: Skip past leading '$' */
    name = (char *) tokenPtr->start + 1;
    size = tokenPtr->size - 1;

    save = name[size];
    name[size] = '\0';
    objPtr = Tcl_GetVar2Ex(interp, name, NULL, TCL_LEAVE_ERR_MSG);
    if (objPtr == NULL) {
        Tcl_ResetResult(interp);
        Tcl_AppendResult(interp, "can't read \"", name,
                         "\": no such column or variable", (char *)0L);
        name[size] = save;

        return TCL_ERROR;
    }
    name[size] = save;
    value = Tcl_GetStringFromObj(objPtr, &size);

    if (dsPtr) {
        Quote(dsPtr, value, quote);
    } else {
        if (quote != Dbi_QuoteNone) {
            Tcl_DString ds1, *ds1Ptr = &ds1;

            Tcl_DStringInit(ds1Ptr);
            Quote(ds1Ptr, value, quote);
            Tcl_AppendToObj(resObj,
                            Tcl_DStringValue(ds1Ptr),
                            Tcl_DStringLength(ds1Ptr));
            Tcl_DStringFree(ds1Ptr);
        } else {
            Tcl_AppendObjToObj(resObj, objPtr);
        }
    }

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * AppendInt --
 *
 *      Append the string rep of the given int to the result buffer.
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
AppendInt(Tcl_Interp *UNUSED(interp), unsigned int rowint,
          Tcl_Obj *resObj, Ns_DString *dsPtr)
{
    char buf[TCL_INTEGER_SPACE];

    snprintf(buf, sizeof(buf), "%u", rowint);
    if (dsPtr) {
        Ns_DStringAppend(dsPtr, buf);
    } else {
        Tcl_AppendToObj(resObj, buf, -1);
    }
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
    int         numVarTokens;
    TCL_SIZE_T  length, varIdx;

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
                NewTextToken(parsePtr, string, (int)(p-string));
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
        NewTextToken(parsePtr, string, (int)(p-string));
    }

    if (numVarTokens == 0) {
        Tcl_SetObjResult(interp, Tcl_NewStringObj("template contains no variables", -1));
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
    const char    *tokenString, *colName;
    int            tokIdx, varIdx;
    size_t         i, colIdx, numCols;
    TCL_SIZE_T     tokenSize;

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

        varColMap[varIdx] = VARTYPE_TCL;

        for (colIdx = 0; colIdx < numCols; colIdx++) {

            Dbi_ColumnName(handle, (unsigned int)colIdx, &colName);

            if (strncmp(colName, tokenString, (size_t)tokenSize) == 0) {
                varColMap[varIdx] = (int)colIdx;
                break;
            }
        }

        /*
         * Check for special variables.
         */

        if (varColMap[varIdx] == VARTYPE_TCL) {
            for (i = 0; i < sizeof(specials) / sizeof(specials[0]); i++) {
                if (strncmp(specials[i].varName, tokenString, (size_t)tokenSize) == 0) {
                    varColMap[varIdx] = specials[i].type;
                }
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

    if (parsePtr->numTokens == parsePtr->tokensAvailable) {
        /*
         * Expand the token array.
         */
        TCL_SIZE_T newCount = parsePtr->tokensAvailable * 2;

        tokenPtr = (Tcl_Token *) ckalloc((unsigned) ((size_t)newCount * sizeof(Tcl_Token)));
        memcpy((void *) tokenPtr, (void *) parsePtr->tokenPtr,
               ((size_t)parsePtr->tokensAvailable * sizeof(Tcl_Token)));
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

/*
 * Local Variables:
 * mode: c
 * c-basic-offset: 4
 * fill-column: 72
 * indent-tabs-mode: nil
 * End:
 */
