/* 
 * tcltypes.c --
 *
 *      Tcl object types for speedy access to pools and statements.
 */

#include "dbi.h"

NS_RCSID("@(#) $Header$");



/*
 * Static functions defined in this file
 */

void DbiInitTclTypes();

static Tcl_UpdateStringProc  UpdateStringOfPool;
static Tcl_SetFromAnyProc    SetPoolFromAny;
static void SetPoolInternalRep(Tcl_Obj *objPtr, Dbi_Pool *pool);

/* Tcl_FreeInternalRepProc FreeStmt; */
/* Tcl_DupInternalRepProc  DupStmt; */
/* Tcl_UpdateStringProc    UpdateStringOfStmt; */
/* Tcl_SetFromAnyProc      SetStmtFromAny; */


/*
 * Static variables defined in this file.
 */

static Tcl_ObjType poolType = {
    "nsdbi:pool",
    (Tcl_FreeInternalRepProc *) NULL,
    (Tcl_DupInternalRepProc *) NULL,
    UpdateStringOfPool,
    SetPoolFromAny
};

/* static Tcl_ObjType stmtType = { */
/*     "nsdbi:statement", */
/*     FreeStmt, */
/*     DupStmt, */
/*     UpdateStringOfStmt, */
/*     SetStmtFromAny */
/* }; */




/*
 *----------------------------------------------------------------------
 * DbiInitTclObjTypes --
 *
 *      One time initialization of the nsdbi Tcl_Obj types.
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
DbiInitTclObjTypes()
{
    Tcl_RegisterObjType(&poolType);
    //Tcl_RegisterObjType(&stmtType);
}


/*
 *----------------------------------------------------------------------
 * DbiGetPoolFromObj --
 *
 *      Loop up a pool pointer given the pool name, or use the default
 *      pool if no pool specified.
 *
 * Results:
 *      Pointer to pool, or NULL on error with message left in interp
 *
 * Side effects:
 *      The result of the lookup is cached as the internal rep of
 *      the objPtr, so that repeated lookups can be done quickly.
 *
 *----------------------------------------------------------------------
 */

Dbi_Pool *
DbiGetPoolFromObj(Tcl_Interp *interp, ServerData *sdataPtr, Tcl_Obj *objPtr)
{
    Dbi_Pool *pool;
    char     *poolname;

    if (objPtr && objPtr->typePtr == &poolType) {
        pool = (Dbi_Pool *) objPtr->internalRep.otherValuePtr;
        return pool;
    }
    if (!objPtr) {
        if (sdataPtr->defpoolPtr == NULL) {
            Tcl_AppendToObj(Tcl_GetObjResult(interp),
                            "no pool specified and no default configured",
                            -1);
            return NULL;
        } else {
            return (Dbi_Pool *) sdataPtr->defpoolPtr;
        }
    }
    poolname = Tcl_GetString(objPtr);
    pool = DbiGetPool(sdataPtr, poolname);
    if (pool == NULL) {
        Tcl_AppendToObj(Tcl_GetObjResult(interp),
                        "invalid pool name or pool not available to virtual server",
                        -1);
        return NULL;
    }
    SetPoolInternalRep(objPtr, pool);
    return pool;
}


/*
 *----------------------------------------------------------------------
 * UpdateStringOfPool --
 *
 *     This procedure is called to convert a Tcl object from pool
 *     internal form to it's string form, which is the name of the pool.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      The string representation of the objec tis updated.
 *
 *----------------------------------------------------------------------
 */

static void
UpdateStringOfPool(Tcl_Obj *objPtr)
{
    Dbi_Pool *pool = (Dbi_Pool *) objPtr->internalRep.otherValuePtr;
    unsigned int len;
    char *buf;

    len = strlen(pool->name);
    buf = (char *) ckalloc(len + 1);
    memcpy(buf, pool->name, len+1);
    objPtr->bytes = buf;
    objPtr->length = len;
}


/*
 *----------------------------------------------------------------------
 * SetPoolFromAny --
 *
 *     This procedure is called to convert a Tcl object to pool
 *     internal form.  However, this doesn't make sense (need to have
 *     a ServerData pointer to validate access to pool) so the procedure
 *     allways generates an error.
 *
 * Results:
 *      Always TCL_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
SetPoolFromAny(Tcl_Interp *interp, Tcl_Obj *objPtr)
{
    Tcl_AppendToObj(Tcl_GetObjResult(interp),
                    "can't convert value to pool except via DbiGetPoolFromObj API",
                    -1);
    return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * SetPoolInternalRep --
 *
 *      Set the internal Dbi_Pool, freeing a previous internal rep if
 *      necessary.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Object will be an pool obj type.
 *
 *----------------------------------------------------------------------
 */

static void
SetPoolInternalRep(Tcl_Obj *objPtr, Dbi_Pool *pool)
{
    Tcl_ObjType *typePtr = objPtr->typePtr;

    if (typePtr != NULL && typePtr->freeIntRepProc != NULL) {
        (*typePtr->freeIntRepProc)(objPtr);
    }
    objPtr->typePtr = &poolType;
    objPtr->internalRep.otherValuePtr = pool;
    Tcl_InvalidateStringRep(objPtr);
    objPtr->length = 0;
}
