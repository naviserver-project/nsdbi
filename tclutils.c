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
 * tclutils.c --
 *
 *      Utility routines for creating Tcl dbi commands.
 */

#include "nsdbi.h"

NS_RCSID("@(#) $Header$");





/*
 *----------------------------------------------------------------------
 *
 * Dbi_TclGetPool --
 *
 *      Return a pool handle using one of 3 methods:
 *
 *      - Look up the pool using the given pool name
 *      - Use the pool of the most recently cached handle
 *      - Use the server default pool
 *
 * Results:
 *      Pointer to Dbi_Pool or NULL if no default pool.
 *
 * Side effects:
 *      The Tcl object may be converted to dbi:pool type, and en error
 *      may be left in the interp if conversion fails.
 *
 *----------------------------------------------------------------------
 */

Dbi_Pool *
Dbi_TclGetPool(Tcl_Interp *interp, CONST char *server, Tcl_Obj *poolObj)
{
    Dbi_Pool          *pool;
    static const char *poolType = "dbi:pool";

    if (poolObj != NULL) {
        if (Ns_TclGetOpaqueFromObj(poolObj, poolType, (void **) &pool)
                != TCL_OK) {
            pool = Dbi_GetPool(server, Tcl_GetString(poolObj));
            if (pool != NULL) {
                Ns_TclSetOpaqueObj(poolObj, poolType, pool);
            } else {
                Tcl_SetResult(interp,
                    "invalid pool name or pool not available to virtual server",
                    TCL_STATIC);
            }
        }
    } else {
        pool = Dbi_DefaultPool(server);
        if (pool == NULL) {
            Tcl_SetResult(interp,
                "no pool specified and no default configured",
                 TCL_STATIC);
        }
    }

    return pool;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_TclErrorResult --
 *
 *      Set the Tcl error from the handle code and message.
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
Dbi_TclErrorResult(Tcl_Interp *interp, Dbi_Handle *handle)
{
    Tcl_SetErrorCode(interp, Dbi_ExceptionCode(handle), NULL);
    Tcl_AppendResult(interp, Dbi_ExceptionMsg(handle), NULL);
}
