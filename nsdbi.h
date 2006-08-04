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
 * nsdbi.h --
 *
 *      Public types and function declarations for the nsdbi module.
 *
 *  $Header$
 */

#ifndef NSDBI_H
#define NSDBI_H

#include "ns.h"


#define DBI_MAX_BIND 32

typedef enum {
    DBI_EXEC_ROWS = 1,
    DBI_EXEC_DML,
    DBI_EXEC_ERROR
} DBI_EXEC_STATUS;

typedef enum {
    DBI_VALUE = 0,
    DBI_END_COL,
    DBI_END_ROWS,
    DBI_VALUE_ERROR
} DBI_VALUE_STATUS;


/*
 * The following defines opaque pool and statement handles.
 */

typedef struct _Dbi_Pool      *Dbi_Pool;
typedef struct _Dbi_Statement *Dbi_Statement;

/*
 * The following struct defines a handle in a pool.
 */

typedef struct Dbi_Handle {
    Dbi_Pool        *pool; /* The pool this handle belongs to. */
    void            *arg;  /* Driver private handle context. */
} Dbi_Handle;


/*
 * The following define the functions that loadable
 * drivers must implement.
 */

typedef int
(Dbi_OpenProc)(Dbi_Handle *, void *driverArg)
    NS_GNUC_NONNULL(1);

typedef void
(Dbi_CloseProc)(Dbi_Handle *, void *driverArg)
    NS_GNUC_NONNULL(1);

typedef int
(Dbi_ConnectedProc)(Dbi_Handle *, void *driverArg)
    NS_GNUC_NONNULL(1);

typedef void
(Dbi_BindVarProc)(Ns_DString *, CONST char *name, int bindIdx,
                  void *driverArg)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

typedef int
(Dbi_PrepareProc)(Dbi_Handle *, CONST char *sql, int length,
                  unsigned int id, unsigned int nqueries,
                  void **stmtArg, void *driverArg)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(6);

typedef void
(Dbi_PrepareCloseProc)(void *stmtArg, void *driverArg)
    NS_GNUC_NONNULL(1);

typedef DBI_EXEC_STATUS
(Dbi_ExecProc)(Dbi_Handle *, CONST char *sql, int length,
               CONST char **values, unsigned int *lengths, int nvalues,
               int *ncolsPtr, int *nrowsPtr,
               void *stmtArg, void *driverArg)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2)
     NS_GNUC_NONNULL(7) NS_GNUC_NONNULL(8);

typedef int
(Dbi_ValueProc)(Dbi_Handle *, int col, int row,
                CONST char **valuePtr, int *lengthPtr, void *driverArg)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(4) NS_GNUC_NONNULL(5);

typedef int
(Dbi_ColumnProc)(Dbi_Handle *, int col,
                 CONST char **columnPtr, int *lengthPtr, void *driverArg)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3) NS_GNUC_NONNULL(4);

typedef void
(Dbi_FlushProc)(Dbi_Handle *, void *driverArg)
    NS_GNUC_NONNULL(1);

typedef int
(Dbi_ResetProc)(Dbi_Handle *, void *driverArg)
    NS_GNUC_NONNULL(1);

/*
 * The following structure specifies the driver-specific functions
 * to call for each Dbi_ routine.
 */

typedef struct Dbi_Driver {

    void                 *arg;          /* Driver callback data. */

    /*
     * The following callbacks and data are (so far) all required.
     */

    CONST char           *name;         /* Driver name. */
    CONST char           *database;     /* Database name. */
    Dbi_OpenProc         *openProc;
    Dbi_CloseProc        *closeProc;
    Dbi_ConnectedProc    *connectedProc;
    Dbi_BindVarProc      *bindVarProc;
    Dbi_PrepareProc      *prepareProc;
    Dbi_PrepareCloseProc *prepareCloseProc;
    Dbi_ExecProc         *execProc;
    Dbi_ValueProc        *valueProc;
    Dbi_ColumnProc       *columnProc;
    Dbi_FlushProc        *flushProc;
    Dbi_ResetProc        *resetProc;

} Dbi_Driver;



NS_EXTERN int
Dbi_RegisterDriver(CONST char *server, CONST char *module,
                   Dbi_Driver *driver, int size)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

NS_EXTERN Dbi_Pool *
Dbi_GetPool(CONST char *server, CONST char *poolname)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN Dbi_Pool *
Dbi_DefaultPool(CONST char *server)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ListPools(Ns_DString *ds, CONST char *server)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN int
Dbi_GetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *pool, Ns_Conn *conn,
              Ns_Time *timeoutPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN void
Dbi_PutHandle(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ReleaseConnHandles(Ns_Conn *conn)
    NS_GNUC_NONNULL(1);

NS_EXTERN Dbi_Statement *
Dbi_Prepare(Dbi_Handle *, CONST char *sql, int length)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN int
Dbi_GetNumVariables(Dbi_Statement *stmt)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_GetBindVariable(Dbi_Statement *stmt, int idx, CONST char **namePtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);

NS_EXTERN DBI_EXEC_STATUS
Dbi_Exec(Dbi_Handle *, Dbi_Statement *,
         CONST char **values, unsigned int *lengths)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN int
Dbi_NumColumns(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_NumRows(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN DBI_VALUE_STATUS
Dbi_NextValue(Dbi_Handle *, CONST char **valuePtr, int *vlengthPtr,
              CONST char **columnPtr, int *clengthPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

NS_EXTERN void
Dbi_Flush(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_Reset(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

NS_EXTERN void
Dbi_BouncePool(Dbi_Pool *pool)
    NS_GNUC_NONNULL(1);

NS_EXTERN char *
Dbi_Stats(Ns_DString *ds, Dbi_Pool *poolPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN CONST char *
Dbi_PoolName(Dbi_Pool *)
    NS_GNUC_NONNULL(1);

NS_EXTERN CONST char *
Dbi_DriverName(Dbi_Pool *)
    NS_GNUC_NONNULL(1);

NS_EXTERN CONST char *
Dbi_DatabaseName(Dbi_Pool *)
    NS_GNUC_NONNULL(1);

NS_EXTERN void
Dbi_SetException(Dbi_Handle *handle, CONST char *sqlstate, CONST char *fmt, ...)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_PRINTF(3, 4);

NS_EXTERN void
Dbi_ResetException(Dbi_Handle *handle)
     NS_GNUC_NONNULL(1);

NS_EXTERN char *
Dbi_ExceptionCode(Dbi_Handle *handle)
     NS_GNUC_NONNULL(1);

NS_EXTERN char *
Dbi_ExceptionMsg(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ExceptionPending(Dbi_Handle *handle)
     NS_GNUC_NONNULL(1);


#endif /* NSDBI_H */
