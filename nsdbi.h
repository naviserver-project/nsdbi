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


/*
 * The following are dbi return codes.
 */

#define DBI_DML           0x01
#define DBI_ROWS          0x02
#define DBI_LAST_COL      0x04
#define DBI_END_DATA      0x08
#define DBI_NO_DATA       0x10

#define DBI_READONLY      -3

#define DBI_MAX_BIND      32


/*
 * The following defines opaque pool and statement handles.
 */

typedef struct _Dbi_Pool       *Dbi_Pool;
typedef struct _Dbi_Statement  *Dbi_Statement;

/*
 * The following struct defines a database handle.
 */

typedef struct Dbi_Handle {
    Dbi_Pool        *pool; /* The pool this handle belongs to. */
    void            *arg;  /* Driver private handle context. */
} Dbi_Handle;

/*
 * The following struct defines a query which is used to
 * send SQL to the database and fetch the result.
 */

typedef struct Dbi_Query {

    Dbi_Handle      *handle;       /* The db handle to query. */
    Dbi_Statement   *stmt;         /* The statement to send. */
    void            *arg;          /* Driver private query context. */

    /*
     * The following defines any values for bind variables.
     */

    int              nbound;       /* The number of bound values. */
    struct {
        char        *value;        /* The values. */
        int          len;          /* Value length. */
    } bind[DBI_MAX_BIND];

    /*
     * The following maintains the query result.
     */

    struct {
        int          fetchingRows; /* Is there a pending result set? */
        int          numCols;      /* Number of columns in penfing result. */
        int          numRows;      /* Number of rows in pending result. */
        int          currentCol;   /* The current column index. */
        int          currentRow;   /* The current row index. */
    } result;

} Dbi_Query;

/*
 * The following typedefs define the functions that loadable
 * drivers must implement.
 */

typedef int         (Dbi_OpenProc)    (Dbi_Handle *, void *arg);
typedef void        (Dbi_CloseProc)   (Dbi_Handle *, void *arg);
typedef void        (Dbi_BindVarProc) (Ns_DString *, CONST char *name, int bindIdx, void *arg);
typedef int         (Dbi_ExecProc)    (Dbi_Query *, void *arg);
typedef int         (Dbi_ValueProc)   (Dbi_Query *, CONST char **value, int *len,
                                       void *arg);
typedef int         (Dbi_ColumnProc)  (Dbi_Query *, CONST char **column, int *len,
                                       void *arg);
typedef void        (Dbi_FlushProc)   (Dbi_Query *, void *arg);
typedef int         (Dbi_ResetProc)   (Dbi_Handle *, void *arg);

/*
 * The following structure specifies the driver-specific functions
 * to call for each Dbi_ routine.
 */

typedef struct Dbi_Driver {
    CONST char           *name;
    CONST char           *database;
    Dbi_OpenProc         *openProc;
    Dbi_CloseProc        *closeProc;
    Dbi_BindVarProc      *bindVarProc;
    Dbi_ExecProc         *execProc;
    Dbi_ValueProc        *valueProc;
    Dbi_ColumnProc       *columnProc;
    Dbi_FlushProc        *flushProc;
    Dbi_ResetProc        *resetProc;
    void                 *arg;
} Dbi_Driver;


/*
 * drv.c:
 */

NS_EXTERN int
Dbi_RegisterDriver(CONST char *server, CONST char *module, Dbi_Driver *driver)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

NS_EXTERN CONST char *
Dbi_DriverName(Dbi_Pool *)
    NS_GNUC_NONNULL(1);

NS_EXTERN CONST char *
Dbi_DatabaseName(Dbi_Pool *)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_Exec(Dbi_Query *)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_NextValue(Dbi_Query *, CONST char **, int *, CONST char **, int *)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

NS_EXTERN void
Dbi_Flush(Dbi_Query *)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ResetHandle(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

/*
 * init.c:
 */

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
Dbi_GetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *pool, Ns_Conn *conn, int wait)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN void
Dbi_PutHandle(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ReleaseConnHandles(Ns_Conn *conn)
    NS_GNUC_NONNULL(1);

NS_EXTERN void
Dbi_BouncePool(Dbi_Pool *pool)
    NS_GNUC_NONNULL(1);

NS_EXTERN char *
Dbi_Stats(Ns_DString *ds, Dbi_Pool *poolPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

/*
 * stmt.c
 */

NS_EXTERN Dbi_Statement *
Dbi_StatementAlloc(CONST char *sql, int len)
     NS_GNUC_NONNULL(1);

NS_EXTERN void
Dbi_StatementFree(Dbi_Statement *)
     NS_GNUC_NONNULL(1);

NS_EXTERN CONST char *
Dbi_StatementBoundSQL(Dbi_Statement *, int *len)
     NS_GNUC_NONNULL(1);

NS_EXTERN CONST char *
Dbi_StatementSQL(Dbi_Statement *, int *len)
     NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_StatementGetBindVar(Dbi_Statement *, int idx, CONST char **key)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);

NS_EXTERN int
Dbi_QueryGetBindValue(Dbi_Query *, int idx,
                      CONST char **value, int *len)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3) NS_GNUC_NONNULL(4);

NS_EXTERN int
Dbi_QuerySetBindValue(Dbi_Query *, int idx, CONST char *value, int len)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);


/*
 * util.c:
 */

NS_EXTERN int
Dbi_Select(Dbi_Query *)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_0or1Row(Dbi_Query *)
     NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_1Row(Dbi_Query *)
     NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_DML(Dbi_Query *)
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
