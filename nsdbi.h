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
#include "nsattributes.h"

/*
 * The following are dbi return codes.
 */

#define DBI_DML                   1
#define DBI_ROWS                  2
#define DBI_LAST_COL              4
#define DBI_END_DATA              8
#define DBI_NO_DATA              16

#define DBI_READONLY             -3


/*
 * Database pool structure.
 */

struct Dbi_Driver;

typedef struct Dbi_Pool {
    struct Dbi_Driver *driver;
    char              *name;
    char              *description;
    char              *datasource;
    char              *user;
    char              *password;
    int                nhandles;
    int                fVerbose;
    int                fVerboseError;
} Dbi_Pool;

/*
 * Database handle structure.
 */

typedef struct Dbi_Handle {
    Dbi_Pool          *pool;
    int                connected;
    void              *arg;  /* Driver private connection context. */
} Dbi_Handle;

/*
 * Statement structure for performing queries.
 */

typedef struct Dbi_Statement {
    Dbi_Pool          *pool;
    Ns_DString         dsSql;
    int                fetchingRows;
    Tcl_HashTable      bindVars;
    void               *arg;   /* Driver private statement context. */
} Dbi_Statement;

/*
 * Structure for tracking the values of bind variables.
 */
 
typedef struct Dbi_BindValue {
    CONST char    *data;
    int            len;
} Dbi_BindValue;


/*
 * The following typedefs define the functions that loadable
 * drivers implement.
 */

typedef int (Dbi_InitProc)            (CONST char *server, CONST char *module, CONST char *driver);
typedef CONST char *(Dbi_NameProc)    (Dbi_Handle *);
typedef CONST char *(Dbi_DbTypeProc)  (Dbi_Handle *);
typedef int (Dbi_OpenProc)            (Dbi_Handle *);
typedef void (Dbi_CloseProc)          (Dbi_Handle *);
typedef int (Dbi_BindVarProc)         (Ns_DString *, int bindIdx);
typedef int (Dbi_ExecProc)            (Dbi_Handle *, Dbi_Statement *, int *nrows, int *ncols);
typedef int (Dbi_ValueProc)           (Dbi_Handle *, Dbi_Statement *, int rowIdx, int colIdx, CONST char **value, int *len);
typedef int (Dbi_ColumnProc)          (Dbi_Handle *, Dbi_Statement *, int colIdx, CONST char **column, int *len);
typedef void (Dbi_FlushProc)          (Dbi_Statement *);
typedef int (Dbi_ResetProc)           (Dbi_Handle *);


/*
 * The following structure specifies the driver-specific functions
 * to call for each Dbi_ routine.
 */

typedef struct Dbi_Driver {
    char                 *name;
    Dbi_InitProc         *initProc;
    Dbi_NameProc         *nameProc;
    Dbi_DbTypeProc       *typeProc;
    Dbi_OpenProc         *openProc;
    Dbi_CloseProc        *closeProc;
    Dbi_BindVarProc      *bindVarProc;
    Dbi_ExecProc         *execProc;
    Dbi_ValueProc        *valueProc;
    Dbi_ColumnProc       *columnProc;
    Dbi_FlushProc        *flushProc;
    Dbi_ResetProc        *resetProc;
} Dbi_Driver;


/*
 * drv.c:
 */

NS_EXTERN int Dbi_RegisterDriver(Dbi_Driver *driver) _nsnonnull();
NS_EXTERN CONST char *Dbi_DriverName(Dbi_Handle *) _nsnonnull();
NS_EXTERN CONST char *Dbi_DriverDbType(Dbi_Handle *) _nsnonnull();
NS_EXTERN int Dbi_DML(Dbi_Handle *, Dbi_Statement *stmt, int *nrows, int *ncols) _nsnonnull(1,2);
NS_EXTERN int Dbi_Select(Dbi_Handle *, Dbi_Statement *, int *nrows, int *ncols) _nsnonnull(1,2);
NS_EXTERN int Dbi_Exec(Dbi_Handle *, Dbi_Statement *, int *nrows, int *ncols) _nsnonnull(1,2);
NS_EXTERN int Dbi_NextValue(Dbi_Statement *, CONST char **, int *, CONST char **, int *) _nsnonnull(1,2,3);
NS_EXTERN void Dbi_Flush(Dbi_Statement *) _nsnonnull();
NS_EXTERN int Dbi_ResetHandle(Dbi_Handle *) _nsnonnull();

/*
 * init.c:
 */

NS_EXTERN Dbi_Pool *Dbi_GetPool(CONST char *server, CONST char *pool) _nsnonnull();
NS_EXTERN Dbi_Pool *Dbi_PoolDefault(CONST char *server) _nsnonnull();
NS_EXTERN CONST char *Dbi_PoolDbType(Dbi_Pool *poolPtr) _nsnonnull();
NS_EXTERN CONST char *Dbi_PoolDriverName(Dbi_Pool *poolPtr) _nsnonnull();
NS_EXTERN int Dbi_PoolList(Ns_DString *ds, CONST char *server) _nsnonnull();
NS_EXTERN void Dbi_PoolPutHandle(Dbi_Handle *handle) _nsnonnull();
NS_EXTERN int Dbi_PoolGetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *poolPtr) _nsnonnull();
NS_EXTERN int Dbi_PoolTimedGetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *poolPtr, int wait) _nsnonnull();
NS_EXTERN void Dbi_BouncePool(Dbi_Pool *poolPtr) _nsnonnull();
NS_EXTERN void Dbi_PoolStats(Ns_DString *ds, Dbi_Pool *poolPtr) _nsnonnull();

/*
 * stmt.c
 */

NS_EXTERN Dbi_Statement *Dbi_StatementAlloc(Dbi_Pool *, CONST char *sql, int len) _nsnonnull();
NS_EXTERN void Dbi_StatementFree(Dbi_Statement *) _nsnonnull();
NS_EXTERN int Dbi_StatementBindValue(Dbi_Statement *, char *name, char *value, int len) _nsnonnull();

/*
 * util.c:
 */

NS_EXTERN void Dbi_QuoteValue(Ns_DString *pds, CONST char *string);
NS_EXTERN int Dbi_0or1Row(Dbi_Handle *handle, Dbi_Statement *stmt, int *nrows, int *ncols) _nsnonnull(1, 2);
NS_EXTERN int Dbi_1Row(Dbi_Handle *handle, Dbi_Statement *stmt, int *ncols) _nsnonnull(1, 2);
NS_EXTERN void Dbi_SetException(Dbi_Handle *handle, CONST char *sqlstate, CONST char *fmt, ...)
     _nsprintflike(3, 4) _nsnonnull(1, 2);
NS_EXTERN void Dbi_ResetException(Dbi_Handle *handle) _nsnonnull();
NS_EXTERN char *Dbi_ExceptionCode(Dbi_Handle *handle) _nsnonnull();
NS_EXTERN char *Dbi_ExceptionMsg(Dbi_Handle *handle) _nsnonnull();
NS_EXTERN int Dbi_ExceptionPending(Dbi_Handle *handle) _nsnonnull();

#endif /* NSDBI_H */
