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

typedef struct Dbi_Pool {
    char             *name;
    char             *description;
    char             *datasource;
    char             *user;
    char             *password;
    int               nhandles;
    int               fVerbose;
    int               fVerboseError;
} Dbi_Pool;

/*
 * Database handle structure.
 */

typedef struct Dbi_Handle {
    Dbi_Pool         *poolPtr;
    int               connected;
    int               fetchingRows;
    void             *arg;
} Dbi_Handle;


/*
 * The following typedefs define the functions that loadable
 * drivers implement.
 */

typedef int    (Dbi_InitProc)        (char *server, char *module, char *driver);
typedef char  *(Dbi_NameProc)        (Dbi_Handle *);
typedef char  *(Dbi_DbTypeProc)      (Dbi_Handle *);
typedef int    (Dbi_OpenProc)        (Dbi_Handle *);
typedef void   (Dbi_CloseProc)       (Dbi_Handle *);
typedef int    (Dbi_ExecProc)        (Dbi_Handle *, char *sql, int *nrows, int *ncols);
typedef int    (Dbi_ValueProc)       (Dbi_Handle *, int rowIdx, int colIdx, char **value, int *len);
typedef int    (Dbi_ColumnProc)      (Dbi_Handle *, int colIdx, char **column, int *len);
typedef int    (Dbi_CancelProc)      (Dbi_Handle *);
typedef int    (Dbi_FlushProc)       (Dbi_Handle *);
typedef int    (Dbi_ResetProc)       (Dbi_Handle *);
typedef int    (Dbi_TableListProc)   (Dbi_Handle *, int incsys, int *ntables);
typedef int    (Dbi_GetTableInfoProc)(Dbi_Handle *, char *table, int *ncols);
typedef char  *(Dbi_BestRowProc)     (Ns_DString *pk, Dbi_Handle *, char *table);


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
    Dbi_ExecProc         *execProc;
    Dbi_ValueProc        *valueProc;
    Dbi_ColumnProc       *columnProc;
    Dbi_CancelProc       *cancelProc;
    Dbi_FlushProc        *flushProc;
    Dbi_ResetProc        *resetProc;
    Dbi_TableListProc    *tableListProc;
    Dbi_GetTableInfoProc *tableInfoProc;
    Dbi_BestRowProc      *bestRowProc;
} Dbi_Driver;


/*
 * drv.c:
 */

NS_EXTERN int   Dbi_RegisterDriver(Dbi_Driver *driver);
NS_EXTERN char *Dbi_DriverName(Dbi_Handle *) _nsnonnull();
NS_EXTERN char *Dbi_DriverDbType(Dbi_Handle *) _nsnonnull();
NS_EXTERN int   Dbi_DML(Dbi_Handle *, char *sql, int *nrows, int *ncols) _nsnonnull(1, 2);
NS_EXTERN int   Dbi_Select(Dbi_Handle *, char *sql, int *nrows, int *ncols) _nsnonnull(1, 2);
NS_EXTERN int   Dbi_Exec(Dbi_Handle *, char *sql, int *nrows, int *ncols) _nsnonnull(1, 2);
NS_EXTERN int   Dbi_NextValue(Dbi_Handle *, char **value, int *vLen, char **column, int *cLen);
NS_EXTERN int   Dbi_CurrentColumn(Dbi_Handle *, char **column, int *len);
NS_EXTERN int   Dbi_Cancel(Dbi_Handle *) _nsnonnull();
NS_EXTERN int   Dbi_Flush(Dbi_Handle *) _nsnonnull();
NS_EXTERN int   Dbi_ResetHandle(Dbi_Handle *) _nsnonnull();
NS_EXTERN int   Dbi_TableList(Dbi_Handle *, int incsys, int *ntables);
NS_EXTERN int   Dbi_GetTableInfo(Dbi_Handle *, char *table, int *ncols);
NS_EXTERN char *Dbi_BestRow(Ns_DString *ds, Dbi_Handle *handle, char *table);

/*
 * init.c:
 */

NS_EXTERN Dbi_Pool *Dbi_GetPool(char *server, char *pool) _nsnonnull();
NS_EXTERN Dbi_Pool *Dbi_PoolDefault(char *server) _nsnonnull();
NS_EXTERN char *Dbi_PoolDbType(Dbi_Pool *poolPtr) _nsnonnull();
NS_EXTERN char *Dbi_PoolDriverName(Dbi_Pool *poolPtr) _nsnonnull();
NS_EXTERN int Dbi_PoolList(Ns_DString *ds, char *server) _nsnonnull();
NS_EXTERN void Dbi_PoolPutHandle(Dbi_Handle *handle) _nsnonnull();
NS_EXTERN int Dbi_PoolGetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *poolPtr) _nsnonnull();
NS_EXTERN int Dbi_PoolTimedGetHandle(Dbi_Handle **handlePtrPtr, Dbi_Pool *poolPtr, int wait) _nsnonnull();
NS_EXTERN int Dbi_BouncePool(Dbi_Pool *poolPtr) _nsnonnull();

/*
 * util.c:
 */

NS_EXTERN void Dbi_QuoteValue(Ns_DString *pds, char *string);
NS_EXTERN int Dbi_0or1Row(Dbi_Handle *handle, char *sql, int *nrows, int *ncols) _nsnonnull(1, 2);
NS_EXTERN int Dbi_1Row(Dbi_Handle *handle, char *sql, int *ncols) _nsnonnull(1, 2);
NS_EXTERN int Dbi_InterpretSqlFile(Dbi_Handle *handle, char *filename);
NS_EXTERN void Dbi_SetException(Dbi_Handle *handle, char *sqlstate, char *fmt, ...)
     _nsprintflike(3, 4) _nsnonnull(1, 2);

#endif /* NSDBI_H */
