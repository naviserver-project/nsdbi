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

#define DBI_DML                1
#define DBI_ROWS               2
#define DBI_END_DATA           4
#define DBI_NO_DATA            8

/*
 * Database handle structure.
 */

typedef struct Dbi_Handle {
    char       *driver;
    char       *datasource;
    char       *user;
    char       *password;
    void       *connection;
    char       *poolname;
    int         connected;
    int         verbose;
    Ns_Set     *row;
    char        cExceptionCode[6];
    Ns_DString  dsExceptionMsg;
    void       *context;
    void       *statement;
    int         fetchingRows;
} Dbi_Handle;

/*
 * The following structure is no longer supported and only provided to
 * allow existing database modules to compile.  All of the TableInfo
 * routines now log an unsupported use error and return an error result.
 */

typedef struct {
    Ns_Set  *table;
    int      size;
    int      ncolumns;
    Ns_Set **columns;
} Dbi_TableInfo;

/*
 * The following typedefs define the functions that loadable
 * drivers implement.
 */

typedef int            (Dbi_InitProc)        (char *server, char *module, char *driver);
typedef char          *(Dbi_NameProc)        (Dbi_Handle *);
typedef char          *(Dbi_DbTypeProc)      (Dbi_Handle *);
typedef int            (Dbi_OpenProc)        (Dbi_Handle *);
typedef void           (Dbi_CloseProc)       (Dbi_Handle *);
typedef int            (Dbi_ExecProc)        (Dbi_Handle *, char *sql);
typedef Ns_Set *       (Dbi_BindRowProc)     (Dbi_Handle *);
typedef int            (Dbi_GetRowProc)      (Dbi_Handle *, Ns_Set *row);
typedef int            (Dbi_CancelProc)      (Dbi_Handle *);
typedef int            (Dbi_FlushProc)       (Dbi_Handle *);
typedef int            (Dbi_ResetProc)       (Dbi_Handle *);
typedef char          *(Dbi_TableListProc)   (Ns_DString *tables, Dbi_Handle *, int incsys);
typedef Dbi_TableInfo *(Dbi_GetTableInfoProc)(Dbi_Handle *, char *table);
typedef char          *(Dbi_BestRowProc)     (Ns_DString *pk, Dbi_Handle *, char *table);

/* 
 * The following enum defines known dbi driver function ids.
 */

typedef enum {
    Dbi_InitId,
    Dbi_NameId,
    Dbi_DbTypeId,
    Dbi_OpenId,
    Dbi_CloseId,
    Dbi_ExecId,
    Dbi_BindRowId,
    Dbi_GetRowId,
    Dbi_CancelId,
    Dbi_FlushId,
    Dbi_ResetId,
    Dbi_TableListId,
    Dbi_GetTableInfoId,
    Dbi_BestRowId
} Dbi_ProcId;

/*
 * Database procedure structure used when registering
 * a driver. 
 */

typedef struct Dbi_Proc {
    Dbi_ProcId  id;
    void       *func;
} Dbi_Proc;


/*
 * drv.c:
 */

NS_EXTERN int Dbi_RegisterDriver(char *driver, Dbi_Proc *procs);
NS_EXTERN char *Dbi_DriverName(Dbi_Handle *handle);
NS_EXTERN char *Dbi_DriverDbType(Dbi_Handle *handle);
NS_EXTERN int Dbi_DML(Dbi_Handle *handle, char *sql);
NS_EXTERN Ns_Set *Dbi_Select(Dbi_Handle *handle, char *sql);
NS_EXTERN int Dbi_Exec(Dbi_Handle *handle, char *sql);
NS_EXTERN Ns_Set *Dbi_BindRow(Dbi_Handle *handle);
NS_EXTERN int Dbi_GetRow(Dbi_Handle *handle, Ns_Set *row);
NS_EXTERN int Dbi_Flush(Dbi_Handle *handle);
NS_EXTERN int Dbi_Cancel(Dbi_Handle *handle);
NS_EXTERN int Dbi_ResetHandle(Dbi_Handle *handle);
NS_EXTERN char *Dbi_TableList(Ns_DString *ds, Dbi_Handle *handle, int incsys);
NS_EXTERN Dbi_TableInfo *Dbi_GetTableInfo(Ns_DString *ds, Dbi_Handle *handle, char *table);
NS_EXTERN char *Dbi_BestRow(Ns_DString *ds, Dbi_Handle *handle, char *table);

/*
 * init.c:
 */

NS_EXTERN char *Dbi_PoolDescription(char *pool);
NS_EXTERN char *Dbi_PoolDefault(char *server);
NS_EXTERN char *Dbi_PoolDataSource(char *pool);
NS_EXTERN char *Dbi_PoolDbType(char *pool);
NS_EXTERN char *Dbi_PoolDriverName(char *pool);
NS_EXTERN int Dbi_PoolNHandles(char *pool);
NS_EXTERN char *Dbi_PoolPassword(char *pool);
NS_EXTERN char *Dbi_PoolUser(char *pool);
NS_EXTERN char *Dbi_PoolList(char *server);
NS_EXTERN int Dbi_PoolAllowable(char *server, char *pool);
NS_EXTERN void Dbi_PoolPutHandle(Dbi_Handle *handle);
NS_EXTERN Dbi_Handle *Dbi_PoolTimedGetHandle(char *server, char *pool, int wait);
NS_EXTERN Dbi_Handle *Dbi_PoolGetHandle(char *server, char *pool);
NS_EXTERN int Dbi_PoolGetMultipleHandles(Dbi_Handle **handles, char *server,
                                         char *pool, int nwant);
NS_EXTERN int Dbi_PoolTimedGetMultipleHandles(Dbi_Handle **handles, char *server,
                                              char *pool, int nwant, int wait);
NS_EXTERN int Dbi_BouncePool(char *pool);

/*
 * util.c:
 */

NS_EXTERN void Dbi_QuoteValue(Ns_DString *pds, char *string);
NS_EXTERN Ns_Set *Dbi_0or1Row(Dbi_Handle *handle, char *sql, int *nrows);
NS_EXTERN Ns_Set *Dbi_1Row(Dbi_Handle *handle, char *sql);
NS_EXTERN int Dbi_InterpretSqlFile(Dbi_Handle *handle, char *filename);
NS_EXTERN void Dbi_SetException(Dbi_Handle *handle, char *code, char *msg);
NS_EXTERN Dbi_TableInfo *Dbi_NewTableInfo(char *table);
NS_EXTERN void Dbi_FreeTableInfo(Dbi_TableInfo *tinfo);
NS_EXTERN void Dbi_AddColumnInfo(Dbi_TableInfo *tinfo, Ns_Set *cinfo);
NS_EXTERN int Dbi_ColumnIndex(Dbi_TableInfo *tinfo, char *name);

#endif /* NSDBI_H */
