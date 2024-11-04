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
 */

#ifndef NSDBI_H
#define NSDBI_H

#include <ns.h>


/*
 * The following defines the maximum number of bind variables
 * which may appear in a single statement.
 */

#define DBI_MAX_BIND 32
#define DBI_NUM_ROWS_UNKNOWN -1

/*
 * The following define SQL transaction isolation levels.
 */

typedef enum {
    Dbi_ReadUncommitted = 0,
    Dbi_ReadCommitted,
    Dbi_RepeatableRead,
    Dbi_Serializable
} Dbi_Isolation;

/*
 * The following define quoting levels for templating in dbi_rows
 */

typedef enum {
    Dbi_QuoteNone = 0,
    Dbi_QuoteHTML,
    Dbi_QuoteJS
} Dbi_quotingLevel;

/*
 * The following define output formats for dbi_rows
 */

typedef enum {
    Dbi_ResultFlatList = 0,
    Dbi_ResultSets,
    Dbi_ResultDicts,
    Dbi_ResultAvLists,
    Dbi_ResultDict,
    Dbi_ResultLists
} Dbi_resultFormat;


/*
 * The following are valid configuration options for a
 * pool of handles.
 */

typedef enum {
    DBI_CONFIG_MAXHANDLES = 0,
    DBI_CONFIG_MAXROWS,
    DBI_CONFIG_MAXIDLE,
    DBI_CONFIG_MAXOPEN,
    DBI_CONFIG_MAXQUERIES,
    DBI_CONFIG_TIMEOUT
} DBI_CONFIG_OPTION;


/*
 * The following defines an opaque pool handle.
 */

typedef struct _Dbi_Pool *Dbi_Pool;

/*
 * The following structure defines a handle in a pool.
 */

typedef struct Dbi_Handle {
    Dbi_Pool           *pool;        /* The pool this handle belongs to. */
    unsigned int        rowIdx;      /* The current row of the result set. */
    ClientData          driverData;  /* Driver private handle context. */
    int                 numRowsHint; /* Rows affected in a DBI_Exec() operation */
} Dbi_Handle;

/*
 * The following structure defines a single result value.
 */

typedef struct Dbi_Value {
    const char   *data;  /* NULL for null SQL values. */
    size_t        length;   /* Length of data in bytes. */
    int           binary;   /* 1 if data is binary, utf8 otherwise. */
} Dbi_Value;


/*
 * Library initialization.
 */

NS_EXTERN void
Dbi_LibInit(void);

NS_EXTERN Ns_TclTraceProc DbiInitInterp;

/*
 * Functions for acquiring handles from pools.
 */

NS_EXTERN Dbi_Pool *
Dbi_DefaultPool(const char *server)
    NS_GNUC_NONNULL(1);

NS_EXTERN Dbi_Pool *
Dbi_GetPool(const char *server, const char *poolname)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ListPools(Ns_DString *ds, const char *server)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN int
Dbi_GetHandle(Dbi_Pool *pool, Ns_Time *timeoutPtr, Dbi_Handle **handlePtrPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);

NS_EXTERN void
Dbi_PutHandle(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_Reset(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

/*
 * Functions for preparing and describing queries.
 */

NS_EXTERN int
Dbi_Prepare(Dbi_Handle *handle, const char *sql, TCL_SIZE_T length)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN unsigned int
Dbi_NumVariables(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_VariableName(Dbi_Handle *handle, unsigned int index, const char **namePtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);

NS_EXTERN unsigned int
Dbi_NumColumns(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ColumnName(Dbi_Handle *handle, unsigned int index, const char **namePtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);


/*
 * Functions for executing queries.
 */

NS_EXTERN int
Dbi_Exec(Dbi_Handle *handle, Dbi_Value *values, int maxRows)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ExecDirect(Dbi_Handle *handle, const char *sql)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN int
Dbi_NextRow(Dbi_Handle *handle, int *endPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN int
Dbi_ColumnLength(Dbi_Handle *handle, unsigned int index,
                 size_t *lengthPtr, int *binaryPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3) NS_GNUC_NONNULL(4);

NS_EXTERN int
Dbi_ColumnValue(Dbi_Handle *handle, unsigned int index,
                char *value, size_t size)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(3);

NS_EXTERN void
Dbi_Flush(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

/*
 * Functions for managing transactions.
 */

NS_EXTERN int
Dbi_Begin(Dbi_Handle *handle, Dbi_Isolation isolation)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_Commit(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_Rollback(Dbi_Handle *handle)
    NS_GNUC_NONNULL(1);

/*
 * Functions for managing pools.
 */

NS_EXTERN void
Dbi_BouncePool(Dbi_Pool *pool)
    NS_GNUC_NONNULL(1);

NS_EXTERN char *
Dbi_Stats(Ns_DString *ds, Dbi_Pool *poolPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN const char *
Dbi_PoolName(Dbi_Pool *pool)
    NS_GNUC_NONNULL(1);

NS_EXTERN const char *
Dbi_DriverName(Dbi_Pool *pool)
    NS_GNUC_NONNULL(1);

NS_EXTERN const char *
Dbi_DatabaseName(Dbi_Pool *pool)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ConfigInt(Dbi_Pool *pool, DBI_CONFIG_OPTION opt, int newValue)
    NS_GNUC_NONNULL(1);

NS_EXTERN void
Dbi_ConfigTime(Dbi_Pool *pool, DBI_CONFIG_OPTION opt, Ns_Time *newValue, Ns_Time *oldValuePtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(4);

/*
 * Functions for dealing with exceptions.
 */

NS_EXTERN void
Dbi_SetException(Dbi_Handle *, const char *sqlstate, const char *fmt, ...)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_PRINTF(3, 4);

NS_EXTERN void
Dbi_ResetException(Dbi_Handle *)
     NS_GNUC_NONNULL(1);

NS_EXTERN char *
Dbi_ExceptionCode(Dbi_Handle *)
     NS_GNUC_NONNULL(1);

NS_EXTERN char *
Dbi_ExceptionMsg(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

NS_EXTERN int
Dbi_ExceptionPending(Dbi_Handle *)
     NS_GNUC_NONNULL(1);

NS_EXTERN void
Dbi_LogException(Dbi_Handle *, Ns_LogSeverity severity)
    NS_GNUC_NONNULL(1);

/*
 * Functions for Tcl commands.
 */

NS_EXTERN Dbi_Pool *
Dbi_TclGetPool(Tcl_Interp *interp, Tcl_Obj *poolObj)
    NS_GNUC_NONNULL(1);

NS_EXTERN Dbi_Handle *
Dbi_TclGetHandle(Tcl_Interp *interp, Dbi_Pool *pool, Ns_Time *timeoutPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN void
Dbi_TclPutHandle(Tcl_Interp *interp, Dbi_Handle *handle)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

NS_EXTERN int
Dbi_TclBindVariables(Tcl_Interp *interp, Dbi_Handle *handle,
                     Dbi_Value *dbValues, Tcl_Obj *tclValues, int autoNull)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

NS_EXTERN void
Dbi_TclErrorResult(Tcl_Interp *interp, Dbi_Handle *handle);


#endif /* NSDBI_H */
