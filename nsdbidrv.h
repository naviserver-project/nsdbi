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
 * nsdbidrv.h --
 *
 *      Public types and function declarations for the nsdbi module.
 *
 *  $Header$
 */

#ifndef NSDBIDRV_H
#define NSDBIDRV_H

#include "nsdbi.h"



/*
 * The following define the phases of a transaction that a driver
 * Dbi_TransactionProc must handle.
 */

typedef enum {
    Dbi_TransactionBegin,
    Dbi_TransactionCommit,
    Dbi_TransactionRollback
} Dbi_TransactionCmd;

/*
 * The following structure describes a statement to be executed one
 * or more times with it's associated handle.
 */

typedef struct Dbi_Statement {

    CONST char         *sql;        /* SQL to execute. */
    CONST int           length;     /* Length of SQL. */
    CONST unsigned int  id;         /* Unique (per handle) statement ID. */
    CONST unsigned int  nqueries;   /* Total queries for this statement. */
    ClientData          driverData; /* Driver private statement context. */

} Dbi_Statement;

/*
 * The following enum defines ids for calback functions
 * which a driver must implement.
 */

typedef enum {
    Dbi_OpenProcId = 1,
    Dbi_CloseProcId,
    Dbi_ConnectedProcId,
    Dbi_PrepareProcId,
    Dbi_PrepareCloseProcId,
    Dbi_BindVarProcId,
    Dbi_ExecProcId,
    Dbi_NextRowProcId,
    Dbi_ColumnLengthProcId,
    Dbi_ColumnValueProcId,
    Dbi_ColumnNameProcId,
    Dbi_TransactionProcId,
    Dbi_FlushProcId,
    Dbi_ResetProcId
} Dbi_ProcId;

/*
 * The following structure is used to register driver callbacks.
 */

typedef struct Dbi_DriverProc {
    Dbi_ProcId     id;
    void          *proc;
} Dbi_DriverProc;


/*
 * The following typedefs prototype the callbacks which must be
 * implemented by a database driver.
 */

typedef int
Dbi_OpenProc(ClientData configData, Dbi_Handle *)
    NS_GNUC_NONNULL(1);

typedef void
Dbi_CloseProc(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

typedef int
Dbi_ConnectedProc(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

typedef void
Dbi_BindVarProc(Ns_DString *, CONST char *name, int bindIdx)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

typedef int
Dbi_PrepareProc(Dbi_Handle *, Dbi_Statement *,
                unsigned int *numVarsPtr, unsigned int *numColsPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2)
    NS_GNUC_NONNULL(3) NS_GNUC_NONNULL(4);

typedef void
Dbi_PrepareCloseProc(Dbi_Handle *, Dbi_Statement *)
    NS_GNUC_NONNULL(1);

typedef int
Dbi_ExecProc(Dbi_Handle *, Dbi_Statement *,
             Dbi_Value *values, unsigned int numValues)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

typedef int
Dbi_NextRowProc(Dbi_Handle *, Dbi_Statement *,
                int *endPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(3);

typedef int
Dbi_ColumnLengthProc(Dbi_Handle *, Dbi_Statement *, unsigned int index,
                     size_t *lengthPtr, int *binaryPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2)
    NS_GNUC_NONNULL(4) NS_GNUC_NONNULL(5);

typedef int
Dbi_ColumnValueProc(Dbi_Handle *, Dbi_Statement *, unsigned int index,
                    char *value, size_t length)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(4);

typedef int
Dbi_ColumnNameProc(Dbi_Handle *, Dbi_Statement *, unsigned int index,
                   CONST char **columnPtr)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2) NS_GNUC_NONNULL(4);

typedef int
Dbi_TransactionProc(Dbi_Handle *, unsigned int depth,
                    Dbi_TransactionCmd cmd, Dbi_Isolation isolation)
    NS_GNUC_NONNULL(1);

typedef int
Dbi_FlushProc(Dbi_Handle *, Dbi_Statement *)
    NS_GNUC_NONNULL(1);

typedef int
Dbi_ResetProc(Dbi_Handle *)
    NS_GNUC_NONNULL(1);


/*
 * The following function is used to register driver callbacks
 * and create a pool of handles to the configured database.
 */

NS_EXTERN int
Dbi_RegisterDriver(CONST char *server, CONST char *module,
                   CONST char *driver, CONST char *database,
                   CONST Dbi_DriverProc *procs, ClientData configData)
     NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2)
     NS_GNUC_NONNULL(3) NS_GNUC_NONNULL(4)
     NS_GNUC_NONNULL(5);



#endif /* NSDBIDRV_H */
