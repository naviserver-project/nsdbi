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

#ifndef DBI_H
#define DBI_H

#include "nsdbi.h"


/*
 * The following structure defines a database pool.
 */

struct Handle;

typedef struct Pool {

    /* Publicly visible in a Dbi_Pool struct */

    Dbi_Driver        *driver;
    char              *name;
    char              *description;
    char              *datasource;
    char              *user;
    char              *password;
    int                nhandles;
    int                fVerbose;
    int                fVerboseError;

    /* Private to a Pool struct */

    struct Handle     *firstPtr;
    struct Handle     *lastPtr;
    int                npresent;
    Ns_Mutex           lock;
    Ns_Cond            getCond;
    int                cache_handles;

    int                maxwait;
    time_t             maxidle;
    time_t             maxopen;
    int                maxopps;
    int                stale_on_close;

    struct {
        unsigned int attempts;
        unsigned int successes;
        unsigned int misses;
        unsigned int opps;
        unsigned int otimecloses;
        unsigned int atimecloses;
        unsigned int oppscloses;
    } stats;

} Pool;


/*
 * The following structure defines the internal
 * state of a database handle.
 */

typedef struct Handle {

    /* Publicly visible in a Dbi_Handle struct */

    Pool             *poolPtr;
    int               connected;
    void             *arg;  /* Driver private connection context. */

    /* Private to a Handle struct */

    struct Handle    *nextPtr;
    struct Statement *stmtPtr;
    char              cExceptionCode[6];
    Ns_DString        dsExceptionMsg;
    time_t            otime;
    time_t            atime;
    int               n;      /* handle n of nhandles when acquired */
    int               stale_on_close;
    int               reason; /* why the handle is being disconnected */

    struct {
        unsigned int opps;
    } stats;

} Handle;


/*
 * The following structure defines an SQL statement.
 */

typedef struct Statement {

    /* Publicly visible in a Dbi_Statement struct */

    Pool             *poolPtr;
    Ns_DString        dsSql;
    int               fetchingRows;
    Tcl_HashTable     bindVars;
    void             *arg;  /* Driver private statement context. */

    /* Private to a Statement struct */

    Handle           *handlePtr;
    int               numCols;
    int               currentCol;
    int               numRows;
    int               currentRow;
} Statement;


/*
 * The following structure maintains per-server data.
 */

typedef struct ServerData {
    char          *server;
    Pool          *defpoolPtr;
    Tcl_HashTable  poolsTable;
} ServerData;



/*
 * init.c
 */

extern void DbiInitPools(void);
extern Dbi_Pool *DbiGetPool(ServerData *sdataPtr, CONST char *poolname) _nsnonnull();
extern void DbiInitServer(CONST char *server) _nsnonnull();
extern ServerData *DbiGetServer(CONST char *server) _nsnonnull();
extern void DbiLogSql(Dbi_Statement *) _nsnonnull();

/*
 * drv.c
 */

extern Dbi_Driver *DbiLoadDriver(char *drivername) _nsnonnull();
extern void DbiDriverInit(char *server, Dbi_Driver *driver) _nsnonnull();
extern int DbiOpen(Dbi_Handle *);
extern void DbiClose(Dbi_Handle *) _nsnonnull();

/*
 * tclcmds.c
 */

extern Ns_TclInterpInitProc DbiAddCmds;
extern Ns_TclInterpInitProc DbiAddTraces;

/*
 * tcltypes.c
 */

extern void DbiInitTclObjTypes(void);
extern Dbi_Pool *DbiGetPoolFromObj(Tcl_Interp *interp, ServerData *sdataPtr, Tcl_Obj *objPtr);


#endif
