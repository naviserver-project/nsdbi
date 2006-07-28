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

    char              *name;
    int                nhandles;
    Dbi_Driver        *driver;
    struct Handle     *firstPtr;
    struct Handle     *lastPtr;
    int                npresent;
    Ns_Mutex           lock;
    Ns_Cond            getCond;

    int                maxwait;
    time_t             maxidle;
    time_t             maxopen;
    int                maxqueries;
    int                stale_on_close;
    int                stopping;

    struct {
        unsigned int handlegets;
        unsigned int handlemisses;
        unsigned int handleopens;
        unsigned int handlefailures;
        unsigned int queries;
        unsigned int otimecloses;
        unsigned int atimecloses;
        unsigned int querycloses;
    } stats;

} Pool;


/*
 * The following structure defines the internal
 * state of a database handle.
 */

typedef struct Handle {

    /* Publicly visible in a Dbi_Handle. */

    struct Pool      *poolPtr;      /* The pool this handle belongs to. */
    void             *arg;          /* Driver private handle context. */

    /* Private to a Handle. */

    struct Handle    *nextPtr;      /* Next handle in the pool. */
    Ns_Conn          *conn;         /* Conn that handle is cached for, if any. */
    char              cExceptionCode[6];
    Ns_DString        dsExceptionMsg;
    time_t            otime;        /* Time when handle was connected to db. */
    time_t            atime;        /* Time when handle was last used. */
    int               n;            /* Handle n of nhandles when acquired. */
    int               stale_on_close;
    int               reason;       /* Why the handle is being disconnected. */

    struct {
        unsigned int queries;       /* Total queries via current connection. */
    } stats;

} Handle;

/*
 * The following structure tracks which pools are
 * available to a virtual server.
 */

typedef struct ServerData {
    CONST char    *server;
    Dbi_Pool      *defpoolPtr;
    Tcl_HashTable  poolsTable;
} ServerData;


/*
 * The following are some convenience macros to access structures etc.
 */

#define DbiPoolForHandle(handle)     (((Handle *) handle)->poolPtr)
#define DbiDriverForHandle(handle)   (((Handle *) handle)->poolPtr->driver)
#define DbiDriverForPool(pool)       (((Pool *) pool)->driver)

#define DbiPoolName(pool)            (((Pool *) pool)->name)
#define DbiPoolNameForHandle(handle) (((Handle *) handle)->poolPtr->name)

#define DbiLog(handle,level,msg,...)                   \
    Ns_Log(level, "nsdbi[%s]: " msg,                   \
           DbiPoolNameForHandle(handle), __VA_ARGS__)



extern Dbi_Pool *
DbiGetPool(ServerData *sdataPtr, CONST char *poolname)
    NS_GNUC_NONNULL(1) NS_GNUC_NONNULL(2);

extern ServerData *
DbiGetServer(CONST char *server)
    NS_GNUC_NONNULL(1);

extern int
DbiConnected(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

extern int
DbiOpen(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

extern void
DbiClose(Dbi_Handle *)
    NS_GNUC_NONNULL(1);

extern void DbiInitTclObjTypes(void);
extern Ns_TclInterpInitProc DbiInitInterp;


#endif
