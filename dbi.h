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

extern void DbiInitPools(void);
extern void DbiInitServer(char *server);
extern Ns_TclTraceProc DbiAddCmds, DbiReleaseHandles;
extern void 		DbiClose(Dbi_Handle *);
extern void 		DbiDisconnect(Dbi_Handle *);
extern struct DbiDriver *DbiGetDriver(Dbi_Handle *);
extern struct DbiDriver *DbiLoadDriver(char *driver);
extern void 		DbiLogSql(Dbi_Handle *, char *sql);
extern int 		DbiOpen(Dbi_Handle *);
extern void 		DbiDriverInit(char *server, struct DbiDriver *);

#endif
