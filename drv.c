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
 * drv.c --
 *
 *	Routines for handling the loadable db driver interface.
 */

#include "dbi.h"

NS_RCSID("@(#) $Header$");

/*
 * The following typedefs define the functions provided by
 * loadable drivers.
 */

typedef int (InitProc) (char *server, char *module, char *driver);
typedef char *(NameProc) (Dbi_Handle *);
typedef char *(TypeProc) (Dbi_Handle *);
typedef int (OpenProc) (Dbi_Handle *);
typedef void (CloseProc) (Dbi_Handle *);
typedef int (DMLProc) (Dbi_Handle *, char *sql);
typedef Ns_Set *(SelectProc) (Dbi_Handle *, char *sql);
typedef int (ExecProc) (Dbi_Handle *, char *sql);
typedef Ns_Set *(BindProc) (Dbi_Handle *);
typedef int (GetProc) (Dbi_Handle *, Ns_Set *);
typedef int (FlushProc) (Dbi_Handle *);
typedef int (CancelProc) (Dbi_Handle *);
typedef int (ResetProc) (Dbi_Handle *);
typedef int (SpStartProc) (Dbi_Handle *handle, char *procname);
typedef int (SpSetParamProc) (Dbi_Handle *handle, char *args);
typedef int (SpExecProc) (Dbi_Handle *handle);
typedef int (SpReturnCodeProc) (Dbi_Handle *dbhandle, char *returnCode,
				int bufsize);
typedef Ns_Set *(SpGetParamsProc) (Dbi_Handle *handle);

/*
 * The following structure specifies the driver-specific functions
 * to call for each Dbi_ routine.
 */

typedef struct DbiDriver {
    char	*name;
    int		 registered;
    InitProc	*initProc;
    NameProc	*nameProc;
    TypeProc	*typeProc;
    OpenProc	*openProc;
    CloseProc 	*closeProc;
    DMLProc 	*dmlProc;
    SelectProc	*selectProc;
    ExecProc	*execProc;
    BindProc	*bindProc;
    GetProc 	*getProc;
    FlushProc	*flushProc;
    CancelProc	*cancelProc;
    ResetProc	*resetProc;
    SpStartProc *spstartProc;
    SpSetParamProc   *spsetparamProc;
    SpExecProc       *spexecProc;
    SpReturnCodeProc *spreturncodeProc;
    SpGetParamsProc  *spgetparamsProc;
} DbiDriver;
    
/*
 * Static variables defined in this file
 */

static Tcl_HashTable driversTable;


/*
 *----------------------------------------------------------------------
 *
 * Dbi_RegisterDriver --
 *
 *	Register dbi procs for a driver.  This routine is called by
 *	driver modules when loaded.
 *
 * Results:
 *	NS_OK if procs registered, NS_ERROR otherwise.
 *
 * Side effects:
 *	Driver structure is allocated and function pointers are set
 *	to the given array of procs.
 *
 *----------------------------------------------------------------------
 */

static void
UnsupProcId(char *name)
{
    Ns_Log(Warning, "dbidrv: unsupported function id '%s'", name);
}

int
Dbi_RegisterDriver(char *driver, Dbi_Proc *procs)
{
    Tcl_HashEntry *hPtr;
    DbiDriver *driverPtr = NULL;

    hPtr = Tcl_FindHashEntry(&driversTable, driver);
    if (hPtr == NULL) {
        Ns_Log(Error, "dbidrv: no such driver '%s'", driver);
	return NS_ERROR;
    }
    driverPtr = (DbiDriver *) Tcl_GetHashValue(hPtr);
    if (driverPtr->registered) {
        Ns_Log(Error, "dbidrv: a driver is already registered as '%s'",
	       driver);
        return NS_ERROR;
    }
    driverPtr->registered = 1;
    
    while (procs->func != NULL) {
	switch (procs->id) {
	    case DbFn_ServerInit:
		driverPtr->initProc = (InitProc *) procs->func;
		break;
	    case DbFn_Name:
		driverPtr->nameProc = (NameProc *) procs->func;
		break;
	    case DbFn_DbType:
		driverPtr->typeProc = (TypeProc *) procs->func;
		break;
	    case DbFn_OpenDb:
		driverPtr->openProc = (OpenProc *) procs->func;
		break;
	    case DbFn_CloseDb:
		driverPtr->closeProc = (CloseProc *) procs->func;
		break;
	    case DbFn_DML:
		driverPtr->dmlProc = (DMLProc *) procs->func;
		break;
	    case DbFn_Select:
		driverPtr->selectProc = (SelectProc *) procs->func;
		break;
	    case DbFn_GetRow:
		driverPtr->getProc = (GetProc *) procs->func;
		break;
	    case DbFn_Flush:
		driverPtr->flushProc = (FlushProc *) procs->func;
		break;
	    case DbFn_Cancel:
		driverPtr->cancelProc = (CancelProc *) procs->func;
		break;
	    case DbFn_Exec:
		driverPtr->execProc = (ExecProc *) procs->func;
		break;
	    case DbFn_BindRow:
		driverPtr->bindProc = (BindProc *) procs->func;
		break;
	    case DbFn_ResetHandle:
		driverPtr->resetProc = (ResetProc *) procs->func;
		break;

	    case DbFn_SpStart:
		driverPtr->spstartProc = (SpStartProc *) procs->func;
		break;
		
	    case DbFn_SpSetParam:
		driverPtr->spsetparamProc = (SpSetParamProc *) procs->func;
		break;

	    case DbFn_SpExec:
		driverPtr->spexecProc = (SpExecProc *) procs->func;
		break;

	    case DbFn_SpReturnCode:
		driverPtr->spreturncodeProc = (SpReturnCodeProc *) procs->func;
		break;

	    case DbFn_SpGetParams:
		driverPtr->spgetparamsProc = (SpGetParamsProc *) procs->func;
		break;

	    /*
	     * The following functions are no longer supported.
	     */

	    case DbFn_End:
		UnsupProcId("End");
		break;
		
	    case DbFn_GetTableInfo:
		UnsupProcId("GetTableInfo");
		break;
		
	    case DbFn_TableList:
		UnsupProcId("TableList");
		break;
		
	    case DbFn_BestRowId:
		UnsupProcId("BestRowId");
		break;
		
	    default:
		Ns_Log(Error, "dbidrv: unknown driver id '%d'", procs->id);
		return NS_ERROR;
		break;
	}
	++procs;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DriverName --
 *
 *	Return the string name of the driver.
 *
 * Results:
 *	String name.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_DriverName(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    char *name = NULL;

    if (driverPtr != NULL && driverPtr->nameProc != NULL) {

    	name = (*driverPtr->nameProc)(handle);
    }

    return name;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DriverType --
 *
 *	Return the string name of the database type (e.g., "sybase").
 *
 * Results:
 *	String name.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

char *
Dbi_DriverDbType(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    
    if (driverPtr == NULL ||
	driverPtr->typeProc == NULL ||
	handle->connected == NS_FALSE) {

    	return NULL;
    }

    return (*driverPtr->typeProc)(handle);
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_DML --
 *
 *	Execute an SQL statement which is expected to be DML.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_DML(Dbi_Handle *handle, char *sql)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (driverPtr != NULL && handle->connected) {

	if (driverPtr->execProc != NULL) {
    	    status = Dbi_Exec(handle, sql);
	    if (status == DBI_DML) {
		status = NS_OK;
	    } else {
		if (status == DBI_ROWS) {
        	    Dbi_SetException(handle, "DBI",
		    	    "Query was not a DML or DDL command.");
        	    Dbi_Flush(handle);
		}
		status = NS_ERROR;
	    }
	} else if (driverPtr->dmlProc != NULL) {
    	    status = (*driverPtr->dmlProc)(handle, sql);
	    DbiLogSql(handle, sql);
	}
    }
    
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Select --
 *
 *	Execute an SQL statement which is expected to return rows.
 *
 * Results:
 *	Pointer to Ns_Set of selected columns or NULL on error.
 *
 * Side effects:
 *	SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

Ns_Set *
Dbi_Select(Dbi_Handle *handle, char *sql)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    Ns_Set *setPtr = NULL;

    if (driverPtr != NULL && handle->connected) {

	if (driverPtr->execProc != NULL) {
    	    if (Dbi_Exec(handle, sql) == NS_ROWS) {
    		setPtr = Dbi_BindRow(handle);
	    } else {
		if(!handle->dsExceptionMsg.length)
        	   Dbi_SetException(handle, "NSDB",
		    	"Query was not a statement returning rows.");
	    }
	} else if (driverPtr->selectProc != NULL) {
    	    Ns_SetTrunc(handle->row, 0);
    	    setPtr = (*driverPtr->selectProc)(handle, sql);	
	    Dbi_LogSql(handle, sql);
	}
    }
    
    return setPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Exec --
 *
 *	Execute an SQL statement.
 *
 * Results:
 *	DBI_DML, DBI_ROWS, or NS_ERROR.
 *
 * Side effects:
 *	SQL is sent to database for evaluation.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Exec(Dbi_Handle *handle, char *sql)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;
    
    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->execProc != NULL) {

    	status = (*driverPtr->execProc)(handle, sql);
	DbiLogSql(handle, sql);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_BindRow --
 *
 *	Bind the column names from a pending result set.  This routine
 *	is normally called right after an Dbi_Exec if the result
 *	was DBI_ROWS.
 *
 * Results:
 *	Pointer to Ns_Set.
 *
 * Side effects:
 *	Column names of result rows are set in the Ns_Set. 
 *
 *----------------------------------------------------------------------
 */

Ns_Set *
Dbi_BindRow(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    Ns_Set *setPtr = NULL;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->bindProc != NULL) {

    	Ns_SetTrunc(handle->row, 0);
    	setPtr = (*driverPtr->bindProc)(handle);
    }
    
    return setPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_GetRow --
 *
 *	Fetch the next row waiting in a result set.  This routine
 *	is normally called repeatedly after an Dbi_Select or
 *	an Dbi_Exec and Dbi_BindRow.
 *
 * Results:
 *	DBI_END_DATA if there are no more rows, NS_OK or NS_ERROR
 *	otherwise.
 *
 * Side effects:
 *	The values of the given set are filled in with those of the
 *	next row.
 *
 *----------------------------------------------------------------------
 */
int
Dbi_GetRow(Dbi_Handle *handle, Ns_Set *row)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->getProc != NULL) {

    	status = (*driverPtr->getProc)(handle, row);
    }
    
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Flush --
 *
 *	Flush rows pending in a result set.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Rows waiting in the result set are dumped, perhaps by simply
 *	fetching them over one by one.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Flush(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->flushProc != NULL) {

    	status = (*driverPtr->flushProc)(handle);
    }
    
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_Cancel --
 *
 *	Cancel the execution of a select and dump pending rows.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Depending on the driver, a running select call which executes
 *	as rows are fetched may be interrupted.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_Cancel(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->cancelProc != NULL) {

    	status = (*driverPtr->cancelProc)(handle);
    }
    
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ResetHandle --
 *
 *	Reset a handle after a cancel operation.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Handle is available for new commands.
 *
 *----------------------------------------------------------------------
 */

int
Dbi_ResetHandle (Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->resetProc != NULL) {

    	status = (*driverPtr->resetProc)(handle);
    }
    
    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiLoadDriver --
 *
 *	Load a database driver for one or more pools.
 *
 * Results:
 *	Pointer to driver structure or NULL on error.
 *
 * Side effects:
 *	Driver module file may be mapped into the process.
 *
 *----------------------------------------------------------------------
 */

struct DbiDriver *
DbiLoadDriver(char *driver)
{
    Tcl_HashEntry  *hPtr;
    char           *module, *path;
    int             new;
    DbiDriver	   *driverPtr;
    static int 	    initialized = NS_FALSE;

    if (initialized == NS_FALSE) {
	Tcl_InitHashTable(&driversTable, TCL_STRING_KEYS);
	initialized = NS_TRUE;
    }

    hPtr = Tcl_CreateHashEntry(&driversTable, driver, &new);
    if (new == 0) {
	driverPtr = (DbiDriver *) Tcl_GetHashValue(hPtr);
    } else {
	driverPtr = ns_malloc(sizeof(DbiDriver));
	memset(driverPtr, 0, sizeof(DbiDriver));
	driverPtr->name = Tcl_GetHashKey(&driversTable, hPtr);
        Tcl_SetHashValue(hPtr, driverPtr);
        module = Ns_ConfigGetValue("ns/dbi/drivers", driver);
        if (module == NULL) {
	    Ns_Log(Error, "dbidrv: no such driver '%s'", driver);
	} else {
	    path = Ns_ConfigGetPath(NULL, NULL, "dbi", "driver", driver, NULL);
            if (Ns_ModuleLoad(driver, path, module, "Dbi_DriverInit")
		    != NS_OK) {
		Ns_Log(Error, "dbidrv: failed to load driver '%s'",
		       driver);
            }
        }
    }
    if (driverPtr->registered == 0) {
	return NULL;
    }

    return driverPtr;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiServerInit --
 *
 *	Invoke driver provided server init proc (e.g., to add driver
 *	specific Tcl commands).
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

void
DbiDriverInit(char *server, DbiDriver *driverPtr)
{
    if (driverPtr->initProc != NULL &&
	((*driverPtr->initProc) (server, "dbi", driverPtr->name)) != NS_OK) {

	Ns_Log(Warning, "dbidrv: init proc failed for driver '%s'",
	       driverPtr->name);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * DbiOpen --
 *
 *	Open a connection to the database.  This routine is called
 *	from the pool routines in dbiinit.c.
 *
 * Results:
 *	NS_OK or NS_ERROR.
 *
 * Side effects:
 *	Database may be connected by driver specific routine.
 *
 *----------------------------------------------------------------------
 */

int
DbiOpen(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);

    Ns_Log(Notice, "dbidrv: opening database '%s:%s'", handle->driver,
	   handle->datasource);
    if (driverPtr == NULL ||
	driverPtr->openProc == NULL ||
	(*driverPtr->openProc) (handle) != NS_OK) {

	Ns_Log(Error, "dbidrv: failed to open database '%s:%s'",
	       handle->driver, handle->datasource);
	handle->connected = NS_FALSE;
        return NS_ERROR;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * DbiClose --
 *
 *	Close a connection to the database.  This routine is called
 *	from the pool routines in dbiinit.c
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

void
DbiClose(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    
    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->closeProc != NULL) {

    	(*driverPtr->closeProc)(handle);
    }
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SpStart --
 *
 *	Start execution of a stored procedure. 
 *
 * Results:
 *	NS_OK/NS_ERROR. 
 *
 * Side effects:
 *	Begins an SP; see Dbi_SpExec. 
 *
 *----------------------------------------------------------------------
 */

int
Dbi_SpStart(Dbi_Handle *handle, char *procname)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int status = NS_ERROR;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->spstartProc != NULL) {

	status = (*driverPtr->spstartProc)(handle, procname);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SpSetParam --
 *
 *	Set a parameter in a store procedure; must have executed 
 *	Dbi_SpStart first. paramname looks like "@x", paramtype is 
 *	like "int" or "varchar", inout is "in" or "out", value is 
 *	like "123". 
 *
 * Results:
 *	NS_OK/NS_ERROR 
 *
 * Side effects:
 *	None. 
 *
 *----------------------------------------------------------------------
 */

int
Dbi_SpSetParam(Dbi_Handle *handle, char *paramname, char *paramtype,
                char *inout, char *value)
{
    DbiDriver   *driverPtr = DbiGetDriver(handle);
    int         status = NS_ERROR;
    Ns_DString  args;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->spsetparamProc != NULL) {

	Ns_DStringInit(&args);
	Ns_DStringVarAppend(&args, paramname, " ", paramtype, " ", inout, " ",
			    value, NULL);
	status = (*driverPtr->spsetparamProc)(handle, args.string);
	Ns_DStringFree(&args);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SpExec --
 *
 *	Run an Sp begun with Dbi_SpStart 
 *
 * Results:
 *	NS_OK/NS_ERROR 
 *
 * Side effects:
 *	None. 
 *
 *----------------------------------------------------------------------
 */

int
Dbi_SpExec(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int       status = NS_ERROR;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->spexecProc != NULL) {

	status = (*driverPtr->spexecProc)(handle);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SpReturnCode --
 *
 *	Get the return code from an SP after Dbi_SpExec 
 *
 * Results:
 *	NS_OK/NSERROR 
 *
 * Side effects:
 *	The return code is put into the passed-in buffer, which must 
 *	be at least bufsize in length. 
 *
 *----------------------------------------------------------------------
 */

int
Dbi_SpReturnCode(Dbi_Handle *handle, char *returnCode, int bufsize)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    int       status = NS_ERROR;

    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->spreturncodeProc != NULL) {

	status = (*driverPtr->spreturncodeProc)(handle, returnCode, bufsize);
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Dbi_SpGetParams --
 *
 *	Get output parameters after running an SP w/ Dbi_SpExec. 
 *
 * Results:
 *	NULL or a newly allocated set with output params in it. 
 *
 * Side effects:
 *	Allocs its return value and its members. 
 *
 *----------------------------------------------------------------------
 */

Ns_Set *
Dbi_SpGetParams(Dbi_Handle *handle)
{
    DbiDriver *driverPtr = DbiGetDriver(handle);
    Ns_Set   *aset = NULL;

    Ns_SetTrunc(handle->row, 0);
    if (handle->connected &&
	driverPtr != NULL &&
	driverPtr->spgetparamsProc != NULL) {

	aset = (*driverPtr->spgetparamsProc)(handle);
    }

    return aset;
}
