#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "libpq-fe.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

extern Datum inject_fault(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(inject_fault);
Datum
inject_fault(PG_FUNCTION_ARGS)
{
	char	*faultName = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	*type = TextDatumGetCString(PG_GETARG_DATUM(1));
	char	*ddlStatement = TextDatumGetCString(PG_GETARG_DATUM(2));
	char	*databaseName = TextDatumGetCString(PG_GETARG_DATUM(3));
	char	*tableName = TextDatumGetCString(PG_GETARG_DATUM(4));
	int		startOccurrence = PG_GETARG_INT32(5);
	int		endOccurrence = PG_GETARG_INT32(6);
	int		extraArg = PG_GETARG_INT32(7);
	char	*response;

	response = InjectFault(
		faultName, type, ddlStatement, databaseName,
		tableName, startOccurrence, endOccurrence, extraArg);
	if (!response)
		elog(ERROR, "failed to inject fault");
	if (strncmp(response, "Success:",  strlen("Success:")) != 0)
		elog(ERROR, "%s", response);
	PG_RETURN_TEXT_P(cstring_to_text(response));
}
