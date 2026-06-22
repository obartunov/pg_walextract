/*
 * walextract_ext.c - in-server frontend: mine DDL/DML effects from WAL.
 * Reader pattern follows contrib/pg_walinspect (read_local_xlog_page_no_wait).
 * Output of walextract_core is routed into a tuplestore via walextract_set_emit.
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/array.h"
#include "utils/tuplestore.h"
#include "catalog/pg_type.h"
#include "catalog/pg_authid.h"
#include "utils/acl.h"

#include "walextract_core.h"

PG_MODULE_MAGIC;

typedef struct SinkCtx
{
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
} SinkCtx;

static void
sink_emit(const ChangeEvent *ev, void *sink)
{
	SinkCtx    *s = (SinkCtx *) sink;
	Datum		values[10];
	bool		nulls[10];
	int			i;

	memset(nulls, 0, sizeof(nulls));
	values[0] = LSNGetDatum(ev->record_lsn);
	values[1] = TransactionIdGetDatum(ev->xid);
	values[2] = ObjectIdGetDatum(ev->db_oid);
	values[3] = ObjectIdGetDatum(ev->rel_oid);
	values[4] = ObjectIdGetDatum(ev->relfilenode);
	values[5] = CStringGetTextDatum(walextract_op_name(ev->op));
	if (ev->relname)
		values[6] = CStringGetTextDatum(ev->relname);
	else
		nulls[6] = true;
	values[7] = BoolGetDatum(ev->complete);
	if (ev->nreasons > 0)
	{
		Datum		relems[WALEXTRACT_MAX_REASONS];

		for (i = 0; i < ev->nreasons; i++)
			relems[i] = CStringGetTextDatum(ev->reasons[i]);
		values[8] = PointerGetDatum(construct_array(relems, ev->nreasons,
													TEXTOID, -1, false, TYPALIGN_INT));
	}
	else
		nulls[8] = true;
	values[9] = CStringGetTextDatum(ev->op_text);
	tuplestore_putvalues(s->tupstore, s->tupdesc, values, nulls);
}

/* verbatim from pg_walinspect: build a reader over local cluster WAL */
static XLogReaderState *
InitXLogReaderState(XLogRecPtr lsn)
{
	XLogReaderState *xlogreader;
	ReadLocalXLogPageNoWaitPrivate *private_data;
	XLogRecPtr	first_valid_record;
	char	   *errormsg;

	if (lsn < XLOG_BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not read WAL at LSN %X/%08X",
						LSN_FORMAT_ARGS(lsn))));

	private_data = palloc0_object(ReadLocalXLogPageNoWaitPrivate);

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									private_data);
	if (xlogreader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	first_valid_record = XLogFindNextRecord(xlogreader, lsn, &errormsg);
	if (!XLogRecPtrIsValid(first_valid_record))
		ereport(ERROR,
				errmsg("could not find a valid record after %X/%08X",
					   LSN_FORMAT_ARGS(lsn)));

	return xlogreader;
}

static XLogRecord *
ReadNextXLogRecord(XLogReaderState *xlogreader)
{
	XLogRecord *record;
	char	   *errormsg;

	record = XLogReadRecord(xlogreader, &errormsg);
	if (record == NULL)
	{
		ReadLocalXLogPageNoWaitPrivate *private_data;

		private_data = (ReadLocalXLogPageNoWaitPrivate *) xlogreader->private_data;
		if (private_data->end_of_wal)
			return NULL;
		if (errormsg)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read WAL at %X/%08X: %s",
							LSN_FORMAT_ARGS(xlogreader->EndRecPtr), errormsg)));
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read WAL at %X/%08X",
							LSN_FORMAT_ARGS(xlogreader->EndRecPtr))));
	}
	return record;
}

PG_FUNCTION_INFO_V1(walextract_wal2sql);
Datum
walextract_wal2sql(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);
	XLogRecPtr	curr_flush;
	XLogReaderState *xlogreader;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SinkCtx		sc;
	WalExtractContext *wectx;

	/*
	 * This function reconstructs user table data from WAL.  Restrict it the
	 * same way pg_walinspect restricts WAL inspection: roles with privileges
	 * of pg_read_server_files (superusers inherit this).
	 */
	if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to mine WAL"),
				 errdetail("Only roles with privileges of the \"pg_read_server_files\" role may use this function.")));

	curr_flush = GetFlushRecPtr(NULL);
	if (end_lsn == InvalidXLogRecPtr || end_lsn > curr_flush)
		end_lsn = curr_flush;
	if (start_lsn > end_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("start_lsn %X/%08X is greater than end_lsn %X/%08X",
						LSN_FORMAT_ARGS(start_lsn), LSN_FORMAT_ARGS(end_lsn))));

	InitMaterializedSRF(fcinfo, 0);
	sc.tupstore = rsinfo->setResult;
	sc.tupdesc = rsinfo->setDesc;

	/* in-cluster decode: fresh context per call; reads our own PGDATA */
	wectx = walextract_context_create();
	if (wectx == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	walextract_set_emit(wectx, sink_emit, &sc);
	walextract_set_pgdata(wectx, DataDir);
	walextract_set_bootstrap(wectx, false);
	walextract_set_target_db(wectx, MyDatabaseId);

	xlogreader = InitXLogReaderState(start_lsn);
	while (ReadNextXLogRecord(xlogreader) &&
		   xlogreader->EndRecPtr <= end_lsn)
	{
		CHECK_FOR_INTERRUPTS();
		walextract_record(wectx, xlogreader);
	}

	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);
	walextract_context_free(wectx);
	return (Datum) 0;
}
