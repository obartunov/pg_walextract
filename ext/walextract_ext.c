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
#include "port/pg_bitutils.h"
#include "executor/spi.h"
#include "utils/timestamp.h"

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
	Datum		values[11];
	bool		nulls[11];
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
	if (ev->commit_lsn == InvalidXLogRecPtr)
		nulls[10] = true;		/* not delivered via a COMMIT (should not happen) */
	else
		values[10] = LSNGetDatum(ev->commit_lsn);
	tuplestore_putvalues(s->tupstore, s->tupdesc, values, nulls);
}

/* ===================== machine-path (ChangeBatch) harness ===================== */

typedef struct BatchSinkCtx
{
	Tuplestorestate *tupstore;	/* detail mode only */
	TupleDesc	tupdesc;
	bool		detail;			/* true: emit per-batch rows; false: accumulate only */
	int64		n_batches;
	int64		total_rows;
	int64		total_payload;	/* bytes copied into batch arenas */
	int64		total_nulls;	/* null cells (row x col) */
	bool		any_toast;
} BatchSinkCtx;

/* count set null bits in a vector over [0, nrows) */
static int64
vec_null_count(const ChangeVector *v, int nrows)
{
	int64		c = 0;
	int			full = nrows / 64;
	int			rem = nrows % 64;
	int			w;

	for (w = 0; w < full; w++)
		c += pg_popcount64(v->nullbits[w]);
	if (rem)
		c += pg_popcount64(v->nullbits[full] & (((uint64) 1 << rem) - 1));
	return c;
}

/* bytes actually copied for the batch, and total null cells */
static void
batch_metrics(const ChangeBatch *b, int64 *payload, int64 *nulls)
{
	int64		p = 0;
	int64		n = 0;
	int			j;

	if (!b->schema_missing && b->cols != NULL)
	{
		for (j = 0; j < b->ncols; j++)
		{
			const ChangeVector *v = &b->cols[j];
			int64		vnull = vec_null_count(v, b->nrows);

			n += vnull;
			if (v->attlen > 0)
				p += (int64) (b->nrows - vnull) * v->attlen;	/* fixed: non-null * len */
			else
				p += v->varoff ? (int64) v->varoff[b->nrows] : 0;	/* varlena: blob len */
		}
	}
	*payload = p;
	*nulls = n;
}

static void
batch_sink(const ChangeBatch *b, void *sink)
{
	BatchSinkCtx *s = (BatchSinkCtx *) sink;
	int64		payload;
	int64		nulls;

	batch_metrics(b, &payload, &nulls);
	s->n_batches++;
	s->total_rows += b->nrows;
	s->total_payload += payload;
	s->total_nulls += nulls;
	if (b->has_external_toast)
		s->any_toast = true;

	if (s->detail)
	{
		Datum		values[11];
		bool		isnull[11];

		memset(isnull, 0, sizeof(isnull));
		values[0] = LSNGetDatum(b->record_lsn);
		if (b->commit_lsn == InvalidXLogRecPtr)
			isnull[1] = true;
		else
			values[1] = LSNGetDatum(b->commit_lsn);
		values[2] = TransactionIdGetDatum(b->xid);
		values[3] = ObjectIdGetDatum(b->relfilenode);
		values[4] = ObjectIdGetDatum(b->rel_oid);
		values[5] = Int32GetDatum(b->nrows);
		values[6] = Int32GetDatum(b->ncols);
		values[7] = BoolGetDatum(b->schema_missing);
		values[8] = BoolGetDatum(b->has_external_toast);
		values[9] = Int64GetDatum(payload);
		values[10] = Int64GetDatum(nulls);
		tuplestore_putvalues(s->tupstore, s->tupdesc, values, isnull);
	}
}

/*
 * Range-start dictionary priming v0: seed the dictionary from the CURRENT
 * database's live catalogs before decoding.  Valid only when current catalog
 * state corresponds to the requested range (same relfilenode, not rewritten /
 * dropped).  Startup cost only -- never per-row.
 */
/*
 * Shared identity resolver for WX_SIDECAR 2.  Runs one SPI query that resolves,
 * per ordinary table, the replica-identity descriptor and the resolved
 * identity-key attnums, and feeds EXACTLY ONE sink:
 *   out != NULL  -> append "I <relfile> <relreplident> <natts> <att..>" lines
 *                   (sidecar producer);
 *   ctx != NULL  -> walextract_prime_identity() (live-prime).
 * Using one query for both paths is what makes live-prime and a v2 sidecar
 * classify identically for the same catalog state.  Caller holds an open SPI
 * connection.  relreplident resolution: 'd' -> primary-key columns (none if no
 * valid PK), 'i' -> replica-identity index columns, 'f'/'n' -> none.
 */
static void
walextract_collect_identity(StringInfo out, WalExtractContext *ctx)
{
	const char *q =
		"SELECT c.relfilenode, c.relreplident, "
		"COALESCE(( "
		"  SELECT string_agg(u.k::text, ' ' ORDER BY u.ord) "
		"  FROM pg_catalog.pg_index x "
		"  CROSS JOIN LATERAL unnest(x.indkey::int2[]) WITH ORDINALITY AS u(k, ord) "
		"  WHERE x.indrelid = c.oid AND x.indisvalid AND x.indisready AND u.k > 0 "
		"    AND ((c.relreplident = 'd' AND x.indisprimary) "
		"         OR (c.relreplident = 'i' AND x.indisreplident)) "
		"), '') AS atts "
		"FROM pg_catalog.pg_class c "
		"WHERE c.relkind = 'r' AND c.relfilenode <> 0";
	uint64		i;

	if (SPI_execute(q, true, 0) != SPI_OK_SELECT)
		return;

	for (i = 0; i < SPI_processed; i++)
	{
		TupleDesc	td = SPI_tuptable->tupdesc;
		HeapTuple	t = SPI_tuptable->vals[i];
		bool		isnull;
		Oid			relfile = DatumGetObjectId(SPI_getbinval(t, td, 1, &isnull));
		char		relreplident = DatumGetChar(SPI_getbinval(t, td, 2, &isnull));
		char	   *atts_str = SPI_getvalue(t, td, 3);
		int16		atts[WX_IDENT_MAXATTS];
		int			natts = 0;
		char	   *p = atts_str;

		while (p != NULL && natts < WX_IDENT_MAXATTS)
		{
			int			v;
			int			n2 = 0;

			while (*p == ' ')
				p++;
			if (*p == '\0')
				break;
			if (sscanf(p, "%d%n", &v, &n2) != 1 || n2 == 0)
				break;
			atts[natts++] = (int16) v;
			p += n2;
		}

		if (out != NULL)
		{
			int			k;

			appendStringInfo(out, "I %u %c %d", relfile, relreplident, natts);
			for (k = 0; k < natts; k++)
				appendStringInfo(out, " %d", atts[k]);
			appendStringInfoChar(out, '\n');
		}
		if (ctx != NULL)
			walextract_prime_identity(ctx, relfile, relreplident, atts, natts);
		if (atts_str)
			pfree(atts_str);
	}
}

static void
prime_from_catalog(WalExtractContext *ctx)
{
	uint64		i;

	if (SPI_connect() != SPI_OK_CONNECT)
		return;

	if (SPI_execute("SELECT relfilenode, oid, relkind, relname "
					"FROM pg_catalog.pg_class WHERE relfilenode <> 0", true, 0) == SPI_OK_SELECT)
	{
		TupleDesc	td = SPI_tuptable->tupdesc;

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	t = SPI_tuptable->vals[i];
			bool		isnull;
			Oid			relfile = DatumGetObjectId(SPI_getbinval(t, td, 1, &isnull));
			Oid			relid = DatumGetObjectId(SPI_getbinval(t, td, 2, &isnull));
			char		relkind = DatumGetChar(SPI_getbinval(t, td, 3, &isnull));
			char	   *relname = SPI_getvalue(t, td, 4);

			walextract_prime_rel(ctx, relfile, relid, relkind, relname);
			if (relname)
				pfree(relname);
		}
	}

	if (SPI_execute("SELECT attrelid, attnum, atttypid, attlen, attbyval, attalign, "
					"attisdropped, attname FROM pg_catalog.pg_attribute WHERE attnum > 0",
					true, 0) == SPI_OK_SELECT)
	{
		TupleDesc	td = SPI_tuptable->tupdesc;

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	t = SPI_tuptable->vals[i];
			bool		isnull;
			Oid			attrelid = DatumGetObjectId(SPI_getbinval(t, td, 1, &isnull));
			int16		attnum = DatumGetInt16(SPI_getbinval(t, td, 2, &isnull));
			Oid			atttypid = DatumGetObjectId(SPI_getbinval(t, td, 3, &isnull));
			int16		attlen = DatumGetInt16(SPI_getbinval(t, td, 4, &isnull));
			bool		attbyval = DatumGetBool(SPI_getbinval(t, td, 5, &isnull));
			char		attalign = DatumGetChar(SPI_getbinval(t, td, 6, &isnull));
			bool		attisdropped = DatumGetBool(SPI_getbinval(t, td, 7, &isnull));
			char	   *attname = SPI_getvalue(t, td, 8);

			walextract_prime_attr(ctx, attrelid, attnum, atttypid, attlen,
								   attbyval, attalign, attisdropped, attname);
			if (attname)
				pfree(attname);
		}
	}

	walextract_collect_identity(NULL, ctx);
	SPI_finish();
	walextract_set_dict_primed(ctx, true);
}

/*
 * walextract_export_dictionary() -> text
 *
 * Capture the live catalog as a text "sidecar" (WX_SIDECAR v0) that can later
 * prime a decode of a WAL range whose catalog is no longer current.  The
 * snapshot_lsn is the current flush LSN: a consumer accepts the sidecar only
 * for a range that STARTS at or after snapshot_lsn (capture-then-decode-later),
 * never to reconstruct a diverged past catalog.
 */
PG_FUNCTION_INFO_V1(walextract_export_dictionary);
Datum
walextract_export_dictionary(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	StringInfoData rels;
	StringInfoData attrs;
	StringInfoData idents;
	uint64		nrels = 0;
	uint64		nattrs = 0;
	uint64		i;

	initStringInfo(&buf);
	initStringInfo(&rels);
	initStringInfo(&attrs);
	initStringInfo(&idents);

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("walextract: SPI_connect failed in export")));

	if (SPI_execute("SELECT relfilenode, oid, relkind, relname "
					"FROM pg_catalog.pg_class WHERE relfilenode <> 0",
					true, 0) == SPI_OK_SELECT)
	{
		TupleDesc	td = SPI_tuptable->tupdesc;

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	t = SPI_tuptable->vals[i];
			bool		isnull;
			Oid			relfile = DatumGetObjectId(SPI_getbinval(t, td, 1, &isnull));
			Oid			relid = DatumGetObjectId(SPI_getbinval(t, td, 2, &isnull));
			char		relkind = DatumGetChar(SPI_getbinval(t, td, 3, &isnull));
			char	   *relname = SPI_getvalue(t, td, 4);

			appendStringInfo(&rels, "R %u %u %c %s\n",
							 relfile, relid, relkind, relname ? relname : "");
			if (relname)
				pfree(relname);
			nrels++;
		}
	}

	if (SPI_execute("SELECT attrelid, attnum, atttypid, attlen, attbyval, "
					"attalign, attisdropped, attname "
					"FROM pg_catalog.pg_attribute WHERE attnum > 0",
					true, 0) == SPI_OK_SELECT)
	{
		TupleDesc	td = SPI_tuptable->tupdesc;

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	t = SPI_tuptable->vals[i];
			bool		isnull;
			Oid			attrelid = DatumGetObjectId(SPI_getbinval(t, td, 1, &isnull));
			int16		attnum = DatumGetInt16(SPI_getbinval(t, td, 2, &isnull));
			Oid			atttypid = DatumGetObjectId(SPI_getbinval(t, td, 3, &isnull));
			int16		attlen = DatumGetInt16(SPI_getbinval(t, td, 4, &isnull));
			bool		attbyval = DatumGetBool(SPI_getbinval(t, td, 5, &isnull));
			char		attalign = DatumGetChar(SPI_getbinval(t, td, 6, &isnull));
			bool		attisdropped = DatumGetBool(SPI_getbinval(t, td, 7, &isnull));
			char	   *attname = SPI_getvalue(t, td, 8);

			appendStringInfo(&attrs, "A %u %d %u %d %d %c %d %s\n",
							 attrelid, attnum, atttypid, attlen,
							 attbyval ? 1 : 0, attalign, attisdropped ? 1 : 0,
							 attname ? attname : "");
			if (attname)
				pfree(attname);
			nattrs++;
		}
	}

	walextract_collect_identity(&idents, NULL);
	SPI_finish();

	appendStringInfoString(&buf, "WX_SIDECAR 2\n");
	appendStringInfo(&buf, "system_identifier " UINT64_FORMAT "\n",
					 GetSystemIdentifier());
	appendStringInfo(&buf, "source_db_oid %u\n", MyDatabaseId);
	appendStringInfo(&buf, "snapshot_lsn %X/%X\n",
					 LSN_FORMAT_ARGS(GetFlushRecPtr(NULL)));
	appendStringInfo(&buf, "extracted_at " INT64_FORMAT "\n",
					 (int64) GetCurrentTimestamp());
	appendStringInfoString(&buf, "source live\n");
	appendStringInfo(&buf, "counts " UINT64_FORMAT " " UINT64_FORMAT "\n",
					 nrels, nattrs);
	appendBinaryStringInfo(&buf, rels.data, rels.len);
	appendBinaryStringInfo(&buf, attrs.data, attrs.len);
	appendBinaryStringInfo(&buf, idents.data, idents.len);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(buf.data, buf.len));
}

/*
 * Consume a WX_SIDECAR v0 text blob: validate provenance, then prime the
 * dictionary.  Machine-mode policy: any validation failure is fail-closed
 * (ereport ERROR) -- a rejected sidecar must never silently fall through to an
 * unprimed (and thus partly UNKNOWN) decode.
 *
 * Validation: magic+version; system_identifier == this cluster; source_db_oid ==
 * MyDatabaseId; snapshot_lsn <= start_lsn (the sidecar may only seed a range
 * that begins at/after the captured catalog state -- capture-then-decode-later).
 */
static void
prime_from_sidecar(WalExtractContext *ctx, text *sidecar, XLogRecPtr start_lsn)
{
	char	   *raw = text_to_cstring(sidecar);
	char	   *saveptr = NULL;
	char	   *line;
	bool		have_magic = false;
	bool		have_sysid = false;
	bool		have_db = false;
	bool		have_lsn = false;
	bool		header_done = false;

	for (line = strtok_r(raw, "\n", &saveptr);
		 line != NULL;
		 line = strtok_r(NULL, "\n", &saveptr))
	{
		while (*line == ' ' || *line == '\t' || *line == '\r')
			line++;
		if (*line == '\0')
			continue;

		if (line[0] == 'R' && line[1] == ' ')
		{
			Oid			relfile;
			Oid			relid;
			char		relkind = 0;
			int			off = 0;

			if (!header_done)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: record before complete header")));
			if (sscanf(line, "R %u %u %c %n", &relfile, &relid, &relkind, &off) < 3 || off == 0)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: malformed R record")));
			walextract_prime_rel(ctx, relfile, relid, relkind, line + off);
			continue;
		}
		if (line[0] == 'A' && line[1] == ' ')
		{
			Oid			relid;
			Oid			atttypid;
			int			attnum;
			int			attlen;
			int			abv;
			int			aisd;
			char		attalign = 0;
			int			off = 0;

			if (!header_done)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: record before complete header")));
			if (sscanf(line, "A %u %d %u %d %d %c %d %n",
					   &relid, &attnum, &atttypid, &attlen, &abv, &attalign, &aisd, &off) < 7 || off == 0)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: malformed A record")));
			walextract_prime_attr(ctx, relid, (int16) attnum, atttypid, (int16) attlen,
								  abv ? true : false, attalign, aisd ? true : false, line + off);
			continue;
		}

		if (line[0] == 'I' && line[1] == ' ')
		{
			Oid			relfile;
			char		relreplident = 0;
			int			natts = 0;
			int			off = 0;
			int16		atts[WX_IDENT_MAXATTS];
			int			got = 0;
			char	   *p;

			if (!header_done)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: record before complete header")));
			if (sscanf(line, "I %u %c %d %n", &relfile, &relreplident, &natts, &off) < 3 || off == 0)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: malformed I record")));
			if (natts < 0 || natts > WX_IDENT_MAXATTS)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: I record natts %d out of range", natts)));
			p = line + off;
			for (got = 0; got < natts; got++)
			{
				int			v;
				int			n2 = 0;

				while (*p == ' ')
					p++;
				if (sscanf(p, "%d%n", &v, &n2) != 1 || n2 == 0)
					break;
				atts[got] = (int16) v;
				p += n2;
			}
			if (got != natts)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: I record attnum count mismatch (want %d, got %d)",
									   natts, got)));
			walextract_prime_identity(ctx, relfile, relreplident, atts, natts);
			continue;
		}

		if (!have_magic)
		{
			int			ver = 0;

			if (sscanf(line, "WX_SIDECAR %d", &ver) != 1)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: bad magic (expected WX_SIDECAR)")));
			if (ver != 1 && ver != 2)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: unsupported version %d", ver)));
			have_magic = true;
			continue;
		}
		if (strncmp(line, "system_identifier ", 18) == 0)
		{
			uint64		sysid = 0;

			if (sscanf(line + 18, UINT64_FORMAT, &sysid) != 1)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: bad system_identifier")));
			if (sysid != GetSystemIdentifier())
				ereport(ERROR, (errmsg("walextract: sidecar rejected: system_identifier mismatch")));
			have_sysid = true;
			continue;
		}
		if (strncmp(line, "source_db_oid ", 14) == 0)
		{
			Oid			db = InvalidOid;

			if (sscanf(line + 14, "%u", &db) != 1)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: bad source_db_oid")));
			if (db != MyDatabaseId)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: source_db_oid mismatch (sidecar %u, current %u)",
									   db, MyDatabaseId)));
			have_db = true;
			continue;
		}
		if (strncmp(line, "snapshot_lsn ", 13) == 0)
		{
			uint32		hi = 0;
			uint32		lo = 0;
			XLogRecPtr	snap;

			if (sscanf(line + 13, "%X/%X", &hi, &lo) != 2)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: bad snapshot_lsn")));
			snap = ((uint64) hi << 32) | lo;
			if (snap > start_lsn)
				ereport(ERROR, (errmsg("walextract: sidecar rejected: snapshot_lsn %X/%X is after range start %X/%X",
									   LSN_FORMAT_ARGS(snap), LSN_FORMAT_ARGS(start_lsn))));
			have_lsn = true;
			continue;
		}
		if (strncmp(line, "counts ", 7) == 0)
		{
			if (!(have_magic && have_sysid && have_db && have_lsn))
				ereport(ERROR, (errmsg("walextract: sidecar rejected: incomplete header before counts")));
			header_done = true;
			continue;
		}
		/* extracted_at / source / unknown header lines: informational, ignored */
	}

	if (!header_done)
		ereport(ERROR, (errmsg("walextract: sidecar rejected: missing or incomplete header")));

	walextract_set_dict_primed(ctx, true);
	pfree(raw);
}

/* arg slot 3: empty text means "no sidecar" (lets the SRFs stay STRICT). */
static text *
sidecar_arg_or_null(text *t)
{
	return (VARSIZE_ANY_EXHDR(t) == 0) ? NULL : t;
}

/*
 * Apply the chosen priming source to a fresh context.  A non-NULL sidecar takes
 * precedence over the live-catalog prime flag (the caller asked to decode a
 * historical range against a captured snapshot, not the live catalog).
 */
static void
apply_priming(WalExtractContext *ctx, bool prime, text *sidecar, XLogRecPtr start_lsn)
{
	if (sidecar != NULL)
		prime_from_sidecar(ctx, sidecar, start_lsn);
	else if (prime)
		prime_from_catalog(ctx);
}

/* shared WAL scan in batch mode; ERRORs (fail-closed) if the core stops */
static XLogReaderState *InitXLogReaderState(XLogRecPtr lsn);	/* defined below */
static XLogRecord *ReadNextXLogRecord(XLogReaderState *xlogreader);	/* defined below */

static void
scan_batches(XLogRecPtr start_lsn, XLogRecPtr end_lsn, BatchSinkCtx *bs,
			 bool prime, text *sidecar, int64 *ev_peak, int64 *b_peak)
{
	XLogReaderState *xlogreader;
	WalExtractContext *wectx;
	XLogRecPtr	curr_flush;

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

	wectx = walextract_context_create();
	if (wectx == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	walextract_set_emit_batch(wectx, batch_sink, bs);	/* clears event sink */
	walextract_set_pgdata(wectx, DataDir);
	walextract_set_bootstrap(wectx, false);
	walextract_set_target_db(wectx, MyDatabaseId);
	apply_priming(wectx, prime, sidecar, start_lsn);

	xlogreader = InitXLogReaderState(start_lsn);
	while (ReadNextXLogRecord(xlogreader) && xlogreader->EndRecPtr <= end_lsn)
	{
		CHECK_FOR_INTERRUPTS();
		walextract_record(wectx, xlogreader);
		if (walextract_failed(wectx))
			break;
	}

	if (walextract_failed(wectx))
	{
		const char *msg = walextract_status_message(wectx);

		pfree(xlogreader->private_data);
		XLogReaderFree(xlogreader);
		walextract_context_free(wectx);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("walextract batch mode stopped: %s", msg)));
	}

	*ev_peak = (int64) walextract_buf_peak(wectx);
	*b_peak = (int64) walextract_bbuf_peak(wectx);
	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);
	walextract_context_free(wectx);
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
	apply_priming(wectx, PG_GETARG_BOOL(2),
				  sidecar_arg_or_null(PG_GETARG_TEXT_PP(3)), start_lsn);

	xlogreader = InitXLogReaderState(start_lsn);
	while (ReadNextXLogRecord(xlogreader) &&
		   xlogreader->EndRecPtr <= end_lsn)
	{
		CHECK_FOR_INTERRUPTS();
		walextract_record(wectx, xlogreader);
		if (walextract_failed(wectx))
			break;
	}

	/*
	 * Fail closed: if the transaction assembler could not uphold its contract
	 * (buffer cap, OOM, or a prepared-xact record), do not return a partial or
	 * possibly-uncommitted result set; raise an error instead.
	 */
	if (walextract_failed(wectx))
	{
		const char *msg = walextract_status_message(wectx);

		pfree(xlogreader->private_data);
		XLogReaderFree(xlogreader);
		walextract_context_free(wectx);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("walextract could not assemble transactions: %s", msg)));
	}

	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);
	walextract_context_free(wectx);
	return (Datum) 0;
}

/* ----- machine-path SRFs / selftest ----- */

/*
 * EVENT+NULL baseline: event mode (WX_RENDER_EVENT, raw payload buffered) with
 * a counting-only sink -- no SQL rendering, no tuplestore materialization.  This
 * is the fair machine-vs-machine comparison point for the batch path.
 */
typedef struct EvCountCtx
{
	int64		n;
} EvCountCtx;

static void
event_count_sink(const ChangeEvent *ev, void *sink)
{
	((EvCountCtx *) sink)->n++;
}

PG_FUNCTION_INFO_V1(walextract_wal2event_count);
Datum
walextract_wal2event_count(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	XLogReaderState *xlogreader;
	WalExtractContext *wectx;
	XLogRecPtr	curr_flush;
	EvCountCtx	ec = {0};
	Datum		values[2];
	bool		isnull[2] = {false, false};

	if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to mine WAL")));

	curr_flush = GetFlushRecPtr(NULL);
	if (end_lsn == InvalidXLogRecPtr || end_lsn > curr_flush)
		end_lsn = curr_flush;
	if (start_lsn > end_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("start_lsn is greater than end_lsn")));

	InitMaterializedSRF(fcinfo, 0);
	wectx = walextract_context_create();
	if (wectx == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	walextract_set_render_mode(wectx, WX_RENDER_EVENT);	/* raw payload, no SQL */
	walextract_set_emit(wectx, event_count_sink, &ec);
	walextract_set_pgdata(wectx, DataDir);
	walextract_set_bootstrap(wectx, false);
	walextract_set_target_db(wectx, MyDatabaseId);
	apply_priming(wectx, PG_GETARG_BOOL(2),
				  sidecar_arg_or_null(PG_GETARG_TEXT_PP(3)), start_lsn);

	xlogreader = InitXLogReaderState(start_lsn);
	while (ReadNextXLogRecord(xlogreader) && xlogreader->EndRecPtr <= end_lsn)
	{
		CHECK_FOR_INTERRUPTS();
		walextract_record(wectx, xlogreader);
		if (walextract_failed(wectx))
			break;
	}
	if (walextract_failed(wectx))
	{
		const char *msg = walextract_status_message(wectx);

		pfree(xlogreader->private_data);
		XLogReaderFree(xlogreader);
		walextract_context_free(wectx);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("walextract event mode stopped: %s", msg)));
	}

	values[0] = Int64GetDatum(ec.n);
	values[1] = Int64GetDatum((int64) walextract_buf_peak(wectx));
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, isnull);

	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);
	walextract_context_free(wectx);
	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(walextract_wal2batch);
Datum
walextract_wal2batch(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	BatchSinkCtx bs;
	int64		ev_peak,
				b_peak;

	InitMaterializedSRF(fcinfo, 0);
	memset(&bs, 0, sizeof(bs));
	bs.tupstore = rsinfo->setResult;
	bs.tupdesc = rsinfo->setDesc;
	bs.detail = true;
	scan_batches(start_lsn, end_lsn, &bs, PG_GETARG_BOOL(2),
				 sidecar_arg_or_null(PG_GETARG_TEXT_PP(3)),
				 &ev_peak, &b_peak);
	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(walextract_batch_stats);
Datum
walextract_batch_stats(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	BatchSinkCtx bs;
	int64		ev_peak = 0,
				b_peak = 0;
	Datum		values[7];
	bool		isnull[7];

	InitMaterializedSRF(fcinfo, 0);
	memset(&bs, 0, sizeof(bs));
	bs.detail = false;
	scan_batches(start_lsn, end_lsn, &bs, PG_GETARG_BOOL(2),
				 sidecar_arg_or_null(PG_GETARG_TEXT_PP(3)),
				 &ev_peak, &b_peak);

	memset(isnull, 0, sizeof(isnull));
	values[0] = Int64GetDatum(bs.n_batches);
	values[1] = Int64GetDatum(bs.total_rows);
	values[2] = Int64GetDatum(bs.total_payload);
	values[3] = Int64GetDatum(bs.total_nulls);
	values[4] = BoolGetDatum(bs.any_toast);
	values[5] = Int64GetDatum(ev_peak);
	values[6] = Int64GetDatum(b_peak);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, isnull);
	return (Datum) 0;
}

static const char *
mode_name(WalExtractActiveMode m)
{
	switch (m)
	{
		case WX_MODE_EVENT:
			return "event";
		case WX_MODE_BATCH:
			return "batch";
		default:
			return "none";
	}
}

/*
 * Proves the single-active contract WITHOUT touching opaque context state:
 * installing one sink clears the other.  Expected:
 *   after_set_event=event;after_set_batch=batch
 */
PG_FUNCTION_INFO_V1(walextract_mode_selftest);
Datum
walextract_mode_selftest(PG_FUNCTION_ARGS)
{
	WalExtractContext *c = walextract_context_create();
	char		buf[80];
	const char *a,
			   *b;

	if (c == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

	walextract_set_emit_batch(c, batch_sink, NULL);
	walextract_set_emit(c, sink_emit, NULL);	/* must clear batch -> event */
	a = mode_name(walextract_active_mode(c));

	walextract_set_emit(c, sink_emit, NULL);
	walextract_set_emit_batch(c, batch_sink, NULL); /* must clear event -> batch */
	b = mode_name(walextract_active_mode(c));

	walextract_context_free(c);
	snprintf(buf, sizeof(buf), "after_set_event=%s;after_set_batch=%s", a, b);
	PG_RETURN_TEXT_P(cstring_to_text(buf));
}
