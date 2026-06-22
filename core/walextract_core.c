/*
 * walextract_core.c - WAL mining core.
 *
 * Produces a structured ChangeEvent stream (primary output); op_text is a
 * derived formatter field.  All mutable state lives in WalExtractContext.
 * This is a physical WAL miner: WAL record != committed change, physical
 * tuple != logical DML, catalog tuple != original DDL.  Single-db scope.
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/xlogreader.h"
#include "access/rmgr.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/tupmacs.h"
#include "varatt.h"
#include "catalog/pg_class.h"
#include "catalog/pg_attribute.h"
#include "storage/bufpage.h"

#include "walextract_core.h"

/* ---- on-disk layout of base/<db>/pg_filenode.map (see relmapper.c) ---- */
#define MINE_RELMAP_MAGIC 0x592717
#define MINE_RELMAP_MAX   64
typedef struct MineRelMapFile
{
	int32		magic;
	int32		num_mappings;
	struct
	{
		uint32		mapoid;
		uint32		mapfilenumber;
	}			maps[MINE_RELMAP_MAX];
	uint32		crc;
} MineRelMapFile;

/* ---- TID-keyed dictionary entry (prior identity of a catalog tuple) ---- */
#define MDE_EMPTY	0
#define MDE_USED	1
#define MDE_DELETED 2				/* tombstone: keeps the probe chain intact */
typedef struct MineDictEnt
{
	uint8		state;
	Oid			rel;
	BlockNumber blk;
	OffsetNumber off;
	uint16		ulen;
	char		udata[1024];
} MineDictEnt;
#define MINE_DICT_SZ 8192

/* ---- relfilenode -> (oid, relkind, relname) learned from mined pg_class ---- */
typedef struct MineRelMapEnt
{
	bool		used;
	Oid			relfile;
	Oid			relid;
	char		relkind;
	char		relname[64];
} MineRelMapEnt;
#define MINE_NREL 8192

/* ---- per-relation column descriptors keyed by relid, from pg_attribute ---- */
typedef struct MineCol
{
	int16		attnum;
	Oid			atttypid;
	int16		attlen;
	bool		attbyval;
	char		attalign;
	bool		attisdropped;
	char		attname[64];
} MineCol;
typedef struct MineRelDesc
{
	bool		used;
	Oid			relid;
	int			ncols;
	MineCol		cols[80];
} MineRelDesc;
#define MINE_NDESC 1024

/* ---- explicit context: all mutable decoder state ---- */
struct WalExtractContext
{
	/* output sink */
	WalExtractEmit emit_cb;
	void	   *emit_sink;

	/* configuration */
	char		pgdata[1024];
	bool		do_bootstrap;
	Oid			pgclass_fn;		/* may be preset via env override */
	Oid			pgattr_fn;

	/* single-db binding (replaces a global "loaded" flag) */
	Oid			target_db;		/* caller-set target; InvalidOid = debug bind-first */
	Oid			bound_db;
	bool		dict_loaded;

	/* current record provenance */
	XLogRecPtr	cur_lsn;
	TransactionId cur_xid;

	/* dictionaries */
	MineDictEnt dict[MINE_DICT_SZ];
	MineRelMapEnt relfile[MINE_NREL];
	MineRelDesc desc[MINE_NDESC];
};

/* ===================== small helpers ===================== */

const char *
walextract_op_name(WalChangeOp op)
{
	switch (op)
	{
		case WCO_INSERT:
			return "INSERT";
		case WCO_UPDATE:
			return "UPDATE";
		case WCO_DELETE:
			return "DELETE";
		case WCO_DDL_CATALOG:
			return "DDL_CATALOG";
		default:
			return "UNKNOWN";
	}
}

static void
we_emit_event(WalExtractContext *ctx, ChangeEvent *ev)
{
	if (ctx->emit_cb)
		ctx->emit_cb(ev, ctx->emit_sink);
}

static void
ev_add_reason(ChangeEvent *ev, const char *reason)
{
	int			i;

	for (i = 0; i < ev->nreasons; i++)
		if (ev->reasons[i] == reason)	/* stable-string identity */
			return;
	if (ev->nreasons < WALEXTRACT_MAX_REASONS)
		ev->reasons[ev->nreasons++] = reason;
}

static bool we_appendf(char *buf, size_t cap, size_t *pos, const char *fmt,...) pg_attribute_printf(4, 5);
static bool
we_appendf(char *buf, size_t cap, size_t *pos, const char *fmt,...)
{
	va_list		ap;
	int			n;

	if (*pos >= cap)
		return true;			/* buffer already full: truncated */
	va_start(ap, fmt);
	n = vsnprintf(buf + *pos, cap - *pos, fmt, ap);
	va_end(ap);
	if (n < 0)
		return false;
	if (*pos + (size_t) n >= cap)
	{
		*pos = cap - 1;
		return true;			/* output did not fit: truncated */
	}
	*pos += (size_t) n;
	return false;
}

static bool
mine_buf_append_part(char *dst, size_t cap, uint16 *pos, const char *src, size_t len)
{
	if (len > cap || (size_t) *pos > cap - len)
		return false;			/* would read/write past dst: fail safe */
	memcpy(dst + *pos, src, len);
	*pos += (uint16) len;
	return true;
}

static bool
we_append_text_oneline(char *buf, size_t cap, size_t *pos, const char *s)
{
	const char *p;
	bool		trunc = false;

	for (p = s; p && *p; p++)
	{
		unsigned char ch = (unsigned char) *p;

		trunc |= we_appendf(buf, cap, pos, "%c", ch < 0x20 ? ' ' : ch);
	}
	return trunc;
}

/* ===================== relmapper bootstrap ===================== */

static void mine_scan_catalog(WalExtractContext *ctx, Oid db, Oid filenode, bool is_class);

static void
mine_ensure_dict(WalExtractContext *ctx, Oid dboid)
{
	char		path[1200];
	FILE	   *f;
	MineRelMapFile m;
	int			i;

	if (ctx->dict_loaded)
		return;
	if (ctx->pgclass_fn && ctx->pgattr_fn)	/* env override */
	{
		ctx->dict_loaded = true;
		return;
	}
	if (ctx->pgdata[0] == '\0')
		return;
	snprintf(path, sizeof(path), "%s/base/%u/pg_filenode.map", ctx->pgdata, dboid);
	f = fopen(path, "rb");
	if (f == NULL)
		return;
	if (fread(&m, 1, sizeof(m), f) >= 8 && m.magic == MINE_RELMAP_MAGIC)
	{
		for (i = 0; i < m.num_mappings && i < MINE_RELMAP_MAX; i++)
		{
			if (m.maps[i].mapoid == RelationRelationId)
				ctx->pgclass_fn = m.maps[i].mapfilenumber;
			else if (m.maps[i].mapoid == AttributeRelationId)
				ctx->pgattr_fn = m.maps[i].mapfilenumber;
		}
		if (ctx->pgclass_fn && ctx->pgattr_fn)
		{
			ctx->dict_loaded = true;
			if (ctx->do_bootstrap)
			{
				mine_scan_catalog(ctx, dboid, ctx->pgclass_fn, true);
				mine_scan_catalog(ctx, dboid, ctx->pgattr_fn, false);
			}
		}
	}
	fclose(f);
}

/* ===================== dictionary (open addressing + tombstones) ===================== */

static uint32
mine_hash(Oid rel, BlockNumber blk, OffsetNumber off)
{
	return (((uint32) rel * 131u + blk) * 131u + off) % MINE_DICT_SZ;
}

static MineDictEnt *
mine_dict_find(WalExtractContext *ctx, Oid rel, BlockNumber blk, OffsetNumber off)
{
	uint32		h = mine_hash(rel, blk, off);
	int			i;

	for (i = 0; i < MINE_DICT_SZ; i++)
	{
		MineDictEnt *e = &ctx->dict[(h + i) % MINE_DICT_SZ];

		if (e->state == MDE_EMPTY)
			return NULL;
		if (e->state == MDE_USED && e->rel == rel && e->blk == blk && e->off == off)
			return e;
		/* MDE_DELETED: keep probing */
	}
	return NULL;
}

static void
mine_dict_put(WalExtractContext *ctx, Oid rel, BlockNumber blk, OffsetNumber off,
			  char *udata, uint16 ulen)
{
	uint32		h = mine_hash(rel, blk, off);
	int			i;
	int			tomb = -1;

	for (i = 0; i < MINE_DICT_SZ; i++)
	{
		int			idx = (h + i) % MINE_DICT_SZ;
		MineDictEnt *e = &ctx->dict[idx];

		if (e->state == MDE_USED && e->rel == rel && e->blk == blk && e->off == off)
		{
			e->ulen = Min(ulen, (uint16) sizeof(e->udata));
			memcpy(e->udata, udata, e->ulen);
			return;
		}
		if (e->state == MDE_DELETED)
		{
			if (tomb < 0)
				tomb = idx;
			continue;
		}
		if (e->state == MDE_EMPTY)
		{
			MineDictEnt *t = &ctx->dict[tomb >= 0 ? tomb : idx];

			t->state = MDE_USED;
			t->rel = rel;
			t->blk = blk;
			t->off = off;
			t->ulen = Min(ulen, (uint16) sizeof(t->udata));
			memcpy(t->udata, udata, t->ulen);
			return;
		}
	}
	if (tomb >= 0)				/* table saturated but a tombstone is reusable */
	{
		MineDictEnt *t = &ctx->dict[tomb];

		t->state = MDE_USED;
		t->rel = rel;
		t->blk = blk;
		t->off = off;
		t->ulen = Min(ulen, (uint16) sizeof(t->udata));
		memcpy(t->udata, udata, t->ulen);
	}
}

static void
mine_dict_del(WalExtractContext *ctx, Oid rel, BlockNumber blk, OffsetNumber off)
{
	MineDictEnt *e = mine_dict_find(ctx, rel, blk, off);

	if (e)
		e->state = MDE_DELETED;
}

static bool
mine_is_catalog(WalExtractContext *ctx, Oid rel)
{
	return (rel == ctx->pgclass_fn || rel == ctx->pgattr_fn);
}

static void
mine_identity(WalExtractContext *ctx, Oid rel, char *udata, char *buf, size_t buflen)
{
	if (rel == ctx->pgclass_fn)
	{
		Form_pg_class f = (Form_pg_class) udata;

		snprintf(buf, buflen, "relation %s (relkind=%c)", NameStr(f->relname), f->relkind);
	}
	else if (rel == ctx->pgattr_fn)
	{
		Form_pg_attribute f = (Form_pg_attribute) udata;

		snprintf(buf, buflen, "attrelid=%u attnum=%d attname=%s",
				 f->attrelid, f->attnum, NameStr(f->attname));
	}
	else
		snprintf(buf, buflen, "filenode=%u", rel);
}

/* ===================== relfilenode map ===================== */

static void
mine_relfile_put(WalExtractContext *ctx, Oid relfile, Oid relid, char relkind, const char *relname)
{
	uint32		h;
	int			i;

	if (relfile == InvalidOid)
		return;
	h = (relfile * 131u) % MINE_NREL;
	for (i = 0; i < MINE_NREL; i++)
	{
		MineRelMapEnt *e = &ctx->relfile[(h + i) % MINE_NREL];

		if (!e->used || e->relfile == relfile)
		{
			e->used = true;
			e->relfile = relfile;
			e->relid = relid;
			e->relkind = relkind;
			strlcpy(e->relname, relname ? relname : "", sizeof(e->relname));
			return;
		}
	}
}

static Oid
mine_relfile_get(WalExtractContext *ctx, Oid relfile)
{
	uint32		h = (relfile * 131u) % MINE_NREL;
	int			i;

	for (i = 0; i < MINE_NREL; i++)
	{
		MineRelMapEnt *e = &ctx->relfile[(h + i) % MINE_NREL];

		if (!e->used)
			return InvalidOid;
		if (e->relfile == relfile)
			return e->relid;
	}
	return InvalidOid;
}

static const char *
mine_relname_get(WalExtractContext *ctx, Oid relfile)
{
	uint32		h = (relfile * 131u) % MINE_NREL;
	int			i;

	for (i = 0; i < MINE_NREL; i++)
	{
		MineRelMapEnt *e = &ctx->relfile[(h + i) % MINE_NREL];

		if (!e->used)
			return NULL;
		if (e->relfile == relfile)
			return e->relname[0] ? e->relname : NULL;
	}
	return NULL;
}

/* ===================== column descriptors ===================== */

static MineRelDesc *
mine_desc_get(WalExtractContext *ctx, Oid relid, bool create)
{
	uint32		h = (relid * 131u) % MINE_NDESC;
	int			i;

	for (i = 0; i < MINE_NDESC; i++)
	{
		MineRelDesc *d = &ctx->desc[(h + i) % MINE_NDESC];

		if (d->used && d->relid == relid)
			return d;
		if (!d->used)
		{
			if (!create)
				return NULL;
			d->used = true;
			d->relid = relid;
			d->ncols = 0;
			return d;
		}
	}
	return NULL;
}

static void
mine_udesc_add(WalExtractContext *ctx, Form_pg_attribute f)
{
	MineRelDesc *d = mine_desc_get(ctx, f->attrelid, true);
	int			i,
				pos;

	if (d == NULL || d->ncols >= (int) (sizeof(d->cols) / sizeof(d->cols[0])))
		return;
	for (i = 0; i < d->ncols; i++)
		if (d->cols[i].attnum == f->attnum)
			return;
	for (pos = d->ncols; pos > 0 && d->cols[pos - 1].attnum > f->attnum; pos--)
		d->cols[pos] = d->cols[pos - 1];
	d->cols[pos].attnum = f->attnum;
	d->cols[pos].atttypid = f->atttypid;
	d->cols[pos].attlen = f->attlen;
	d->cols[pos].attbyval = f->attbyval;
	d->cols[pos].attalign = f->attalign;
	d->cols[pos].attisdropped = f->attisdropped;
	strlcpy(d->cols[pos].attname, NameStr(f->attname), sizeof(d->cols[pos].attname));
	d->ncols++;
}

/* ===================== value rendering (semantic honesty) ===================== */

/*
 * Append one value as a SQL literal.  Returns true only if the value was
 * rendered faithfully.  External TOAST pointers and types the renderer does
 * not understand are NOT faithfully representable: a placeholder is written,
 * *reason is set, and false is returned so the caller can mark complete=false.
 */
static bool
mine_append_literal(char *buf, size_t cap, size_t *pos, Oid typid, char *p, int len,
					const char **reason)
{
	bool		trunc = false;

	(void) len;
	switch (typid)
	{
		case 16:
			trunc = we_appendf(buf, cap, pos, "%s", (*(bool *) p) ? "true" : "false");
			break;
		case 21:
			trunc = we_appendf(buf, cap, pos, "%d", *(int16 *) p);
			break;
		case 23:
			trunc = we_appendf(buf, cap, pos, "%d", *(int32 *) p);
			break;
		case 26:
			trunc = we_appendf(buf, cap, pos, "%u", *(Oid *) p);
			break;
		case 20:
			trunc = we_appendf(buf, cap, pos, INT64_FORMAT, *(int64 *) p);
			break;
		case 700:
			trunc = we_appendf(buf, cap, pos, "%g", *(float *) p);
			break;
		case 701:
			trunc = we_appendf(buf, cap, pos, "%g", *(double *) p);
			break;
		case 18:				/* "char": 1-byte internal char, NOT varlena */
			{
				char		ch = *(char *) p;

				if (ch == '\'')
					trunc = we_appendf(buf, cap, pos, "''''");
				else if (ch == 0)
					trunc = we_appendf(buf, cap, pos, "''");
				else
					trunc = we_appendf(buf, cap, pos, "'%c'", ch);
				break;
			}
		case 25:
		case 1043:
		case 1042:
			if (VARATT_IS_EXTERNAL(p))
			{
				we_appendf(buf, cap, pos, "NULL");
				*reason = WER_TOAST_EXTERNAL;
				return false;
			}
			else
			{
				int			n = (int) VARSIZE_ANY_EXHDR(p);
				char	   *s = VARDATA_ANY(p);
				int			k;

				trunc |= we_appendf(buf, cap, pos, "'");
				for (k = 0; k < n; k++)
				{
					if (s[k] == '\'')
						trunc |= we_appendf(buf, cap, pos, "''");
					else
						trunc |= we_appendf(buf, cap, pos, "%c", s[k]);
				}
				trunc |= we_appendf(buf, cap, pos, "'");
				break;
			}
		default:
			we_appendf(buf, cap, pos, "NULL");
			*reason = WER_UNKNOWN_TYPE;
			return false;
	}

	if (trunc)
	{
		*reason = WER_VALUE_TRUNCATED;
		return false;
	}
	return true;
}

/* ===================== identifier quoting + op_text formatter ===================== */

/*
 * Always double-quote identifiers using the actual mined name.  This is
 * case/keyword safe; the core is frontend+backend neutral so it cannot use the
 * backend's quote_identifier().
 */
static bool
mine_append_ident(char *buf, size_t cap, size_t *pos, const char *id)
{
	const char *p;
	bool		trunc = false;

	trunc |= we_appendf(buf, cap, pos, "\"");
	for (p = id; p && *p; p++)
	{
		if (*p == '"')
			trunc |= we_appendf(buf, cap, pos, "\"\"");
		else
			trunc |= we_appendf(buf, cap, pos, "%c", *p);
	}
	trunc |= we_appendf(buf, cap, pos, "\"");
	return trunc;
}

/*
 * Render op_text purely from ev->cols[].  op_text is derived output, never the
 * source of truth.  Relation is NOT schema-qualified (schema unresolved in v0):
 * see WER_SCHEMA_MISSING and the README same-search_path caveat.
 */
static bool
we_append_ident_oneline(char *buf, size_t cap, size_t *pos, const char *id)
{
	const char *p;
	bool		trunc = false;

	trunc |= we_appendf(buf, cap, pos, "\"");
	for (p = id; p && *p; p++)
	{
		unsigned char ch = (unsigned char) *p;

		if (ch == '"')
			trunc |= we_appendf(buf, cap, pos, "\"\"");
		else if (ch < 0x20)			/* control/newline -> space: stay one line */
			trunc |= we_appendf(buf, cap, pos, " ");
		else
			trunc |= we_appendf(buf, cap, pos, "%c", ch);
	}
	trunc |= we_appendf(buf, cap, pos, "\"");
	return trunc;
}

static bool
mine_format_insert(const ChangeEvent *ev, char *buf, size_t cap)
{
	size_t		pos = 0;
	int			i;
	bool		trunc = false;

	/*
	 * Incomplete events must never look like replayable SQL.  A single '--'
	 * comments only ONE line, and decoded values/identifiers may contain
	 * newlines, so an incomplete event renders a one-line, control-char free
	 * summary with no user values at all.
	 */
	if (!ev->complete)
	{
		trunc |= we_appendf(buf, cap, &pos, "-- INCOMPLETE INSERT rel=");
		if (ev->relname)
			trunc |= we_append_ident_oneline(buf, cap, &pos, ev->relname);
		else
			trunc |= we_appendf(buf, cap, &pos, "#%u", ev->relfilenode);
		trunc |= we_appendf(buf, cap, &pos, " reasons={");
		for (i = 0; i < ev->nreasons; i++)
			trunc |= we_appendf(buf, cap, &pos, "%s%s", i ? "," : "", ev->reasons[i]);
		trunc |= we_appendf(buf, cap, &pos, "}");
		return trunc;
	}

	trunc |= we_appendf(buf, cap, &pos, "INSERT INTO ");
	if (ev->relname)
		trunc |= mine_append_ident(buf, cap, &pos, ev->relname);
	else
		trunc |= we_appendf(buf, cap, &pos, "?");
	trunc |= we_appendf(buf, cap, &pos, " (");
	for (i = 0; i < ev->ncols; i++)
	{
		trunc |= mine_append_ident(buf, cap, &pos, ev->cols[i].attname);
		if (i + 1 < ev->ncols)
			trunc |= we_appendf(buf, cap, &pos, ", ");
	}
	trunc |= we_appendf(buf, cap, &pos, ") VALUES (");
	for (i = 0; i < ev->ncols; i++)
	{
		if (ev->cols[i].isnull || ev->cols[i].value_text == NULL)
			trunc |= we_appendf(buf, cap, &pos, "NULL");
		else
			trunc |= we_appendf(buf, cap, &pos, "%s", ev->cols[i].value_text);
		if (i + 1 < ev->ncols)
			trunc |= we_appendf(buf, cap, &pos, ", ");
	}
	trunc |= we_appendf(buf, cap, &pos, ");");
	return trunc;
}

/* ===================== user-table INSERT decode ===================== */

static void
mine_decode_dml(WalExtractContext *ctx, HeapTupleHeader htup, RelFileLocator *rloc)
{
	Oid			relfile = rloc->relNumber;
	Oid			relid;
	const char *relname;
	MineRelDesc *d;
	ChangeEvent ev;
	char		op_text[8192];

	/*
	 * Focus the stream on user relations and relations we already track.
	 * Untracked system catalogs (filenode < FirstNormalObjectId, no mapping)
	 * are DDL side effects, surfaced via the pg_class/pg_attribute path, not
	 * as user changes.  This keeps the physical stream usable for ProGate.
	 */
	relid = mine_relfile_get(ctx, relfile);
	if (relfile < FirstNormalObjectId && relid == InvalidOid)
		return;
	if (relid == InvalidOid)
		relid = relfile;		/* fresh table: relfilenode == oid */
	relname = mine_relname_get(ctx, relfile);

	memset(&ev, 0, sizeof(ev));
	ev.record_lsn = ctx->cur_lsn;
	ev.commit_lsn = InvalidXLogRecPtr;
	ev.xid = ctx->cur_xid;
	ev.db_oid = ctx->bound_db;
	ev.rel_oid = relid;
	ev.relfilenode = relfile;
	ev.relname = relname;
	ev.schema_name = NULL;
	ev.op = WCO_INSERT;
	ev.ncols = 0;

	d = mine_desc_get(ctx, relid, false);
	if (d == NULL || d->ncols == 0)
	{
		ev.complete = false;
		ev_add_reason(&ev, WER_DICTIONARY_MISSING);
		mine_format_insert(&ev, op_text, sizeof(op_text));
		ev.op_text = op_text;
		we_emit_event(ctx, &ev);
		return;
	}

	{
		int			natts = HeapTupleHeaderGetNatts(htup);
		bool		hasnulls = (htup->t_infomask & HEAP_HASNULL) != 0;
		uint8	   *bp = htup->t_bits;
		char	   *tp = (char *) htup + htup->t_hoff;
		long		off = 0;
		int			i;
		char		valstore[3072];
		size_t		valpos = 0;

		ev.complete = true;
		for (i = 0; i < d->ncols && i < natts; i++)
		{
			MineCol    *c = &d->cols[i];
			bool		isnull = (hasnulls && att_isnull(i, bp));
			char	   *vptr = NULL;

			/* compute pointer + advance offset for stored (non-null) values */
			if (!isnull)
			{
				if (c->attlen == -1)
					off = att_align_pointer(off, c->attalign, -1, tp + off);
				else if (c->attlen != -2)
					off = att_align_nominal(off, c->attalign);
				vptr = tp + off;
			}

			/* dropped columns keep physical storage but are not logical cols */
			if (!c->attisdropped && ev.ncols < WALEXTRACT_MAX_COLS)
			{
				ChangeColumn *cc = &ev.cols[ev.ncols++];

				cc->attnum = c->attnum;
				cc->typid = c->atttypid;
				cc->attname = c->attname;
				cc->isnull = isnull;
				cc->complete = true;
				cc->reason = NULL;
				cc->value_text = NULL;
				cc->raw_ptr = NULL;
				cc->raw_len = 0;

				if (!isnull)
				{
					/*
					 * Bounded per-column rendering.  If valstore is exhausted we
					 * must NOT render or point value_text past the buffer: fall
					 * back to a static placeholder and mark the value truncated.
					 */
					if (valpos >= sizeof(valstore) - 1)
					{
						cc->value_text = "NULL";
						cc->complete = false;
						cc->reason = WER_VALUE_TRUNCATED;
						ev.complete = false;
						ev_add_reason(&ev, WER_VALUE_TRUNCATED);
					}
					else
					{
						const char *reason = NULL;
						size_t		start = valpos;
						bool		ok;

						ok = mine_append_literal(valstore, sizeof(valstore), &valpos,
												 c->atttypid, vptr, c->attlen, &reason);
						if (valpos >= sizeof(valstore))
							valpos = sizeof(valstore) - 1;
						valstore[valpos++] = '\0';	/* terminate + separate */
						cc->value_text = valstore + start;
						cc->raw_ptr = vptr;
						if (c->attlen == -1)
							cc->raw_len = VARSIZE_ANY(vptr);
						else if (c->attlen == -2)
							cc->raw_len = strlen(vptr) + 1;
						else
							cc->raw_len = (Size) c->attlen;
						if (!ok)
						{
							cc->complete = false;
							cc->reason = reason;
							ev.complete = false;
							if (reason)
								ev_add_reason(&ev, reason);
						}
					}
				}
			}

			if (!isnull)
			{
				if (c->attlen == -1)
					off += VARSIZE_ANY(vptr);
				else if (c->attlen == -2)
					off += strlen(vptr) + 1;
				else
					off += c->attlen;
			}
		}

		/*
		 * Capacity / coverage honesty: a tuple with more attributes than we hold
		 * descriptors for (descriptor capped at WALEXTRACT_MAX_COLS, or only
		 * partially mined) means not every column was decoded.
		 */
		if (natts > d->ncols)
		{
			ev.complete = false;
			ev_add_reason(&ev, d->ncols >= WALEXTRACT_MAX_COLS
						  ? WER_TOO_MANY_COLUMNS : WER_DICTIONARY_MISSING);
		}

		/*
		 * Schema is unresolved in v0.  This is an advisory replay caveat (the
		 * statement is valid only under a same-search_path target); it does NOT
		 * clear complete, which reflects payload-decode fidelity only.
		 */
		if (ev.relname)
			ev_add_reason(&ev, WER_SCHEMA_MISSING);

		if (mine_format_insert(&ev, op_text, sizeof(op_text)) && ev.complete)
		{
			ev.complete = false;
			ev_add_reason(&ev, WER_VALUE_TRUNCATED);
			mine_format_insert(&ev, op_text, sizeof(op_text));
		}
		ev.op_text = op_text;
		we_emit_event(ctx, &ev);
	}
}

/* ===================== offline catalog bootstrap (dev guard) ===================== */

/*
 * Bootstrap the base dictionary by scanning a catalog heap file directly.
 * DEV GUARD: safe only on a quiesced/offline PGDATA (or filesystem snapshot).
 * Reading a live cluster's catalog files is unsafe (torn pages, no clog/MVCC).
 */
static void
mine_scan_catalog(WalExtractContext *ctx, Oid db, Oid filenode, bool is_class)
{
	char		path[1300];
	FILE	   *f;
	char		pg[BLCKSZ];

	snprintf(path, sizeof(path), "%s/base/%u/%u", ctx->pgdata, db, filenode);
	f = fopen(path, "rb");
	if (f == NULL)
		return;
	while (fread(pg, 1, BLCKSZ, f) == BLCKSZ)
	{
		Page		page = (Page) pg;
		OffsetNumber off,
					max;

		if (PageIsNew(page))
			continue;
		max = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= max; off++)
		{
			ItemId		lp = PageGetItemId(page, off);
			HeapTupleHeader h;
			char	   *ud;

			if (!ItemIdIsNormal(lp))
				continue;
			h = (HeapTupleHeader) PageGetItem(page, lp);
			if (h->t_infomask & HEAP_XMIN_INVALID)
				continue;		/* aborted insert */
			if (!(h->t_infomask & HEAP_XMAX_INVALID) &&
				HeapTupleHeaderGetRawXmax(h) != InvalidTransactionId)
				continue;		/* deleted or superseded version */
			ud = (char *) h + h->t_hoff;
			if (is_class)
			{
				Form_pg_class c = (Form_pg_class) ud;

				mine_relfile_put(ctx, c->relfilenode, c->oid, c->relkind, NameStr(c->relname));
			}
			else
			{
				Form_pg_attribute a = (Form_pg_attribute) ud;

				if (a->attnum > 0)
					mine_udesc_add(ctx, a);
			}
		}
	}
	fclose(f);
}

/* ===================== catalog effect events (DDL_CATALOG) ===================== */

static void
mine_emit_catalog_insert(WalExtractContext *ctx, RelFileLocator *rloc, char *userdata)
{
	ChangeEvent ev;
	char		op_text[512];
	size_t		pos = 0;

	memset(&ev, 0, sizeof(ev));
	ev.record_lsn = ctx->cur_lsn;
	ev.commit_lsn = InvalidXLogRecPtr;
	ev.xid = ctx->cur_xid;
	ev.db_oid = ctx->bound_db;
	ev.relfilenode = rloc->relNumber;
	ev.op = WCO_DDL_CATALOG;
	ev.complete = false;		/* catalog effect, not original DDL */

	if (rloc->relNumber == ctx->pgclass_fn)
	{
		Form_pg_class f = (Form_pg_class) userdata;

		ev.rel_oid = f->oid;
		ev.relname = NameStr(f->relname);
		we_appendf(op_text, sizeof(op_text), &pos, "-- catalog: pg_class INSERT relname=");
		we_append_text_oneline(op_text, sizeof(op_text), &pos, NameStr(f->relname));
		we_appendf(op_text, sizeof(op_text), &pos, " relkind=%c relnamespace=%u",
				   f->relkind, f->relnamespace);
		ev.op_text = op_text;
		we_emit_event(ctx, &ev);
		mine_relfile_put(ctx, f->relfilenode, f->oid, f->relkind, NameStr(f->relname));
	}
	else if (rloc->relNumber == ctx->pgattr_fn)
	{
		Form_pg_attribute f = (Form_pg_attribute) userdata;

		if (f->attnum > 0)
		{
			ev.rel_oid = f->attrelid;
			we_appendf(op_text, sizeof(op_text), &pos,
					   "-- catalog: pg_attribute INSERT attrelid=%u attnum=%d attname=",
					   f->attrelid, f->attnum);
			we_append_text_oneline(op_text, sizeof(op_text), &pos, NameStr(f->attname));
			we_appendf(op_text, sizeof(op_text), &pos, " atttypid=%u", f->atttypid);
			ev.op_text = op_text;
			we_emit_event(ctx, &ev);
			mine_udesc_add(ctx, f);
		}
	}
}

/* ===================== tuple reconstruction ===================== */

static void
mine_take_tuple(WalExtractContext *ctx, char *src, Size srclen, uint16 im2, uint16 im,
				uint8 hoff, RelFileLocator *rloc, BlockNumber blk, OffsetNumber off)
{
	char		tupbuf[2 * BLCKSZ];
	HeapTupleHeader htup = (HeapTupleHeader) tupbuf;
	char	   *userdata;
	uint16		ulen;

	if (srclen > 2 * BLCKSZ - SizeofHeapTupleHeader)
		return;
	memset(htup, 0, SizeofHeapTupleHeader);
	memcpy((char *) htup + SizeofHeapTupleHeader, src, srclen);
	htup->t_infomask2 = im2;
	htup->t_infomask = im;
	htup->t_hoff = hoff;
	userdata = (char *) htup + htup->t_hoff;
	ulen = (uint16) (srclen - (htup->t_hoff - SizeofHeapTupleHeader));

	if (mine_is_catalog(ctx, rloc->relNumber))
	{
		mine_emit_catalog_insert(ctx, rloc, userdata);
		mine_dict_put(ctx, rloc->relNumber, blk, off, userdata, ulen);
	}
	else
		mine_decode_dml(ctx, htup, rloc);
}

static void
mine_take_page_item(WalExtractContext *ctx, Page page, OffsetNumber off,
					RelFileLocator *rloc, BlockNumber blk)
{
	ItemId		lp = PageGetItemId(page, off);
	HeapTupleHeader h;
	char	   *userdata;
	uint16		ulen;

	if (!ItemIdIsNormal(lp))
		return;
	h = (HeapTupleHeader) PageGetItem(page, lp);
	userdata = (char *) h + h->t_hoff;
	ulen = (uint16) (ItemIdGetLength(lp) - h->t_hoff);
	if (mine_is_catalog(ctx, rloc->relNumber))
	{
		mine_emit_catalog_insert(ctx, rloc, userdata);
		mine_dict_put(ctx, rloc->relNumber, blk, off, userdata, ulen);
	}
	else
		mine_decode_dml(ctx, h, rloc);
}

/* ===================== record dispatch ===================== */

void
walextract_record(WalExtractContext *ctx, XLogReaderState *record)
{
	uint8		rmid = XLogRecGetRmid(record);
	uint8		op = XLogRecGetInfo(record) & XLOG_HEAP_OPMASK;
	RelFileLocator rloc;
	ForkNumber	forknum;
	BlockNumber blk;
	Size		datalen;

	if (rmid != RM_HEAP_ID && rmid != RM_HEAP2_ID)
		return;
	if (!XLogRecGetBlockTagExtended(record, 0, &rloc, &forknum, &blk, NULL))
		return;

	/* single-db scope */
	if (rloc.dbOid == InvalidOid)
		return;					/* shared catalogs: out of single-db scope */
	if (ctx->target_db != InvalidOid)
	{
		/* explicit target: never silently bind to whichever db appears first */
		if (rloc.dbOid != ctx->target_db)
			return;
		ctx->bound_db = ctx->target_db;
	}
	else
	{
		/* debug fallback only: bind to the first real database seen */
		if (ctx->bound_db == InvalidOid)
			ctx->bound_db = rloc.dbOid;
		else if (rloc.dbOid != ctx->bound_db)
			return;
	}

	mine_ensure_dict(ctx, ctx->bound_db);

	ctx->cur_lsn = record->ReadRecPtr;
	ctx->cur_xid = XLogRecGetXid(record);

	/* single heap insert */
	if (rmid == RM_HEAP_ID && op == XLOG_HEAP_INSERT)
	{
		xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
		char	   *blkdata = XLogRecGetBlockData(record, 0, &datalen);

		if (blkdata == NULL)
		{
			char		pagebuf[BLCKSZ];

			if (XLogRecHasBlockImage(record, 0) && RestoreBlockImage(record, 0, pagebuf))
				mine_take_page_item(ctx, (Page) pagebuf, xlrec->offnum, &rloc, blk);
			return;
		}
		{
			xl_heap_header *xlhdr = (xl_heap_header *) blkdata;

			mine_take_tuple(ctx, blkdata + SizeOfHeapHeader, datalen - SizeOfHeapHeader,
							xlhdr->t_infomask2, xlhdr->t_infomask, xlhdr->t_hoff,
							&rloc, blk, xlrec->offnum);
		}
		return;
	}

	/* heap multi-insert (CREATE TABLE writes pg_attribute rows this way) */
	if (rmid == RM_HEAP2_ID && op == XLOG_HEAP2_MULTI_INSERT)
	{
		xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);
		char	   *ptr = XLogRecGetBlockData(record, 0, &datalen);
		int			i;

		if (ptr == NULL)
		{
			char		pagebuf[BLCKSZ];

			if (XLogRecHasBlockImage(record, 0) && RestoreBlockImage(record, 0, pagebuf))
				for (i = 0; i < xlrec->ntuples; i++)
					mine_take_page_item(ctx, (Page) pagebuf, xlrec->offsets[i], &rloc, blk);
			return;
		}
		for (i = 0; i < xlrec->ntuples; i++)
		{
			xl_multi_insert_tuple *xlhdr;
			OffsetNumber toff = xlrec->offsets[i];

			ptr = (char *) SHORTALIGN(ptr);
			xlhdr = (xl_multi_insert_tuple *) ptr;
			ptr += SizeOfMultiInsertTuple;
			mine_take_tuple(ctx, ptr, xlhdr->datalen, xlhdr->t_infomask2,
							xlhdr->t_infomask, xlhdr->t_hoff, &rloc, blk, toff);
			ptr += xlhdr->datalen;
		}
		return;
	}

	/* heap update: catalog RENAME / column change appears here (DDL_CATALOG) */
	if (rmid == RM_HEAP_ID && (op == XLOG_HEAP_UPDATE || op == XLOG_HEAP_HOT_UPDATE))
	{
		xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
		BlockNumber oldblk = blk;
		RelFileLocator r2;
		ForkNumber	f2;
		BlockNumber b2;
		MineDictEnt *olde;
		char		oldid[160] = "<unknown>";
		char		newid[160] = "<undecoded>";
		char	   *recdata;
		ChangeEvent ev;
		char		op_text[512];
		size_t		pos = 0;
		bool		catalog_trunc = false;

		if (!mine_is_catalog(ctx, rloc.relNumber))
			return;				/* user UPDATE not implemented (by design) */
		if (XLogRecGetBlockTagExtended(record, 1, &r2, &f2, &b2, NULL))
			oldblk = b2;

		olde = mine_dict_find(ctx, rloc.relNumber, oldblk, xlrec->old_offnum);
		if (olde)
			mine_identity(ctx, rloc.relNumber, olde->udata, oldid, sizeof(oldid));

		recdata = XLogRecGetBlockData(record, 0, &datalen);
		if (recdata != NULL)
		{
			char	   *recend = recdata + datalen;
			uint16		prefixlen = 0;
			uint16		suffixlen = 0;
			xl_heap_header xlhdr;
			Size		tuplen;
			uint16		hdrregion;
			char		nud[2048];
			uint16		nlen = 0;

			if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
			{
				memcpy(&prefixlen, recdata, sizeof(uint16));
				recdata += sizeof(uint16);
			}
			if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
			{
				memcpy(&suffixlen, recdata, sizeof(uint16));
				recdata += sizeof(uint16);
			}
			memcpy(&xlhdr, recdata, SizeOfHeapHeader);
			recdata += SizeOfHeapHeader;
			tuplen = recend - recdata;
			hdrregion = xlhdr.t_hoff - SizeofHeapTupleHeader;

			if (prefixlen > 0)
			{
				if (!olde || prefixlen > olde->ulen ||
					!mine_buf_append_part(nud, sizeof(nud), &nlen, olde->udata, prefixlen))
					catalog_trunc = true;
			}
			if (!catalog_trunc && tuplen >= hdrregion)
			{
				if (!mine_buf_append_part(nud, sizeof(nud), &nlen,
										  recdata + hdrregion, tuplen - hdrregion))
					catalog_trunc = true;
			}
			if (!catalog_trunc && suffixlen > 0)
			{
				if (!olde || suffixlen > olde->ulen ||
					!mine_buf_append_part(nud, sizeof(nud), &nlen,
										  olde->udata + olde->ulen - suffixlen, suffixlen))
					catalog_trunc = true;
			}
			if (!catalog_trunc && nlen > 0 && (prefixlen == 0 || olde))
			{
				mine_identity(ctx, rloc.relNumber, nud, newid, sizeof(newid));
				mine_dict_put(ctx, rloc.relNumber, blk, xlrec->new_offnum, nud, nlen);
			}
			else if (catalog_trunc)
				strlcpy(newid, "<truncated>", sizeof(newid));
		}
		if (olde)
			mine_dict_del(ctx, rloc.relNumber, oldblk, xlrec->old_offnum);

		memset(&ev, 0, sizeof(ev));
		ev.record_lsn = ctx->cur_lsn;
		ev.commit_lsn = InvalidXLogRecPtr;
		ev.xid = ctx->cur_xid;
		ev.db_oid = ctx->bound_db;
		ev.relfilenode = rloc.relNumber;
		ev.op = WCO_DDL_CATALOG;
		ev.complete = false;
		if (catalog_trunc)
			ev_add_reason(&ev, WER_CATALOG_TRUNCATED);
		we_appendf(op_text, sizeof(op_text), &pos, "-- catalog: %s UPDATE was [",
				   rloc.relNumber == ctx->pgclass_fn ? "pg_class" : "pg_attribute");
		we_append_text_oneline(op_text, sizeof(op_text), &pos, oldid);
		we_appendf(op_text, sizeof(op_text), &pos, "] now [");
		we_append_text_oneline(op_text, sizeof(op_text), &pos, newid);
		we_appendf(op_text, sizeof(op_text), &pos, "]");
		ev.op_text = op_text;
		we_emit_event(ctx, &ev);
		return;
	}

	/* heap delete: catalog DROP appears here (DDL_CATALOG) */
	if (rmid == RM_HEAP_ID && op == XLOG_HEAP_DELETE)
	{
		xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
		MineDictEnt *e;

		if (!mine_is_catalog(ctx, rloc.relNumber))
			return;				/* user DELETE not implemented (by design) */
		e = mine_dict_find(ctx, rloc.relNumber, blk, xlrec->offnum);
		if (e)
		{
			char		id[160];
			ChangeEvent ev;
			char		op_text[256];
			size_t		pos = 0;

			mine_identity(ctx, rloc.relNumber, e->udata, id, sizeof(id));
			memset(&ev, 0, sizeof(ev));
			ev.record_lsn = ctx->cur_lsn;
			ev.commit_lsn = InvalidXLogRecPtr;
			ev.xid = ctx->cur_xid;
			ev.db_oid = ctx->bound_db;
			ev.relfilenode = rloc.relNumber;
			ev.op = WCO_DDL_CATALOG;
			ev.complete = false;
			we_appendf(op_text, sizeof(op_text), &pos, "-- catalog: %s DELETE dropped [",
					   rloc.relNumber == ctx->pgclass_fn ? "pg_class" : "pg_attribute");
			we_append_text_oneline(op_text, sizeof(op_text), &pos, id);
			we_appendf(op_text, sizeof(op_text), &pos, "]");
			ev.op_text = op_text;
			we_emit_event(ctx, &ev);
			mine_dict_del(ctx, rloc.relNumber, blk, xlrec->offnum);
		}
		return;
	}
}

/* ===================== context lifecycle + configuration ===================== */

WalExtractContext *
walextract_context_create(void)
{
	WalExtractContext *ctx = (WalExtractContext *) malloc(sizeof(WalExtractContext));

	if (ctx)
		memset(ctx, 0, sizeof(*ctx));
	return ctx;
}

void
walextract_context_free(WalExtractContext *ctx)
{
	if (ctx)
		free(ctx);
}

void
walextract_context_reset(WalExtractContext *ctx)
{
	WalExtractEmit cb = ctx->emit_cb;
	void	   *sink = ctx->emit_sink;
	bool		boot = ctx->do_bootstrap;
	Oid			tdb = ctx->target_db;
	char		pg[1024];

	strlcpy(pg, ctx->pgdata, sizeof(pg));
	memset(ctx, 0, sizeof(*ctx));
	ctx->emit_cb = cb;
	ctx->emit_sink = sink;
	ctx->do_bootstrap = boot;
	ctx->target_db = tdb;
	strlcpy(ctx->pgdata, pg, sizeof(ctx->pgdata));
}

void
walextract_set_emit(WalExtractContext *ctx, WalExtractEmit cb, void *sink)
{
	ctx->emit_cb = cb;
	ctx->emit_sink = sink;
}

void
walextract_set_pgdata(WalExtractContext *ctx, const char *pgdata)
{
	strlcpy(ctx->pgdata, pgdata, sizeof(ctx->pgdata));
}

void
walextract_set_filenodes(WalExtractContext *ctx, Oid pgclass_fn, Oid pgattr_fn)
{
	if (pgclass_fn != InvalidOid)
		ctx->pgclass_fn = pgclass_fn;
	if (pgattr_fn != InvalidOid)
		ctx->pgattr_fn = pgattr_fn;
}

void
walextract_set_bootstrap(WalExtractContext *ctx, bool on)
{
	ctx->do_bootstrap = on;
}

void
walextract_set_target_db(WalExtractContext *ctx, Oid dboid)
{
	ctx->target_db = dboid;
}
