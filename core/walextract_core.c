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
#include "access/xact.h"
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
	bool		invalid;		/* relfilenode dropped/rewritten at a COMMIT boundary;
								 * slot kept used to preserve the open-addressing probe
								 * chain (this table has no tombstones), but treated as
								 * absent by getters until a put() revives it */
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
	bool		invalid;		/* descriptor invalidated at a rewrite/drop boundary;
								 * slot kept used to preserve the probe chain */
	Oid			relid;
	int			ncols;
	MineCol		cols[80];
} MineRelDesc;
#define MINE_NDESC 1024

/*
 * ---- transaction assembler: per-xid buffered physical ChangeEvents ----
 *
 * A WAL record is not a committed change.  The decoder produces physical
 * ChangeEvents in WAL order; they are buffered here keyed by the record xid and
 * delivered to the emit sink only when the owning transaction's COMMIT record
 * is seen, in their original within-transaction WAL order.  ABORT discards the
 * buffer; transactions still open at the end of the scanned range are never
 * delivered.
 *
 * Each buffered node owns deep copies of every transient string the ChangeEvent
 * points at (op_text, relname, per-column attname/value_text), because those
 * live in caller stack / WAL-record buffers that are reused on the next record.
 * The in-tuple raw datum (cols[].raw_ptr) is the one field that genuinely
 * cannot survive deferral; it is documented emit-callback-only and is set NULL
 * (raw_len 0) on buffered nodes.
 *
 * Buffering is bounded by a per-call event-count cap and a byte cap
 * (WALEXTRACT_MAX_BUFFERED / _BYTES).  These are dev-guard caps, not a true
 * memory-bounded spill framework (out of scope for v0).  Hitting either cap, or
 * a malloc failure, fails closed: the assembler stops and records a fatal
 * status; the frontend turns that into an error rather than emitting
 * partial/uncommitted output.
 */
#define WALEXTRACT_MAX_BUFFERED 1048576		/* live buffered events per call */
#define WALEXTRACT_MAX_BUFFERED_BYTES (256 * 1024 * 1024)	/* live buffered bytes */

typedef struct WeBufEvent
{
	struct WeBufEvent *next;
	size_t		bytes;			/* size of this single allocation (node + packed strings) */
	ChangeEvent ev;				/* structured event; string pointers rewired into the
								 * trailing region of this same allocation.  op_text is
								 * NOT stored for the typed (INSERT) hot path: it is a
								 * downstream formatter product rendered at delivery. */
} WeBufEvent;

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

	/* transaction assembler: WAL-order list of buffered, not-yet-committed events */
	WeBufEvent *buf_head;
	WeBufEvent *buf_tail;
	int			buf_nlive;		/* number of buffered events not yet flushed */
	Size		buf_bytes;		/* total bytes held by the buffer (cap accounting) */
	WalExtractStatus status;	/* fail-closed status (WALEXTRACT_OK = healthy) */
	WalExtractRenderMode render_mode;	/* WX_RENDER_SQL (default) / WX_RENDER_EVENT */

	/* P2A machine-path batch sink + WAL-order batch buffer (parallel to event buffer) */
	WalExtractEmitBatch batch_emit_cb;
	void	   *batch_emit_sink;
	struct WeBufBatch *bbuf_head;
	struct WeBufBatch *bbuf_tail;
	int			bbuf_nlive;
	Size		bbuf_bytes;
	Size		buf_bytes_peak;		/* peak event-buffer bytes (commit-buffer memory) */
	Size		bbuf_bytes_peak;	/* peak batch-buffer bytes */
	const char *batch_unsupported_reason;	/* WEB_* set with WALEXTRACT_FATAL_UNSUPPORTED_BATCH */
	bool		dict_primed;		/* dictionary seeded from a provider before the scan */
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

/* op_text is a downstream formatter product, never assembler state for the
 * typed hot path; rendered at delivery from the structured event (pure fn). */
static bool mine_format_insert(const ChangeEvent *ev, char *buf, size_t cap);

/*
 * Deliver one assembled event to the sink.  This is the only place the SQL-text
 * formatter runs for INSERTs: the assembler buffers the structured ChangeEvent
 * (typed columns), and op_text is produced here -- downstream of all
 * correctness logic -- into a delivery-local buffer that is valid for the
 * duration of the callback.  DDL_CATALOG events have no typed payload in v0, so
 * their effect text is the event's only content and is carried verbatim
 * (op_text already set) rather than re-rendered.
 */
static void
we_deliver(WalExtractContext *ctx, ChangeEvent *ev)
{
	char		op_text[8192];

	if (ctx->emit_cb == NULL)
		return;
	if (ctx->render_mode == WX_RENDER_SQL && ev->op == WCO_INSERT)
	{
		mine_format_insert(ev, op_text, sizeof(op_text));
		ev->op_text = op_text;
	}
	ctx->emit_cb(ev, ctx->emit_sink);
}

/* bytes needed to copy s including its NUL (0 if NULL) */
static size_t
we_strspan(const char *s)
{
	return s ? strlen(s) + 1 : 0;
}

/* copy s into *cursor (if non-NULL), advance the cursor, return the copy */
static const char *
we_pack(char **cursor, const char *s)
{
	char	   *dst;
	size_t		n;

	if (s == NULL)
		return NULL;
	n = strlen(s) + 1;
	dst = *cursor;
	memcpy(dst, s, n);
	*cursor += n;
	return dst;
}

/* copy len raw bytes into *cursor (if any), advance, return the copy */
static const char *
we_pack_raw(char **cursor, const char *p, Size len)
{
	char	   *dst;

	if (p == NULL || len == 0)
		return NULL;
	dst = *cursor;
	memcpy(dst, p, len);
	*cursor += len;
	return dst;
}

/* drop one node from the buffer accounting and free its single allocation */
static void
we_node_free(WalExtractContext *ctx, WeBufEvent *node)
{
	ctx->buf_bytes -= node->bytes;
	ctx->buf_nlive--;
	free(node);
}

/*
 * Capture (buffer) a physical ChangeEvent until its transaction commits.
 *
 * The assembler buffers the STRUCTURED event, not SQL text.  Everything the
 * event needs to be reconstructed after deferral is copied into ONE allocation
 * per event (node + a trailing string region), keeping copying explicit and
 * local so it can later be replaced by a per-call arena / batch storage.
 *
 * Per-column payload depends on render mode:
 *   SQL mode (WX_RENDER_SQL): the in-tuple raw datum (raw_ptr) is dropped
 *     (emit-callback-only by contract) and the rendered value_text is copied;
 *     INSERT op_text is NOT copied -- it is re-rendered downstream at delivery
 *     (we_deliver).
 *   EVENT mode (WX_RENDER_EVENT): value_text is NULL and the raw value bytes are
 *     copied into this node's owned packed storage, with raw_ptr repointed there;
 *     no SQL is rendered.
 * DDL_CATALOG (either mode) has no typed payload in v0 and carries its effect text.
 *
 * Buffering is bounded by a per-call event cap and byte cap.  Hitting either
 * cap, or a malloc failure, fails closed: the assembler sets a fatal status and
 * delivers nothing further; the frontend turns that into an error rather than
 * emitting partial/uncommitted output.
 */
static void
we_emit_event(WalExtractContext *ctx, ChangeEvent *ev)
{
	bool		store_op_text = (ev->op != WCO_INSERT);
	size_t		strbytes = 0;
	size_t		total;
	int			i;
	WeBufEvent *node;
	char	   *cursor;

	if (ctx->status != WALEXTRACT_OK)
		return;					/* already failed closed: emit nothing further */

	if (ctx->buf_nlive >= WALEXTRACT_MAX_BUFFERED)
	{
		ctx->status = WALEXTRACT_FATAL_BUFFER_OVERFLOW;
		return;
	}

	/* size the single per-event allocation */
	strbytes += we_strspan(ev->relname);
	if (store_op_text)
		strbytes += we_strspan(ev->op_text);
	for (i = 0; i < ev->ncols; i++)
	{
		strbytes += we_strspan(ev->cols[i].attname);
		if (ctx->render_mode == WX_RENDER_EVENT)
			strbytes += ev->cols[i].raw_len;	/* raw payload survives buffering */
		else
			strbytes += we_strspan(ev->cols[i].value_text);
	}
	total = sizeof(WeBufEvent) + strbytes;

	if (ctx->buf_bytes + total > (Size) WALEXTRACT_MAX_BUFFERED_BYTES)
	{
		ctx->status = WALEXTRACT_FATAL_BUFFER_OVERFLOW;
		return;
	}

	node = (WeBufEvent *) malloc(total);
	if (node == NULL)
	{
		ctx->status = WALEXTRACT_FATAL_OOM;
		return;
	}
	node->next = NULL;
	node->bytes = total;
	node->ev = *ev;				/* scalars + static reason pointers copied here */
	cursor = (char *) (node + 1);

	/* rewire string pointers into this node's own trailing region */
	node->ev.relname = we_pack(&cursor, ev->relname);
	node->ev.schema_name = NULL;	/* not resolved in v0 */
	node->ev.op_text = store_op_text ? we_pack(&cursor, ev->op_text) : NULL;
	for (i = 0; i < ev->ncols; i++)
	{
		node->ev.cols[i].attname = we_pack(&cursor, ev->cols[i].attname);
		if (ctx->render_mode == WX_RENDER_EVENT)
		{
			/* event mode: preserve raw typed payload; repoint into packed store */
			node->ev.cols[i].value_text = NULL;
			node->ev.cols[i].raw_ptr = we_pack_raw(&cursor, ev->cols[i].raw_ptr,
												   ev->cols[i].raw_len);
			/* raw_len carried over from *ev (scalar); keep it consistent with ptr */
			if (node->ev.cols[i].raw_ptr == NULL)
				node->ev.cols[i].raw_len = 0;
		}
		else
		{
			node->ev.cols[i].value_text = we_pack(&cursor, ev->cols[i].value_text);
			node->ev.cols[i].raw_ptr = NULL;	/* SQL mode keeps rendered text only */
			node->ev.cols[i].raw_len = 0;
		}
	}

	/* append in WAL order */
	if (ctx->buf_tail == NULL)
		ctx->buf_head = ctx->buf_tail = node;
	else
	{
		ctx->buf_tail->next = node;
		ctx->buf_tail = node;
	}
	ctx->buf_nlive++;
	ctx->buf_bytes += total;
	if (ctx->buf_bytes > ctx->buf_bytes_peak)
		ctx->buf_bytes_peak = ctx->buf_bytes;
}

/* is xid the transaction or one of its committed/aborted subxacts? */
static bool
we_xid_in_family(TransactionId xid, TransactionId topxid,
				 const TransactionId *subxacts, int nsub)
{
	int			i;

	if (xid == topxid)
		return true;
	for (i = 0; i < nsub; i++)
		if (xid == subxacts[i])
			return true;
	return false;
}

/*
 * Is this relfilenode in the COMMIT drop list (dropped/rewritten at commit),
 * within the bound database?  Used to suppress buffered DML of a relfilenode
 * that the same transaction destroys, so an INSERT followed by TRUNCATE/DROP in
 * one transaction is never delivered as an apply candidate.
 */
static bool
we_commit_drops_relfile(WalExtractContext *ctx, const xl_xact_parsed_commit *parsed,
						Oid relfile)
{
	int			i;

	if (parsed == NULL || parsed->nrels == 0 || relfile == InvalidOid)
		return false;
	for (i = 0; i < parsed->nrels; i++)
	{
		if (parsed->xlocators[i].relNumber != relfile)
			continue;
		if (ctx->bound_db != InvalidOid &&
			parsed->xlocators[i].dbOid != ctx->bound_db)
			continue;
		return true;
	}
	return false;
}

/*
 * COMMIT: deliver every buffered event whose xid is in {topxid} U subxacts,
 * in WAL order, with commit_lsn set; then drop those nodes.  Walking the single
 * WAL-order list preserves within-transaction order across subtransactions.
 *
 * Boundary safety: an event whose relfilenode appears in this commit's drop list
 * (parsed->xlocators) belongs to storage the same transaction destroys
 * (TRUNCATE/DROP/rewrite); it is removed from the buffer WITHOUT delivery, so it
 * never reaches the sink as an apply candidate.  parsed may be NULL (no drops).
 */
static void
we_flush_family(WalExtractContext *ctx, TransactionId topxid,
				const TransactionId *subxacts, int nsub, XLogRecPtr commit_lsn,
				const xl_xact_parsed_commit *parsed)
{
	WeBufEvent *node = ctx->buf_head;
	WeBufEvent *prev = NULL;

	while (node != NULL)
	{
		WeBufEvent *next = node->next;

		if (we_xid_in_family(node->ev.xid, topxid, subxacts, nsub))
		{
			/* deliver unless the same transaction destroys this storage */
			if (!we_commit_drops_relfile(ctx, parsed, node->ev.relfilenode))
			{
				node->ev.commit_lsn = commit_lsn;
				we_deliver(ctx, &node->ev);
			}

			if (prev == NULL)
				ctx->buf_head = next;
			else
				prev->next = next;
			if (ctx->buf_tail == node)
				ctx->buf_tail = prev;

			we_node_free(ctx, node);
		}
		else
			prev = node;
		node = next;
	}
}

/*
 * ABORT: drop every buffered event in the family; deliver nothing.
 *
 * Dictionary side effects (relfilenode->relname, column descriptors, TID
 * identities) are applied immediately at scan time, NOT transactionally, so an
 * aborted transaction that emitted any DDL_CATALOG event has already mutated
 * the mined dictionary.  v0 cannot roll those mutations back, and a later
 * relfilenode reuse could then mis-decode a committed row against the aborted
 * schema -- silent corruption of the committed stream.  We cannot undo it
 * safely here, so we fail closed: the frontend turns this into an error rather
 * than continuing with a poisoned dictionary.  (Production fix: a transactional
 * dictionary; named blocker, out of scope for v0.)
 */
static void
we_discard_family(WalExtractContext *ctx, TransactionId topxid,
				  const TransactionId *subxacts, int nsub)
{
	WeBufEvent *node = ctx->buf_head;
	WeBufEvent *prev = NULL;

	while (node != NULL)
	{
		WeBufEvent *next = node->next;

		if (we_xid_in_family(node->ev.xid, topxid, subxacts, nsub))
		{
			if (node->ev.op == WCO_DDL_CATALOG)
				ctx->status = WALEXTRACT_FATAL_ABORTED_DDL;

			if (prev == NULL)
				ctx->buf_head = next;
			else
				prev->next = next;
			if (ctx->buf_tail == node)
				ctx->buf_tail = prev;

			we_node_free(ctx, node);
		}
		else
			prev = node;
		node = next;
	}
}

/* free all still-buffered (open at range end / fatal) events without delivery */
static void
we_buf_free_all(WalExtractContext *ctx)
{
	WeBufEvent *node = ctx->buf_head;

	while (node != NULL)
	{
		WeBufEvent *next = node->next;

		free(node);
		node = next;
	}
	ctx->buf_head = ctx->buf_tail = NULL;
	ctx->buf_nlive = 0;
	ctx->buf_bytes = 0;
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
			e->invalid = false;		/* relfilenode reused after a drop: revive mapping */
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
			return e->invalid ? InvalidOid : e->relid;
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
			return (e->invalid || !e->relname[0]) ? NULL : e->relname;
	}
	return NULL;
}

/*
 * Relation trust classification for a relfilenode, derived ONLY from the mined /
 * primed dictionary (never a live catalog peek inside the core).  This is the
 * machine-stream trust boundary: an unknown relfilenode must never be guessed to
 * be user data.
 *
 *   WX_REL_CATALOG  - pg_class / pg_attribute: dictionary-learning, not a stream.
 *   WX_REL_INTERNAL - known relkind that is not an ordinary table (TOAST 't',
 *                     index, sequence, matview, ...) OR an untracked system
 *                     relfilenode (< FirstNormalObjectId): internal storage.
 *   WX_REL_USER     - known ordinary table (relkind 'r'): user DML stream.
 *   WX_REL_UNKNOWN  - user-range relfilenode (>= FirstNormalObjectId) with no
 *                     dictionary entry: untrusted, could be a pre-range table or
 *                     a pre-range TOAST relation.  Machine mode fails closed;
 *                     the forensic path marks it unknown_dictionary.
 */
typedef enum WalExtractRelTrust
{
	WX_REL_CATALOG,
	WX_REL_INTERNAL,
	WX_REL_USER,
	WX_REL_UNKNOWN
} WalExtractRelTrust;

/* relkind for a relfilenode from the dictionary; 0 if unknown/invalidated */
static char
mine_relfile_relkind(WalExtractContext *ctx, Oid relfile)
{
	uint32		h = (relfile * 131u) % MINE_NREL;
	int			i;

	for (i = 0; i < MINE_NREL; i++)
	{
		MineRelMapEnt *e = &ctx->relfile[(h + i) % MINE_NREL];

		if (!e->used)
			return 0;
		if (e->relfile == relfile)
			return e->invalid ? 0 : e->relkind;
	}
	return 0;
}

static WalExtractRelTrust
we_rel_trust(WalExtractContext *ctx, Oid relfile)
{
	char		rk;

	if (mine_is_catalog(ctx, relfile))
		return WX_REL_CATALOG;
	if (relfile < FirstNormalObjectId)
		return WX_REL_INTERNAL;		/* system storage range: never user data */
	rk = mine_relfile_relkind(ctx, relfile);
	if (rk == 0)
		return WX_REL_UNKNOWN;		/* user-range relfilenode, not in dictionary */
	if (rk == RELKIND_RELATION)
		return WX_REL_USER;
	return WX_REL_INTERNAL;			/* TOAST 't', index, sequence, matview, ... */
}

/*
 * Invalidate the relfilenode->relid/name mapping for a relfilenode dropped at a
 * rewrite/TRUNCATE/DROP COMMIT boundary.  The slot is kept used (this table has
 * no tombstones) and marked invalid, so getters report it absent while the
 * probe chain stays intact.  Returns the prior relid (InvalidOid if untracked)
 * and copies the prior relname into namebuf, so the caller can describe the
 * boundary before the mapping is gone.
 */
static Oid
mine_relfile_invalidate(WalExtractContext *ctx, Oid relfile, char *namebuf, size_t namecap)
{
	uint32		h = (relfile * 131u) % MINE_NREL;
	int			i;

	if (namebuf && namecap)
		namebuf[0] = '\0';
	for (i = 0; i < MINE_NREL; i++)
	{
		MineRelMapEnt *e = &ctx->relfile[(h + i) % MINE_NREL];

		if (!e->used)
			return InvalidOid;
		if (e->relfile == relfile)
		{
			Oid			prev = e->invalid ? InvalidOid : e->relid;

			if (namebuf && namecap && !e->invalid && e->relname[0])
				strlcpy(namebuf, e->relname, namecap);
			e->invalid = true;
			return prev;
		}
	}
	return InvalidOid;
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
		{
			if (!d->invalid)
				return d;
			/* invalidated by a boundary: absent for lookup, revived for create */
			if (!create)
				return NULL;
			d->invalid = false;
			d->ncols = 0;
			return d;
		}
		if (!d->used)
		{
			if (!create)
				return NULL;
			d->used = true;
			d->invalid = false;
			d->relid = relid;
			d->ncols = 0;
			return d;
		}
	}
	return NULL;
}

/*
 * Invalidate the column descriptor reachable for a dropped relfilenode.  For a
 * fresh (never-rewritten) relation relfilenode == oid, so the descriptor keyed
 * by relid == relfile is exactly the one a future stale decode of that
 * relfilenode would resolve via the relfilenode==oid fallback in
 * mine_decode_dml().  Poisoning it forces dictionary_missing instead of a
 * collision mis-decode after the relfilenode value is later reused.
 */
static void
mine_desc_invalidate(WalExtractContext *ctx, Oid relid)
{
	uint32		h = (relid * 131u) % MINE_NDESC;
	int			i;

	for (i = 0; i < MINE_NDESC; i++)
	{
		MineRelDesc *d = &ctx->desc[(h + i) % MINE_NDESC];

		if (!d->used)
			return;
		if (d->relid == relid)
		{
			d->invalid = true;
			return;
		}
	}
}

/*
 * Invalidate every dictionary entry for a relid: descriptor + all relmap
 * entries mapping to it.  After this, we_rel_trust() returns UNKNOWN for the
 * relation's relfilenode -> machine mode fails closed, event marks
 * unknown_dictionary.  Used when an in-range catalog mutation changes a
 * relation's metadata and we cannot safely re-learn it (v0: invalidate, never
 * decode with stale metadata).
 */
static void
mine_invalidate_relid(WalExtractContext *ctx, Oid relid)
{
	int			i;

	if (relid == InvalidOid)
		return;
	mine_desc_invalidate(ctx, relid);
	for (i = 0; i < MINE_NREL; i++)
	{
		MineRelMapEnt *e = &ctx->relfile[i];

		if (e->used && !e->invalid && e->relid == relid)
			e->invalid = true;
	}
}

/*
 * Conservative whole-dictionary invalidation: mark every relmap entry and
 * descriptor invalid and clear the primed flag, so we_rel_trust() returns
 * UNKNOWN for everything and machine mode fails closed from here on.
 *
 * Used only for the opaque in-range pg_attribute UPDATE case: when a relation's
 * pg_attribute row was written BEFORE the decoded range (a primed/pre-range
 * relation), an in-range UPDATE to it is prefix/suffix-compressed against an old
 * tuple we never cached, so the new tuple -- and even its attrelid -- cannot be
 * reconstructed.  We cannot identify which relation changed, so we cannot apply
 * the precise per-field check; the only safe action is to stop trusting the
 * dictionary.  (Precise mine_catalog_attr_mutation() still handles every case
 * where the new tuple IS reconstructable, i.e. in-range-learned relations.)
 */
static void
mine_invalidate_all(WalExtractContext *ctx)
{
	int			i;

	for (i = 0; i < MINE_NREL; i++)
		if (ctx->relfile[i].used)
			ctx->relfile[i].invalid = true;
	for (i = 0; i < MINE_NDESC; i++)
		if (ctx->desc[i].used)
			ctx->desc[i].invalid = true;
	walextract_set_dict_primed(ctx, false);
}

/*
 * In-range pg_attribute UPDATE: decide whether the existing mined descriptor is
 * still safe, using the NEW tuple only as evidence (this is invalidate-only --
 * we never re-learn the descriptor from a catalog UPDATE in v0).
 *
 * Only fields that affect tuple deform / typed raw layout invalidate the mined
 * descriptor.  attstorage and attname are intentionally ignored here: storage
 * affects future TOAST policy, not the physical layout contract; attname is
 * forensic label metadata.  (atttypmod / attcollation / stats are likewise not
 * layout-relevant for v0.)  If any decode-relevant field changed (DROP COLUMN
 * flips attisdropped; an in-place type change moves atttypid/attlen/attbyval/
 * attalign), invalidate the relation so machine mode fails closed.
 *
 * pg_class is NOT handled: relation drop/rewrite is covered by the smgr
 * rewrite/drop boundary, and other pg_class UPDATEs are benign.
 */
static void
mine_catalog_attr_mutation(WalExtractContext *ctx, Oid catalog_relfile,
						   const char *newdata, uint32 newlen)
{
	Form_pg_attribute f;
	MineRelDesc *d;
	int			i;

	if (catalog_relfile != ctx->pgattr_fn)
		return;
	if (newdata == NULL || newlen < ATTRIBUTE_FIXED_PART_SIZE)
		return;					/* cannot read the updated attribute (truncated):
								 * decode-relevant changes are small and always
								 * reconstruct, so leave the dictionary as-is */
	f = (Form_pg_attribute) newdata;
	if (f->attnum <= 0)
		return;
	d = mine_desc_get(ctx, f->attrelid, false);
	if (d == NULL || d->invalid)
		return;					/* relation not tracked / already invalid */
	for (i = 0; i < d->ncols; i++)
	{
		MineCol    *c = &d->cols[i];

		if (c->attnum != f->attnum)
			continue;
		if (c->attisdropped != f->attisdropped ||
			c->atttypid != f->atttypid ||
			c->attlen != f->attlen ||
			c->attbyval != f->attbyval ||
			c->attalign != f->attalign)
			mine_invalidate_relid(ctx, f->attrelid);
		return;
	}
}

static void
mine_desc_add_col(WalExtractContext *ctx, Oid relid, int16 attnum, Oid atttypid,
				  int16 attlen, bool attbyval, char attalign, bool attisdropped,
				  const char *attname)
{
	MineRelDesc *d = mine_desc_get(ctx, relid, true);
	int			i,
				pos;

	if (d == NULL || d->ncols >= (int) (sizeof(d->cols) / sizeof(d->cols[0])))
		return;
	for (i = 0; i < d->ncols; i++)
		if (d->cols[i].attnum == attnum)
			return;
	for (pos = d->ncols; pos > 0 && d->cols[pos - 1].attnum > attnum; pos--)
		d->cols[pos] = d->cols[pos - 1];
	d->cols[pos].attnum = attnum;
	d->cols[pos].atttypid = atttypid;
	d->cols[pos].attlen = attlen;
	d->cols[pos].attbyval = attbyval;
	d->cols[pos].attalign = attalign;
	d->cols[pos].attisdropped = attisdropped;
	strlcpy(d->cols[pos].attname, attname ? attname : "", sizeof(d->cols[pos].attname));
	d->ncols++;
}

static void
mine_udesc_add(WalExtractContext *ctx, Form_pg_attribute f)
{
	mine_desc_add_col(ctx, f->attrelid, f->attnum, f->atttypid, f->attlen,
					  f->attbyval, f->attalign, f->attisdropped, NameStr(f->attname));
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
	 * Trust gate.  The stream is focused on relations the dictionary positively
	 * knows.  Internal/system relations (incl. TOAST) are dropped; an unknown
	 * user-range relfilenode is NOT guessed to be a user table -- the forensic
	 * path emits an explicit unknown_dictionary marker instead.
	 */
	{
		WalExtractRelTrust trust = we_rel_trust(ctx, relfile);

		if (trust == WX_REL_INTERNAL)
			return;				/* system/TOAST/index/etc: not user rows */
		if (trust == WX_REL_UNKNOWN)
		{
			memset(&ev, 0, sizeof(ev));
			ev.record_lsn = ctx->cur_lsn;
			ev.commit_lsn = InvalidXLogRecPtr;
			ev.xid = ctx->cur_xid;
			ev.db_oid = ctx->bound_db;
			ev.rel_oid = relfile;
			ev.relfilenode = relfile;
			ev.relname = NULL;
			ev.op = WCO_INSERT;
			ev.ncols = 0;
			ev.complete = false;
			ev_add_reason(&ev, WER_UNKNOWN_DICTIONARY);
			mine_format_insert(&ev, op_text, sizeof(op_text));
			ev.op_text = op_text;
			we_emit_event(ctx, &ev);
			return;
		}
		/* WX_REL_USER (WX_REL_CATALOG never reaches mine_decode_dml) */
		relid = mine_relfile_get(ctx, relfile);
	}
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
					/* raw typed refs captured in both modes (for event payload
					 * buffering and for SQL/debug rendering of raw_len). */
					cc->raw_ptr = vptr;
					if (c->attlen == -1)
						cc->raw_len = VARSIZE_ANY(vptr);
					else if (c->attlen == -2)
						cc->raw_len = strlen(vptr) + 1;
					else
						cc->raw_len = (Size) c->attlen;

					if (ctx->render_mode == WX_RENDER_EVENT)
					{
						/*
						 * Event mode (product path): keep raw typed payload, do
						 * NOT build a SQL literal.  complete is raw-payload
						 * relative: only an external TOAST pointer means the value
						 * is not present in this WAL record.  unknown_type and
						 * value_truncated are SQL-renderer reasons and do not apply.
						 */
						if (c->attlen == -1 && VARATT_IS_EXTERNAL(vptr))
						{
							cc->complete = false;
							cc->reason = WER_TOAST_EXTERNAL;
							ev.complete = false;
							ev_add_reason(&ev, WER_TOAST_EXTERNAL);
						}
					}
					else if (valpos >= sizeof(valstore) - 1)
					{
						/* SQL mode: render buffer exhausted -> value_truncated. */
						cc->value_text = "NULL";
						cc->complete = false;
						cc->reason = WER_VALUE_TRUNCATED;
						ev.complete = false;
						ev_add_reason(&ev, WER_VALUE_TRUNCATED);
					}
					else
					{
						/* SQL mode: bounded SQL literal rendering (debug/compat). */
						const char *reason = NULL;
						size_t		start = valpos;
						bool		ok;

						ok = mine_append_literal(valstore, sizeof(valstore), &valpos,
												 c->atttypid, vptr, c->attlen, &reason);
						if (valpos >= sizeof(valstore))
							valpos = sizeof(valstore) - 1;
						valstore[valpos++] = '\0';	/* terminate + separate */
						cc->value_text = valstore + start;
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

		if (ctx->render_mode == WX_RENDER_SQL)
		{
			if (mine_format_insert(&ev, op_text, sizeof(op_text)) && ev.complete)
			{
				ev.complete = false;
				ev_add_reason(&ev, WER_VALUE_TRUNCATED);
				mine_format_insert(&ev, op_text, sizeof(op_text));
			}
			ev.op_text = op_text;
		}
		else
			ev.op_text = NULL;		/* event mode: SQL is a downstream formatter */
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

/* ===================== rewrite/truncate/drop boundary ===================== */

/*
 * A COMMIT record carries, in xl_xact_parsed_commit.xlocators, the set of
 * relfilenodes this transaction drops at commit.  This is the authoritative
 * boundary signal for TRUNCATE, DROP, CLUSTER/VACUUM FULL and rewriting ALTER
 * under any wal_level (under wal_level=replica there is no XLOG_HEAP_TRUNCATE
 * record at all; the destructive effect is realized through this drop list plus
 * a pg_class relfilenode swap).
 *
 * For every dropped relfilenode we were tracking we (1) invalidate its
 * relfilenode->relid/name mapping and the descriptor reachable via the
 * relfilenode==oid fallback, so no later record decodes through a stale mapping
 * after the relfilenode value is reused, and (2) emit one explicit forensic
 * boundary event.  This runs only on COMMIT, only when nrels > 0, and is
 * O(nrels) with O(1) per-locator invalidation; it adds no per-row cost to the
 * INSERT/EVENT hot path.
 */
static void
we_apply_drop_boundaries(WalExtractContext *ctx, xl_xact_parsed_commit *parsed,
						 XLogRecPtr commit_lsn, TransactionId topxid)
{
	int			i;

	for (i = 0; i < parsed->nrels; i++)
	{
		RelFileLocator loc = parsed->xlocators[i];
		Oid			relfile = loc.relNumber;
		Oid			prev;
		char		oldname[64];
		ChangeEvent ev;
		char		op_text[256];
		size_t		pos = 0;

		/* single-db scope: only act within the bound database */
		if (ctx->bound_db != InvalidOid && loc.dbOid != ctx->bound_db)
			continue;

		/*
		 * Invalidate unconditionally (cheap, O(1)); this upholds the invariant
		 * even for a relfilenode learned only partially.  Capture the prior
		 * identity first so we can describe the boundary.
		 */
		prev = mine_relfile_invalidate(ctx, relfile, oldname, sizeof(oldname));
		mine_desc_invalidate(ctx, relfile);

		/* surface a boundary only for relfilenodes we were actually tracking */
		if (prev == InvalidOid && oldname[0] == '\0')
			continue;

		memset(&ev, 0, sizeof(ev));
		ev.record_lsn = commit_lsn;
		ev.commit_lsn = commit_lsn;
		ev.xid = topxid;
		ev.db_oid = loc.dbOid;
		ev.rel_oid = prev;
		ev.relfilenode = relfile;
		ev.relname = oldname[0] ? oldname : NULL;
		ev.op = WCO_DDL_CATALOG;
		ev.complete = false;
		ev_add_reason(&ev, WER_REWRITE_BOUNDARY);
		we_appendf(op_text, sizeof(op_text), &pos,
				   "-- boundary: RELFILENODE_DROP/REWRITE relfilenode=%u db=%u",
				   relfile, loc.dbOid);
		if (oldname[0])
		{
			we_appendf(op_text, sizeof(op_text), &pos, " was rel ");
			we_append_text_oneline(op_text, sizeof(op_text), &pos, oldname);
		}
		ev.op_text = op_text;
		we_deliver(ctx, &ev);
	}
}

/* ===================== P2A ChangeBatch (machine path) ===================== */

typedef struct WeBufBatch
{
	struct WeBufBatch *next;
	size_t		bytes;			/* size of this single allocation (node + arena) */
	ChangeBatch b;				/* cols[] + column arrays live in the trailing arena */
} WeBufBatch;

static int
we_desc_nlogical(const MineRelDesc *d)
{
	int			n = 0,
				i;

	for (i = 0; i < d->ncols; i++)
		if (!d->cols[i].attisdropped)
			n++;
	return n;
}

/*
 * Allocate one columnar batch for a MULTI_INSERT record in a single arena:
 * ChangeVector array, then per logical column a null bitmap plus either a flat
 * fixed-width value array or a varlena offset array + blob.  Typing is snapshot
 * from the descriptor (no per-row attname, no descriptor deref on the consumer).
 * d == NULL => schema_missing batch (rows counted only, no typed columns).
 */
static WeBufBatch *
we_batch_begin(WalExtractContext *ctx, Oid relid, Oid relfile,
			   MineRelDesc *d, int nrows, Size total_payload)
{
	int			nlog = d ? we_desc_nlogical(d) : 0;
	size_t		nbb = ((size_t) ((nrows + 63) / 64)) * 8;
	size_t		arena = MAXALIGN((size_t) nlog * sizeof(ChangeVector));
	size_t		total;
	WeBufBatch *bb;
	char	   *cur;
	int			i,
				lj;

	if (d)
	{
		for (i = 0; i < d->ncols; i++)
		{
			MineCol    *c = &d->cols[i];

			if (c->attisdropped)
				continue;
			arena += MAXALIGN(nbb);
			if (c->attlen > 0)
				arena += MAXALIGN((size_t) nrows * c->attlen);
			else
			{
				arena += MAXALIGN((size_t) (nrows + 1) * sizeof(uint32));
				arena += MAXALIGN(total_payload);	/* per-column varblob upper bound */
			}
		}
	}
	total = sizeof(WeBufBatch) + arena;

	if (ctx->bbuf_bytes + total > (Size) WALEXTRACT_MAX_BUFFERED_BYTES)
	{
		ctx->status = WALEXTRACT_FATAL_BUFFER_OVERFLOW;
		return NULL;
	}
	bb = (WeBufBatch *) malloc(total);
	if (bb == NULL)
	{
		ctx->status = WALEXTRACT_FATAL_OOM;
		return NULL;
	}
	memset(bb, 0, sizeof(WeBufBatch));
	bb->bytes = total;
	bb->b.record_lsn = ctx->cur_lsn;
	bb->b.commit_lsn = InvalidXLogRecPtr;
	bb->b.xid = ctx->cur_xid;
	bb->b.db_oid = ctx->bound_db;
	bb->b.rel_oid = relid;
	bb->b.relfilenode = relfile;
	bb->b.nrows = nrows;
	bb->b.ncols = nlog;
	bb->b.schema_missing = (d == NULL);

	if (d == NULL)
		return bb;

	cur = (char *) (bb + 1);
	bb->b.cols = (ChangeVector *) cur;
	cur += MAXALIGN((size_t) nlog * sizeof(ChangeVector));
	memset(bb->b.cols, 0, (size_t) nlog * sizeof(ChangeVector));

	lj = 0;
	for (i = 0; i < d->ncols; i++)
	{
		MineCol    *c = &d->cols[i];
		ChangeVector *v;

		if (c->attisdropped)
			continue;
		v = &bb->b.cols[lj++];
		v->attlen = c->attlen;
		v->byval = c->attbyval;
		v->attalign = c->attalign;
		v->typid = c->atttypid;
		v->nullbits = (uint64 *) cur;
		memset(cur, 0, nbb);
		cur += MAXALIGN(nbb);
		if (c->attlen > 0)
		{
			v->values = (uint8 *) cur;
			cur += MAXALIGN((size_t) nrows * c->attlen);
		}
		else
		{
			v->varoff = (uint32 *) cur;
			cur += MAXALIGN((size_t) (nrows + 1) * sizeof(uint32));
			v->varblob = (uint8 *) cur;
			cur += MAXALIGN(total_payload);
			v->varoff[0] = 0;
		}
	}
	return bb;
}

/* deform one tuple into batch row `row` (column-major scatter); single pass */
static void
we_batch_row(WeBufBatch *bb, MineRelDesc *d, int row, HeapTupleHeader htup)
{
	int			natts = HeapTupleHeaderGetNatts(htup);
	bool		hasnulls = (htup->t_infomask & HEAP_HASNULL) != 0;
	uint8	   *bp = htup->t_bits;
	char	   *tp = (char *) htup + htup->t_hoff;
	long		off = 0;
	int			i,
				lj = 0;

	for (i = 0; i < d->ncols && i < natts; i++)
	{
		MineCol    *c = &d->cols[i];
		bool		isnull = (hasnulls && att_isnull(i, bp));
		ChangeVector *v;
		char	   *vptr;
		Size		rawlen;

		if (c->attisdropped)
		{
			if (!isnull)
			{
				if (c->attlen == -1)
					off = att_align_pointer(off, c->attalign, -1, tp + off);
				else if (c->attlen != -2)
					off = att_align_nominal(off, c->attalign);
				if (c->attlen == -1)
					off += VARSIZE_ANY(tp + off);
				else if (c->attlen == -2)
					off += strlen(tp + off) + 1;
				else
					off += c->attlen;
			}
			continue;
		}

		v = &bb->b.cols[lj++];
		if (isnull)
		{
			v->nullbits[row >> 6] |= (uint64) 1 << (row & 63);
			if (c->attlen <= 0)
				v->varoff[row + 1] = v->varoff[row];
			continue;
		}

		if (c->attlen == -1)
			off = att_align_pointer(off, c->attalign, -1, tp + off);
		else if (c->attlen != -2)
			off = att_align_nominal(off, c->attalign);
		vptr = tp + off;

		if (c->attlen > 0)
		{
			rawlen = (Size) c->attlen;
			memcpy(v->values + (size_t) row * c->attlen, vptr, rawlen);
		}
		else
		{
			/*
			 * varlena/cstring: copy the inline datum verbatim.  An on-disk
			 * TOAST pointer is copied as-is (VARSIZE_ANY = pointer length, fully
			 * present in WAL) and the batch is flagged partial -- TOAST
			 * reassembly is out of P2A scope, but the value is never silently
			 * presented as complete.
			 */
			if (c->attlen == -1 && VARATT_IS_EXTERNAL(vptr))
				bb->b.has_external_toast = true;
			rawlen = (c->attlen == -1) ? VARSIZE_ANY(vptr) : (Size) strlen(vptr) + 1;
			memcpy(v->varblob + v->varoff[row], vptr, rawlen);
			v->varoff[row + 1] = v->varoff[row] + (uint32) rawlen;
		}
		off += rawlen;
	}
	/* logical columns the tuple does not reach (added-after-insert): NULL */
	for (; i < d->ncols; i++)
	{
		ChangeVector *v;

		if (d->cols[i].attisdropped)
			continue;
		v = &bb->b.cols[lj++];
		v->nullbits[row >> 6] |= (uint64) 1 << (row & 63);
		if (v->attlen <= 0)
			v->varoff[row + 1] = v->varoff[row];
	}
}

static void
we_batch_commit(WalExtractContext *ctx, WeBufBatch *bb)
{
	if (bb == NULL)
		return;
	if (ctx->bbuf_tail == NULL)
		ctx->bbuf_head = ctx->bbuf_tail = bb;
	else
	{
		ctx->bbuf_tail->next = bb;
		ctx->bbuf_tail = bb;
	}
	ctx->bbuf_nlive++;
	ctx->bbuf_bytes += bb->bytes;
	if (ctx->bbuf_bytes > ctx->bbuf_bytes_peak)
		ctx->bbuf_bytes_peak = ctx->bbuf_bytes;
}

static void
we_bbatch_node_free(WalExtractContext *ctx, WeBufBatch *bb)
{
	ctx->bbuf_bytes -= bb->bytes;
	ctx->bbuf_nlive--;
	free(bb);
}

/* COMMIT: deliver family batches (unless relfilenode dropped same-tx), then drop */
static void
we_flush_batches(WalExtractContext *ctx, TransactionId topxid,
				 const TransactionId *subxacts, int nsub, XLogRecPtr commit_lsn,
				 const xl_xact_parsed_commit *parsed)
{
	WeBufBatch *node = ctx->bbuf_head;
	WeBufBatch *prev = NULL;

	while (node != NULL)
	{
		WeBufBatch *next = node->next;

		if (we_xid_in_family(node->b.xid, topxid, subxacts, nsub))
		{
			if (!we_commit_drops_relfile(ctx, parsed, node->b.relfilenode))
			{
				node->b.commit_lsn = commit_lsn;
				if (ctx->batch_emit_cb)
					ctx->batch_emit_cb(&node->b, ctx->batch_emit_sink);
			}
			if (prev == NULL)
				ctx->bbuf_head = next;
			else
				prev->next = next;
			if (ctx->bbuf_tail == node)
				ctx->bbuf_tail = prev;
			we_bbatch_node_free(ctx, node);
		}
		else
			prev = node;
		node = next;
	}
}

/* ABORT: drop family batches without delivery */
static void
we_discard_batches(WalExtractContext *ctx, TransactionId topxid,
				   const TransactionId *subxacts, int nsub)
{
	WeBufBatch *node = ctx->bbuf_head;
	WeBufBatch *prev = NULL;

	while (node != NULL)
	{
		WeBufBatch *next = node->next;

		if (we_xid_in_family(node->b.xid, topxid, subxacts, nsub))
		{
			if (prev == NULL)
				ctx->bbuf_head = next;
			else
				prev->next = next;
			if (ctx->bbuf_tail == node)
				ctx->bbuf_tail = prev;
			we_bbatch_node_free(ctx, node);
		}
		else
			prev = node;
		node = next;
	}
}

static void
we_bbuf_free_all(WalExtractContext *ctx)
{
	WeBufBatch *node = ctx->bbuf_head;

	while (node != NULL)
	{
		WeBufBatch *next = node->next;

		free(node);
		node = next;
	}
	ctx->bbuf_head = ctx->bbuf_tail = NULL;
	ctx->bbuf_nlive = 0;
	ctx->bbuf_bytes = 0;
}

/*
 * Batch mode is a machine stream and must never silently drop a user record it
 * cannot represent.  Record a fatal status + stable reason; walextract_record()
 * stops processing on the next entry and the frontend surfaces it as an error.
 */
static void
we_batch_unsupported(WalExtractContext *ctx, const char *reason)
{
	if (ctx->status == WALEXTRACT_OK)
	{
		ctx->status = WALEXTRACT_FATAL_UNSUPPORTED_BATCH;
		ctx->batch_unsupported_reason = reason;
	}
}

/*
 * Machine-mode trust gate for a user-DML heap record.  Returns true (and fails
 * the stream closed) when the record must not flow into the machine stream:
 *   UNKNOWN -> unknown_dictionary  (untrusted relfilenode: never guess "user")
 *   USER    -> op_reason           (trusted user table, op not representable yet)
 * INTERNAL/CATALOG return false: the caller continues (internal skipped, catalog
 * learned).  This is the single place machine mode decides relfilenode trust.
 */
static bool
we_batch_user_dml_blocked(WalExtractContext *ctx, Oid relfile, const char *op_reason)
{
	switch (we_rel_trust(ctx, relfile))
	{
		case WX_REL_UNKNOWN:
			we_batch_unsupported(ctx, WEB_UNKNOWN_DICTIONARY);
			return true;
		case WX_REL_USER:
			we_batch_unsupported(ctx, op_reason);
			return true;
		case WX_REL_INTERNAL:
		case WX_REL_CATALOG:
			return false;
	}
	return false;
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

	if (ctx->status != WALEXTRACT_OK)
		return;					/* failed closed: stop processing */

	/* single-active mode invariant: event XOR batch, never a tee (see header) */
	Assert(!(ctx->emit_cb && ctx->batch_emit_cb));

	/*
	 * Transaction boundary: a COMMIT delivers the transaction's buffered
	 * events (with commit_lsn) in WAL order; an ABORT discards them.  Both use
	 * the record's own authoritative subxact list, so subtransaction children
	 * are flushed/discarded with their parent without a separate savepoint
	 * model.  Prepared (2PC) records are out of scope in v0 and fail closed.
	 */
	if (rmid == RM_XACT_ID)
	{
		uint8		xact_info = XLogRecGetInfo(record);
		uint8		xact_op = xact_info & XLOG_XACT_OPMASK;
		TransactionId topxid = XLogRecGetXid(record);

		if (xact_op == XLOG_XACT_COMMIT)
		{
			xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
			xl_xact_parsed_commit parsed;

			ParseCommitRecord(xact_info, xlrec, &parsed);
			/*
			 * Boundary safety: flush inspects parsed.xlocators and suppresses
			 * buffered DML of any relfilenode this transaction drops/rewrites,
			 * so a same-transaction INSERT+TRUNCATE/DROP is not delivered as an
			 * apply candidate.  The boundary pass then invalidates the stale
			 * mapping for future records and emits the explicit boundary event.
			 */
			we_flush_family(ctx, topxid, parsed.subxacts, parsed.nsubxacts,
							record->ReadRecPtr, &parsed);
			we_flush_batches(ctx, topxid, parsed.subxacts, parsed.nsubxacts,
							 record->ReadRecPtr, &parsed);
			we_apply_drop_boundaries(ctx, &parsed, record->ReadRecPtr, topxid);
		}
		else if (xact_op == XLOG_XACT_ABORT)
		{
			xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
			xl_xact_parsed_abort parsed;

			ParseAbortRecord(xact_info, xlrec, &parsed);
			we_discard_family(ctx, topxid, parsed.subxacts, parsed.nsubxacts);
			we_discard_batches(ctx, topxid, parsed.subxacts, parsed.nsubxacts);
		}
		else if (xact_op == XLOG_XACT_PREPARE ||
				 xact_op == XLOG_XACT_COMMIT_PREPARED ||
				 xact_op == XLOG_XACT_ABORT_PREPARED)
		{
			/*
			 * Prepared transactions split the boundary across PREPARE and a
			 * later COMMIT/ABORT PREPARED whose family root is the parsed
			 * twophase_xid, not the record xid.  Tracking that correctly is a
			 * policy decision deferred past v0; mishandling it would either
			 * drop committed data or misroute a family, so fail closed.
			 */
			ctx->status = WALEXTRACT_FATAL_TWOPHASE;
		}
		/* ASSIGNMENT / INVALIDATIONS / others: not tx boundaries -> ignore */
		return;
	}

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

		/*
		 * Batch mode covers only HEAP2 MULTI_INSERT.  A single user INSERT
		 * would otherwise be buffered as a ChangeEvent and then no-op delivered
		 * (event sink is absent in batch mode), silently dropping a user row.
		 * Single-INSERT coalescing is P3; until then, fail closed.
		 */
		if (ctx->batch_emit_cb &&
			we_batch_user_dml_blocked(ctx, rloc.relNumber, WEB_SINGLE_INSERT_UNSUPPORTED))
			return;

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

		/*
		 * Machine batch path (P2A): a MULTI_INSERT of a user relation becomes ONE
		 * ChangeBatch.  Catalog multi-inserts still go per-tuple below so the
		 * dictionary keeps learning.  Single-active mode: when a batch sink is
		 * registered the event sink is absent, so no tee.
		 */
		if (ctx->batch_emit_cb)
		{
			WalExtractRelTrust trust = we_rel_trust(ctx, rloc.relNumber);

			if (trust == WX_REL_UNKNOWN)
			{
				we_batch_unsupported(ctx, WEB_UNKNOWN_DICTIONARY);
				return;
			}
			if (trust == WX_REL_USER)
			{
				Oid			relfile = rloc.relNumber;
				Oid			relid = mine_relfile_get(ctx, relfile);
				MineRelDesc *d;
				WeBufBatch *bb;
				int			row = 0;

				d = mine_desc_get(ctx, relid, false);

				if (ptr == NULL)
				{
					char		pagebuf[BLCKSZ];

					bb = we_batch_begin(ctx, relid, relfile, d, xlrec->ntuples, BLCKSZ);
					if (bb != NULL && XLogRecHasBlockImage(record, 0) &&
						RestoreBlockImage(record, 0, pagebuf))
					{
						for (i = 0; i < xlrec->ntuples; i++)
						{
							ItemId		lp = PageGetItemId((Page) pagebuf, xlrec->offsets[i]);

							if (!ItemIdIsNormal(lp))
								continue;
							if (d != NULL)
								we_batch_row(bb, d, row, (HeapTupleHeader) PageGetItem((Page) pagebuf, lp));
							row++;
						}
					}
				}
				else
				{
					bb = we_batch_begin(ctx, relid, relfile, d, xlrec->ntuples, datalen);
					for (i = 0; bb != NULL && i < xlrec->ntuples; i++)
					{
						xl_multi_insert_tuple *xlhdr;
						char		tupbuf[2 * BLCKSZ];
						HeapTupleHeader htup = (HeapTupleHeader) tupbuf;

						ptr = (char *) SHORTALIGN(ptr);
						xlhdr = (xl_multi_insert_tuple *) ptr;
						ptr += SizeOfMultiInsertTuple;
						if (d != NULL &&
							xlhdr->datalen <= 2 * BLCKSZ - SizeofHeapTupleHeader)
						{
							memset(htup, 0, SizeofHeapTupleHeader);
							memcpy((char *) htup + SizeofHeapTupleHeader, ptr, xlhdr->datalen);
							htup->t_infomask2 = xlhdr->t_infomask2;
							htup->t_infomask = xlhdr->t_infomask;
							htup->t_hoff = xlhdr->t_hoff;
							we_batch_row(bb, d, row, htup);
						}
						row++;
						ptr += xlhdr->datalen;
					}
				}
				if (bb != NULL)
				{
					bb->b.nrows = (d != NULL) ? row : xlrec->ntuples;
					we_batch_commit(ctx, bb);
				}
				return;
			}
		}

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

		/* batch mode: user UPDATE is not representable as a ChangeBatch -> fail closed */
		if (ctx->batch_emit_cb &&
			we_batch_user_dml_blocked(ctx, rloc.relNumber, WEB_UPDATE_UNSUPPORTED))
			return;

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
				mine_catalog_attr_mutation(ctx, rloc.relNumber, nud, nlen);
			}
			else if (catalog_trunc)
			{
				strlcpy(newid, "<truncated>", sizeof(newid));

				/*
				 * Opaque in-range pg_attribute UPDATE: the new tuple could not
				 * be reconstructed (no cached old tuple -> the relation is
				 * pre-range / primed).  We cannot tell whether a decode-relevant
				 * field changed, so fail closed conservatively for the rest of
				 * the range rather than keep decoding with possibly-stale
				 * descriptors.  pg_class is excluded: its opaque in-range
				 * UPDATEs (relfrozenxid / relpages / stats from autovacuum) do
				 * not change the deform contract, and real drops/rewrites are
				 * handled by the smgr rewrite/drop boundary.
				 */
				if (rloc.relNumber == ctx->pgattr_fn)
					mine_invalidate_all(ctx);
			}
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

		/* batch mode: user DELETE is not representable as a ChangeBatch -> fail closed */
		if (ctx->batch_emit_cb &&
			we_batch_user_dml_blocked(ctx, rloc.relNumber, WEB_DELETE_UNSUPPORTED))
			return;

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
	{
		we_buf_free_all(ctx);
		we_bbuf_free_all(ctx);
		free(ctx);
	}
}

void
walextract_context_reset(WalExtractContext *ctx)
{
	WalExtractEmit cb = ctx->emit_cb;
	void	   *sink = ctx->emit_sink;
	WalExtractEmitBatch bcb = ctx->batch_emit_cb;
	void	   *bsink = ctx->batch_emit_sink;
	bool		boot = ctx->do_bootstrap;
	Oid			tdb = ctx->target_db;
	WalExtractRenderMode mode = ctx->render_mode;
	char		pg[1024];

	we_buf_free_all(ctx);		/* drop any open-transaction buffer first */
	we_bbuf_free_all(ctx);
	strlcpy(pg, ctx->pgdata, sizeof(pg));
	memset(ctx, 0, sizeof(*ctx));
	ctx->emit_cb = cb;
	ctx->emit_sink = sink;
	ctx->batch_emit_cb = bcb;
	ctx->batch_emit_sink = bsink;
	ctx->do_bootstrap = boot;
	ctx->target_db = tdb;
	ctx->render_mode = mode;
	strlcpy(ctx->pgdata, pg, sizeof(ctx->pgdata));
}

void
walextract_set_emit(WalExtractContext *ctx, WalExtractEmit cb, void *sink)
{
	ctx->emit_cb = cb;
	ctx->emit_sink = sink;
	/* single-active mode: installing the event sink disables batch mode */
	ctx->batch_emit_cb = NULL;
	ctx->batch_emit_sink = NULL;
}

Size
walextract_buf_peak(const WalExtractContext *ctx)
{
	return ctx->buf_bytes_peak;
}

WalExtractActiveMode
walextract_active_mode(const WalExtractContext *ctx)
{
	/* single-active mode is enforced by the setters; report the live sink */
	if (ctx->batch_emit_cb != NULL)
		return WX_MODE_BATCH;
	if (ctx->emit_cb != NULL)
		return WX_MODE_EVENT;
	return WX_MODE_NONE;
}

void
walextract_prime_rel(WalExtractContext *ctx, Oid relfile, Oid relid,
					 char relkind, const char *relname)
{
	mine_relfile_put(ctx, relfile, relid, relkind, relname);
}

void
walextract_prime_attr(WalExtractContext *ctx, Oid relid, int16 attnum, Oid atttypid,
					  int16 attlen, bool attbyval, char attalign, bool attisdropped,
					  const char *attname)
{
	mine_desc_add_col(ctx, relid, attnum, atttypid, attlen, attbyval, attalign,
					  attisdropped, attname);
}

void
walextract_set_dict_primed(WalExtractContext *ctx, bool primed)
{
	ctx->dict_primed = primed;
}

bool
walextract_dict_primed(const WalExtractContext *ctx)
{
	return ctx->dict_primed;
}

Size
walextract_bbuf_peak(const WalExtractContext *ctx)
{
	return ctx->bbuf_bytes_peak;
}

void
walextract_set_emit_batch(WalExtractContext *ctx, WalExtractEmitBatch cb, void *sink)
{
	ctx->batch_emit_cb = cb;
	ctx->batch_emit_sink = sink;
	/* single-active mode: installing the batch sink disables event mode */
	ctx->emit_cb = NULL;
	ctx->emit_sink = NULL;
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
walextract_set_render_mode(WalExtractContext *ctx, WalExtractRenderMode mode)
{
	ctx->render_mode = mode;
}

void
walextract_set_target_db(WalExtractContext *ctx, Oid dboid)
{
	ctx->target_db = dboid;
}

/* ===================== assembler status (fail-closed) ===================== */

bool
walextract_failed(const WalExtractContext *ctx)
{
	return ctx->status != WALEXTRACT_OK;
}

WalExtractStatus
walextract_status(const WalExtractContext *ctx)
{
	return ctx->status;
}

const char *
walextract_status_message(const WalExtractContext *ctx)
{
	switch (ctx->status)
	{
		case WALEXTRACT_OK:
			return "ok";
		case WALEXTRACT_FATAL_BUFFER_OVERFLOW:
			return "transaction buffer overflow: too many uncommitted events buffered";
		case WALEXTRACT_FATAL_OOM:
			return "out of memory while buffering transaction events";
		case WALEXTRACT_FATAL_TWOPHASE:
			return "prepared (two-phase) transaction records are not supported";
		case WALEXTRACT_FATAL_ABORTED_DDL:
			return "aborted transaction had already mutated the mined dictionary (non-transactional dictionary cannot be rolled back)";
		case WALEXTRACT_FATAL_UNSUPPORTED_BATCH:
			return ctx->batch_unsupported_reason
				? ctx->batch_unsupported_reason
				: "batch mode encountered an unsupported user-DML record";
	}
	return "unknown";
}
