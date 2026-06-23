/*
 * walextract_core.h - public API of the WAL mining core.
 *
 * Primary output model is the structured ChangeEvent.  op_text is a
 * formatter/debug field derived from it, never the source of truth.
 * All mutable state lives in an explicit WalExtractContext.
 */
#ifndef WALEXTRACT_CORE_H
#define WALEXTRACT_CORE_H

#include "postgres.h"
#include "access/xlogreader.h"

typedef enum WalChangeOp
{
	WCO_UNKNOWN = 0,
	WCO_INSERT,
	WCO_UPDATE,
	WCO_DELETE,
	WCO_DDL_CATALOG
} WalChangeOp;

/* stable reason codes (string identity is significant; do not strdup) */
#define WER_TOAST_EXTERNAL		"toast_external"
#define WER_UNKNOWN_TYPE		"unknown_type"
#define WER_DICTIONARY_MISSING	"dictionary_missing"
#define WER_SCHEMA_MISSING		"schema_missing"
#define WER_TOO_MANY_COLUMNS	"too_many_columns"
#define WER_VALUE_TRUNCATED		"value_truncated"
#define WER_CATALOG_TRUNCATED	"catalog_truncated"
#define WER_REWRITE_BOUNDARY	"rewrite_or_drop_boundary"

#define WALEXTRACT_MAX_REASONS	6
#define WALEXTRACT_MAX_COLS		80

/*
 * One decoded column of an INSERT payload.  This (not op_text) is the source
 * of truth for values: op_text is rendered from these fields.
 */
typedef struct ChangeColumn
{
	int16		attnum;
	Oid			typid;
	const char *attname;		/* mined attribute name (unquoted) */
	bool		isnull;
	bool		complete;		/* false if this value was not faithfully rendered */
	const char *reason;			/* WER_* when !complete, else NULL */
	const char *value_text;		/* v0 renderer result (unquoted SQL literal); NULL if isnull */
	const char *raw_ptr;		/* in-tuple datum; valid ONLY during the emit callback */
	Size		raw_len;		/* length of raw_ptr payload (0 if unknown) */
} ChangeColumn;

/*
 * One physical WAL change.  A WAL record is not a committed change, a physical
 * tuple is not a logical DML, a catalog tuple is not the original DDL: callers
 * (ProGate/ProCopy) must treat this as a physical miner stream, not redo truth.
 */
typedef struct ChangeEvent
{
	XLogRecPtr	record_lsn;		/* LSN of the WAL record */
	XLogRecPtr	commit_lsn;		/* InvalidXLogRecPtr until a TxAssembler sets it */
	TransactionId xid;
	Oid			db_oid;
	Oid			rel_oid;		/* relation OID when known, else relfilenode */
	Oid			relfilenode;
	const char *relname;		/* NULL if unknown */
	const char *schema_name;	/* NULL: schema resolution not implemented yet */
	WalChangeOp op;
	const char *op_text;		/* derived from cols[]; commented out when !complete */
	bool		complete;		/* derived: true only if fully + faithfully decoded */
	const char *reasons[WALEXTRACT_MAX_REASONS];
	int			nreasons;
	int			ncols;			/* INSERT payload column count (0 for non-INSERT) */
	ChangeColumn cols[WALEXTRACT_MAX_COLS];
} ChangeEvent;

/*
 * P2A ChangeBatch (machine consumer path).  One HEAP2 MULTI_INSERT WAL record
 * becomes ONE columnar batch: schema is referenced once (snapshotted typing per
 * column), payload is column-major and arena-owned.  No SQL text, no op_text,
 * no per-row attname, no per-row event.  ChangeEvent is unchanged and remains
 * the forensic/SRF path; ChangeBatch is additive.
 */
typedef struct ChangeVector
{
	int16		attlen;			/* snapshot: >0 fixed, -1 varlena, -2 cstring */
	bool		byval;
	char		attalign;
	Oid			typid;
	/* fixed-len (attlen>0): values is a flat nrows*attlen array (SIMD-addressable) */
	uint8	   *values;
	/* varlena/cstring: varoff[nrows+1] offsets into varblob; values is NULL */
	uint32	   *varoff;
	uint8	   *varblob;
	/* nrows-bit null bitmap, bit set = SQL NULL (value slot then undefined) */
	uint64	   *nullbits;
} ChangeVector;

typedef struct ChangeBatch
{
	XLogRecPtr	record_lsn;		/* one WAL record per batch in v0 */
	XLogRecPtr	commit_lsn;		/* set by TxAssembler at COMMIT */
	TransactionId xid;
	Oid			db_oid;
	Oid			rel_oid;
	Oid			relfilenode;
	int			ncols;			/* 0 when schema_missing */
	int			nrows;
	bool		schema_missing;	/* no descriptor: rows counted, no typed columns */
	ChangeVector *cols;			/* ncols vectors, arena-owned (NULL if schema_missing) */
} ChangeBatch;

/*
 * Transaction-assembler status.  The miner buffers physical ChangeEvents by
 * xid and only emits a transaction's events on its COMMIT record; it must
 * never expose an aborted or still-open transaction as committed.  When the
 * assembler cannot uphold that contract it fails closed: it stops emitting and
 * records a fatal status here, which the frontend must check (walextract_failed)
 * and surface as an error instead of returning partial/uncommitted output.
 */
typedef enum WalExtractStatus
{
	WALEXTRACT_OK = 0,
	WALEXTRACT_FATAL_BUFFER_OVERFLOW,	/* per-call buffered-event cap hit */
	WALEXTRACT_FATAL_OOM,				/* malloc failed while buffering */
	WALEXTRACT_FATAL_TWOPHASE,			/* prepared-xact record: out of scope in v0 */
	WALEXTRACT_FATAL_ABORTED_DDL		/* aborted txn already mutated the (non-txnal) dictionary */
} WalExtractStatus;

typedef enum WalExtractRenderMode
{
	WX_RENDER_SQL = 0,			/* default: render SQL literal/op_text (debug/compat) */
	WX_RENDER_EVENT				/* product hot path: raw typed payload, no SQL rendering */
} WalExtractRenderMode;

typedef struct WalExtractContext WalExtractContext;
typedef void (*WalExtractEmit) (const ChangeEvent *ev, void *sink);
typedef void (*WalExtractEmitBatch) (const ChangeBatch *b, void *sink);

extern WalExtractContext *walextract_context_create(void);
extern void walextract_context_free(WalExtractContext *ctx);
extern void walextract_context_reset(WalExtractContext *ctx);

extern void walextract_set_emit(WalExtractContext *ctx, WalExtractEmit cb, void *sink);
extern void walextract_set_emit_batch(WalExtractContext *ctx, WalExtractEmitBatch cb, void *sink);
extern Size walextract_buf_peak(const WalExtractContext *ctx);
extern Size walextract_bbuf_peak(const WalExtractContext *ctx);
extern void walextract_set_pgdata(WalExtractContext *ctx, const char *pgdata);
extern void walextract_set_filenodes(WalExtractContext *ctx, Oid pgclass_fn, Oid pgattr_fn);
extern void walextract_set_bootstrap(WalExtractContext *ctx, bool on);
extern void walextract_set_target_db(WalExtractContext *ctx, Oid dboid);
extern void walextract_set_render_mode(WalExtractContext *ctx, WalExtractRenderMode mode);

extern void walextract_record(WalExtractContext *ctx, XLogReaderState *record);

extern const char *walextract_op_name(WalChangeOp op);

/* transaction-assembler fatal status (fail-closed); see WalExtractStatus */
extern bool walextract_failed(const WalExtractContext *ctx);
extern WalExtractStatus walextract_status(const WalExtractContext *ctx);
extern const char *walextract_status_message(const WalExtractContext *ctx);

#endif							/* WALEXTRACT_CORE_H */
