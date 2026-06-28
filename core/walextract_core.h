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
#define WER_UNKNOWN_DICTIONARY	"unknown_dictionary"	/* relfilenode/relkind not in
														 * primed/learned dictionary */
#define WER_SCHEMA_MISSING		"schema_missing"
#define WER_TOO_MANY_COLUMNS	"too_many_columns"
#define WER_VALUE_TRUNCATED		"value_truncated"
#define WER_CATALOG_TRUNCATED	"catalog_truncated"
#define WER_REWRITE_BOUNDARY	"rewrite_or_drop_boundary"

/*
 * Batch-mode (machine path) unsupported-record reasons.  Batch mode is a
 * machine stream and must never silently return a partial stream: a user-DML
 * record the batch path cannot yet represent fails closed with one of these as
 * the status reason (see walextract_status_message).  String identity is
 * significant; do not strdup.
 */
#define WEB_SINGLE_INSERT_UNSUPPORTED	"batch_single_insert_unsupported"
#define WEB_UPDATE_UNSUPPORTED			"batch_update_unsupported"
#define WEB_DELETE_UNSUPPORTED			"batch_delete_unsupported"
#define WEB_UNKNOWN_DICTIONARY			"unknown_dictionary"	/* machine mode: relation
																 * not trusted (unprimed) */

/*
 * UPDATE/DELETE machine identity reasons (explicit-evidence policy v0).
 * A row is identity-safe only when the WAL record itself carries old identity
 * (XLH_*_CONTAINS_OLD_TUPLE / _OLD_KEY).  A bare new-tuple key is NOT identity:
 * at wal_level=replica no old identity is logged, and that record cannot be
 * distinguished from a logical key-unchanged update, so inferring identity from
 * the new tuple alone would silently corrupt on apply.
 */
#define WEB_NO_EXPLICIT_OLD_IDENTITY	"no_explicit_old_identity"	/* no CONTAINS_OLD_* */
#define WEB_HOT_UPDATE_UNSUPPORTED		"hot_update_unsupported"
#define WEB_DML_EMIT_PENDING			"dml_emit_pending"	/* explicit identity present;
															 * ChangeDmlBatch emission is the
															 * next increment (temp dev guard) */
#define WEB_IDENTITY_METADATA_MISSING	"identity_metadata_missing"	/* no v2 I record
												 * for this relfile */
#define WEB_REPLICA_IDENTITY_NOTHING	"replica_identity_nothing"	/* relreplident=n:
												 * relation policy says no identity */
#define WEB_IDENTITY_KEY_MISSING		"identity_key_missing"	/* metadata says no usable
												 * identity key (e.g. DEFAULT
												 * with no primary key) */

/*
 * 2b read-only decode outcomes.  Reached only after we_identity_gate returned
 * WEB_DML_EMIT_PENDING for a trusted USER relation (record carried explicit
 * CONTAINS_OLD_TUPLE/_OLD_KEY).  The old-identity payload is reconstructed
 * read-only by mirroring DecodeXLogTuple and deformed against the trusted
 * descriptor to prove the identity material is fully present.  No emission,
 * no reassembly, no TOAST fetch, no new-tuple inference: decode either proves
 * the identity material decodable (decoded_identity_ready, still gated for 2c
 * emission) or fails closed with one of the named reasons below.
 */
#define WEB_DECODED_IDENTITY_READY		"decoded_identity_ready"		/* old key/tuple
												 * fully decoded + validated; emission is 2c */
#define WEB_IDENTITY_EXTERNAL_TOAST		"identity_external_toast"	/* an identity datum is
												 * an on-disk TOAST pointer (not reassembled) */
#define WEB_IDENTITY_INCOMPLETE			"identity_incomplete"		/* payload underflows
												 * the declared tuple layout */
#define WEB_TUPLE_RECONSTRUCTION_UNSUPPORTED	"tuple_reconstruction_unsupported"	/* no
												 * usable descriptor to deform against */
#define WEB_DESCRIPTOR_INVALIDATED		"descriptor_invalidated"	/* descriptor invalidated
												 * at a rewrite/drop boundary */

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
	bool		has_external_toast;	/* >=1 value is an on-disk TOAST pointer (inline
									 * datum copied verbatim, NOT reassembled): the
									 * batch is PARTIAL and the consumer must detoast.
									 * TOAST reassembly is out of P2A scope. */
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
	WALEXTRACT_FATAL_ABORTED_DDL,		/* aborted txn already mutated the (non-txnal) dictionary */
	WALEXTRACT_FATAL_UNSUPPORTED_BATCH	/* batch mode hit a user-DML record it cannot
										 * represent (single INSERT / UPDATE / DELETE);
										 * fail closed instead of dropping it silently */
} WalExtractStatus;

typedef enum WalExtractRenderMode
{
	WX_RENDER_SQL = 0,			/* default: render SQL literal/op_text (debug/compat) */
	WX_RENDER_EVENT				/* product hot path: raw typed payload, no SQL rendering */
} WalExtractRenderMode;

typedef struct WalExtractContext WalExtractContext;
typedef void (*WalExtractEmit) (const ChangeEvent *ev, void *sink);
typedef void (*WalExtractEmitBatch) (const ChangeBatch *b, void *sink);

/* which output sink is currently active (single-active: never both) */
typedef enum WalExtractActiveMode
{
	WX_MODE_NONE = 0,
	WX_MODE_EVENT,
	WX_MODE_BATCH
} WalExtractActiveMode;

extern WalExtractContext *walextract_context_create(void);
extern void walextract_context_free(WalExtractContext *ctx);
extern void walextract_context_reset(WalExtractContext *ctx);

/*
 * Output sink selection is single-active: event mode XOR batch mode.  There is
 * no tee in P2A.  Each setter installs its own sink and CLEARS the other, so a
 * context delivers either ChangeEvents (forensic/SRF) or ChangeBatches (machine
 * path), never both.  walextract_record() asserts the invariant defensively.
 */
extern void walextract_set_emit(WalExtractContext *ctx, WalExtractEmit cb, void *sink);
extern void walextract_set_emit_batch(WalExtractContext *ctx, WalExtractEmitBatch cb, void *sink);
extern WalExtractActiveMode walextract_active_mode(const WalExtractContext *ctx);

/*
 * Range-start dictionary priming (v0).  A provider (e.g. the in-server SRF
 * reading live catalogs) calls these BEFORE the WAL scan to seed the dictionary
 * for relations created before the decoded range.  walextract_set_dict_primed()
 * marks the dictionary trust state.  Priming is a startup cost, never per-row.
 * Limitation: live-catalog priming is only valid when current catalog state
 * corresponds to the requested range (same relfilenode / not rewritten/dropped).
 */
extern void walextract_prime_rel(WalExtractContext *ctx, Oid relfile, Oid relid,
								 char relkind, const char *relname);
extern void walextract_prime_attr(WalExtractContext *ctx, Oid relid, int16 attnum,
								  Oid atttypid, int16 attlen, bool attbyval,
								  char attalign, bool attisdropped, const char *attname);
#define WX_IDENT_MAXATTS 32		/* == INDEX_MAX_KEYS */
extern void walextract_prime_identity(WalExtractContext *ctx, Oid relfile,
									  char relreplident, const int16 *atts, int natts);
extern void walextract_set_dict_primed(WalExtractContext *ctx, bool primed);
extern bool walextract_dict_primed(const WalExtractContext *ctx);
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
