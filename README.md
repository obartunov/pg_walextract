# pg_walextract — physical WAL miner (ChangeEvent + payload)

Materialize DDL/DML *effects* from PostgreSQL WAL using only the WAL records
plus a base dictionary recovered from the cluster. Two frontends share one core:
a `pg_waldump`-derived binary and an in-server SRF (`pg_walinspect`-style).

This is a **physical WAL miner foundation**, not a logical-decoding replacement.

    WAL record   != committed change
    physical tuple != logical DML
    catalog tuple  != original DDL
    op_text        != source of truth

## Machine stream (Update/Delete Identity v0)

`pg_walextract` is **not** a `pg_waldump` text parser. The `pg_waldump` sources
are used as reference knowledge for record layouts; `walextract` owns the
structured machine stream and its explicit safety rules.

Three machine modes share one read-only WAL scan:

- **EVENT** — diagnostic / legacy / forensic per-record view (`walextract_wal2sql`,
  `walextract_wal2event_count`); not a complete apply stream by itself.
- **BATCH** — INSERT/COPY `ChangeBatch` (`walextract_wal2batch`,
  `walextract_batch_stats`); xid-buffered, COMMIT-flushed, ABORT-discarded.
- **DMLBATCH** — UPDATE/DELETE `ChangeDmlBatch` (`walextract_wal2dmlbatch`,
  `walextract_dmlbatch_stats`).

Accepted machine stream rules:

- UPDATE/DELETE machine output exists **only** for explicit, decoded, safe old
  identity; anything else fails closed with a precise reason.
- A poisoned xid family is **never** emitted as a clean apply-safe transaction.
- Detail SRFs are content surfaces; they can omit poisoned xid families.
- Strict stats SRFs are the **completeness gate**: a range is a complete clean
  apply stream only if the matching strict stats SRF succeeds.

Full rules — machine modes, `ChangeBatch`/`ChangeDmlBatch`, explicit
old-identity, WX_SIDECAR 2 identity metadata, live-prime vs sidecar-v2 parity,
the completeness gate, xid-family poison, the supported / fail-closed matrices,
and frozen non-goals — are in
[docs/machine_stream_semantics.md](docs/machine_stream_semantics.md).

## 1. Output model: ChangeEvent with structured payload

The core's primary output is a structured `ChangeEvent` (`core/walextract_core.h`).
For INSERTs the decoded payload is structured per column in `ev.cols[]`
(`ChangeColumn`: attnum, typid, attname, isnull, complete, reason, value_text,
raw_ptr, raw_len). **`op_text` is formatted from `ev.cols[]`**, not produced in
parallel with the decode loop — so the event is the source of truth and
`op_text` is a derived view. `value_text`/`raw_ptr` are valid only during the
emit callback.

`complete` reflects **payload-decode fidelity only**: it is true iff every
column value was rendered faithfully. Honesty reasons attached to a column and
aggregated onto the event:

- external TOAST pointer -> `complete=false`, reason `toast_external`
- type the renderer does not understand -> `complete=false`, reason `unknown_type`
- no column dictionary for the relation -> `complete=false`, reason `dictionary_missing`
- more attributes than descriptor capacity / under-covered descriptor ->
  `complete=false`, reason `too_many_columns`
- a value or statement that overflows the fixed render buffers ->
  `complete=false`, reason `value_truncated`
- a catalog UPDATE whose tuple cannot be reconstructed within the fixed
  reconstruct buffer -> `complete=false` (already), reason `catalog_truncated`

`complete` is never left optimistic on a capacity or truncation boundary (fixed `cols[]`, value, or op_text buffers): hitting one derives `complete=false` with a stable reason. `reasons[]` is a list of caveats, and may be non-empty even when `complete=true`.
In particular every INSERT with a known relation carries `schema_missing`
(see §3). Catalog changes are emitted as `op=DDL_CATALOG`, `complete=false`,
with a `-- catalog: ...` comment as `op_text` (catalog effect, not original DDL).

## 2. State, scope, target database

- All mutable state lives in an explicit `WalExtractContext`; each SRF call uses
  a fresh context. No decoder logic reads process globals.
- **Target database is explicit.** `walextract_set_target_db(ctx, dboid)` binds
  the decode to one database: the extension passes `MyDatabaseId`; the binary
  reads `MINE_DBOID`. Records from other databases (and shared catalogs,
  `dbOid==0`) are skipped. Bind-to-first-database is retained **only** as a
  debug fallback when no target is set, because the first WAL record in a range
  may belong to another database and would otherwise silently capture the wrong
  one.
- The TID-keyed dictionary uses open addressing with `EMPTY/USED/DELETED`
  tombstones, so deletes do not break probe chains.

## 3. Replayability and identifier quoting

`op_text` double-quotes the relation and column identifiers (e.g.
`INSERT INTO "wx_t" ("id", ...)`), using the mined names. Schema is **not**
resolved in v0, so generated SQL is replayable only against a target where the
relation is reachable by unqualified name under the session `search_path`. This
limitation is surfaced machine-readably as the `schema_missing` reason on every
INSERT event; it is a replay caveat, independent of `complete`.

For an **incomplete** event `op_text` is never rendered as values: it is a
single-line, control-character-free summary of the form
`-- INCOMPLETE INSERT rel="..." reasons={...}`. This guarantees an incomplete
`op_text` is one fully-commented line (a value or identifier containing a
newline cannot escape the `--` comment), and that `value_text` is never read
out of bounds. Use the structured `cols[]` / `reasons[]`, not the comment. `DDL_CATALOG` `op_text` is likewise a single
control-character-free commented line: mined relation/column names (which may
contain newlines) are sanitized, and catalog UPDATE reconstruction is bounded
(out-of-range prefix/suffix/length fails safe to `<truncated>` rather than
reading or writing past the fixed buffer).

## 4. Usage

Binary (offline, over a quiesced PGDATA):

    MINE_PGDATA=$PGDATA MINE_DBOID=<oid> [MINE_BOOTSTRAP=1] \
      ./pg_walextract -q -p $PGDATA/pg_wal -s <start_lsn> -e <end_lsn>

In-server SRF (privileged — see §6):

    CREATE EXTENSION walextract;
    SELECT * FROM walextract_wal2sql('<start_lsn>', '<end_lsn>');

SRF columns: `record_lsn, xid, db_oid, rel_oid, relfilenode, op, relation,
complete, reasons text[], op_text, commit_lsn`.  `commit_lsn` is the LSN of the
COMMIT record that released the event (see §7); it is set on every delivered
event because events are delivered only at COMMIT.

## 5. Tests

`ext/sql/walextract.sql` (`pg_regress`, expected in `ext/expected/`) is the
smoke contract; `ext/sql/walextract_txn.sql` is the transaction-assembler
contract; both are wired via `REGRESS = walextract walextract_txn` in
`ext/Makefile`:

    cd ext && PG_CONFIG=/path/to/pg_config \
      PGHOST=/tmp PGPORT=5444 PGUSER=postgres make installcheck

Deterministic by construction (lsn/xid/oid only in boolean checks or filtered
out). It asserts: supported INSERT -> `complete=true` + quoted, payload-derived
op_text; unsupported type (`numeric`) -> `complete=false`, `unknown_type`;
external TOAST -> `complete=false`, `toast_external`; pre-existing relation with
no mined descriptor -> `complete=false`, `dictionary_missing`; every
`complete=false` event has commented (non-replayable) op_text; xid equals the
inserting tuple xmin; and a round-trip of the complete op_text reproduces the
original rows.

`walextract_txn.sql` asserts the transaction boundary: a committed INSERT
appears only with `commit_lsn` set (after its data record, within range); an
INSERT then ROLLBACK is not emitted; two INSERTs in one transaction are
delivered in WAL order sharing one `commit_lsn`; a transaction still open at the
range end is not emitted; and an ABORT of a transaction that did DDL fails
closed (§7).

## 6. Privilege

`walextract_wal2sql()` reconstructs user table data from WAL, so it is
restricted like `pg_walinspect`: a C-level check requires privileges of
`pg_read_server_files` (superusers inherit), and the SQL script revokes EXECUTE
from PUBLIC and grants it to `pg_read_server_files`.

## 7. Not production / not in scope

- **TxAssembler v0 solves transaction *end*, not transaction *start*.** Events
  are buffered by xid and delivered only on the top-level `COMMIT` record
  observed inside the scanned range, in WAL order, with `commit_lsn` set;
  `ABORT` discards the family; transactions still open at the end of the range
  are not emitted. Subtransactions are flushed/discarded via the commit/abort
  record's own subxact list (no savepoint model).
- **Output is NOT apply-safe for an arbitrary `start_lsn`.** If `start_lsn`
  falls inside an already-running transaction, only its tail is observed; on
  `COMMIT` that tail is emitted as a committed *fragment*. That is correct for
  "we saw COMMIT for the buffered fragment" but is a partial transaction for an
  apply stream. Apply-safe consumption requires a proven safe transaction
  boundary or a future open-transaction-state / checkpoint mechanism
  (RangeStartSafety / WalStreamCheckpoint), which v0 does not provide.
- **Fail-closed guards (dev guards, not final policy).** Prepared (two-phase)
  xact records, and an `ABORT` of a transaction that already mutated the
  *non-transactional* mined dictionary via DDL, both fail closed (the SRF
  errors) rather than risk a partial or mis-decoded result. A transactional
  dictionary overlay is a separate future layer.
- **Binary frontend is diagnostic / non-atomic.** `pg_walextract` prints
  delivered events but emits no framed, atomic per-transaction output; it is not
  an apply source until a framed output format exists.
- DDL is reported as a catalog effect, not reconstructed original SQL.
- UPDATE/DELETE of user tables are decoded as a structured machine stream in
  DMLBATCH mode under the explicit old-identity rules (see
  [docs/machine_stream_semantics.md](docs/machine_stream_semantics.md)); the
  EVENT/`wal2sql` view still reports only catalog UPDATE/DELETE.
- TOAST values are not reassembled; toasted columns are marked, not recovered.
- No rewrite tracking: after VACUUM FULL/CLUSTER/ALTER the relfilenode moves and
  later DML decodes as `dictionary_missing` until the new mapping is seen.
- Offline catalog bootstrap (`MINE_BOOTSTRAP`) is a dev guard: safe only on a
  quiesced/offline PGDATA.
- Catalog struct layouts are version-coupled; no CRC validation.
- The SRF context is allocated per call (~14 MB) and is PARALLEL UNSAFE.

ProGate / ProCopy must consume the structured `ChangeEvent` payload (filtered by
`db_oid`/`rel_oid`, gated on `complete`, honoring `reasons`), not the `op_text`
string. Two stream contracts apply: an **apply stream** must be committed,
complete, and safe-start proven; a **forensic stream** may expose
physical/open/aborted events but must always mark them explicitly. v0 delivers
the forensic shape (committed events observed within the range); the apply shape
additionally requires the range-start safety named above.

## 8. Build / packaging

`ext/Makefile` has no hardcoded `PG_CONFIG` (override on the command line or via
the environment). The shared core is compiled from `../core` via an explicit
rule, so the package builds with the `core/` + `ext/` layout as shipped.

## 9. Files

    core/walextract_core.h         ChangeEvent + ChangeColumn + context API
    core/walextract_core.c         miner core (state in context; structured payload)
    bin/pg_walextract.c            pg_waldump-derived binary frontend
    bin/Makefile.pg_waldump.snippet  build wiring for the binary
    ext/walextract_ext.c           in-server SRF frontend (privilege-guarded)
    ext/walextract.control,
    ext/walextract--1.0.sql,       SRF definition + REVOKE/GRANT
    ext/Makefile                   PGXS packaging (no hardcoded PG_CONFIG)
    ext/sql/walextract.sql,
    ext/expected/walextract.out    smoke contract + expected output
    ext/sql/walextract_txn.sql,
    ext/expected/walextract_txn.out  transaction-assembler contract + expected
