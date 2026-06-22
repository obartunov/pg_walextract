# pg_walextract — physical WAL miner (ChangeEvent + payload)

Materialize DDL/DML *effects* from PostgreSQL WAL using only the WAL records
plus a base dictionary recovered from the cluster. Two frontends share one core:
a `pg_waldump`-derived binary and an in-server SRF (`pg_walinspect`-style).

This is a **physical WAL miner foundation**, not a logical-decoding replacement.

    WAL record   != committed change
    physical tuple != logical DML
    catalog tuple  != original DDL
    op_text        != source of truth

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
complete, reasons text[], op_text`.

## 5. Tests

`ext/sql/walextract.sql` (`pg_regress`, expected in `ext/expected/`) is the
smoke contract; `REGRESS = walextract` is wired in `ext/Makefile`:

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

## 6. Privilege

`walextract_wal2sql()` reconstructs user table data from WAL, so it is
restricted like `pg_walinspect`: a C-level check requires privileges of
`pg_read_server_files` (superusers inherit), and the SQL script revokes EXECUTE
from PUBLIC and grants it to `pg_read_server_files`.

## 7. Not production / not in scope

- **Commit order, abort, subxact are NOT handled.** Events are physical WAL
  records in WAL order; `commit_lsn` is always Invalid. A `TxAssembler` is the
  next task and is required before any apply path.
- DDL is reported as a catalog effect, not reconstructed original SQL.
- UPDATE/DELETE of user tables are not decoded (catalog UPDATE/DELETE only).
- TOAST values are not reassembled; toasted columns are marked, not recovered.
- No rewrite tracking: after VACUUM FULL/CLUSTER/ALTER the relfilenode moves and
  later DML decodes as `dictionary_missing` until the new mapping is seen.
- Offline catalog bootstrap (`MINE_BOOTSTRAP`) is a dev guard: safe only on a
  quiesced/offline PGDATA.
- Catalog struct layouts are version-coupled; no CRC validation.
- The SRF context is allocated per call (~14 MB) and is PARALLEL UNSAFE.

ProGate / ProCopy must consume the structured `ChangeEvent` payload (filtered by
`db_oid`/`rel_oid`, gated on `complete`, honoring `reasons`), not the `op_text`
string.

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
