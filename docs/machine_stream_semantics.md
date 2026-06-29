# walextract machine stream semantics

Accepted at tag `walextract-update-delete-identity-v0`.

This document describes the rules of the structured machine stream that
`walextract` produces from PostgreSQL WAL: the machine modes, the
`ChangeBatch`/`ChangeDmlBatch` outputs, the explicit old-identity requirement
for UPDATE/DELETE, the WX_SIDECAR 2 identity metadata, the apply-stream
completeness gate, and the xid-family poison rules. It also records the
supported and fail-closed cases and the frozen non-goals.

`walextract` is not a `pg_waldump` text parser. The `pg_waldump` sources are
used as reference knowledge for record layouts; `walextract` owns the structured
machine stream and its explicit safety rules.

## 1. Machine modes

The extension exposes three machine modes over the same read-only WAL scan. They
are selected by which set-returning function (SRF) is called.

### EVENT
Diagnostic / legacy / forensic-facing per-record event view
(`walextract_wal2sql`, `walextract_wal2event_count`). It renders observed record
effects and is useful for inspection, but it is **not** a complete apply stream
by itself.

### BATCH
INSERT/COPY machine output as `ChangeBatch`
(`walextract_wal2batch` detail, `walextract_batch_stats` strict stats). Rows are
buffered by xid, flushed on the top-level COMMIT in WAL order, and discarded on
ABORT.

### DMLBATCH
UPDATE/DELETE machine output as `ChangeDmlBatch`
(`walextract_wal2dmlbatch` detail, `walextract_dmlbatch_stats` strict stats). A
DML record is emitted **only** when its old identity is explicit, decoded, and
safe (see §3). Batches are buffered by xid, flushed on COMMIT, discarded on
ABORT, and poisoned xid families are suppressed (see §7).

### MACHINEBATCH
The combined machine mode (`walextract_wal2machinebatch` detail,
`walextract_machinebatch_stats` strict stats). One scan emits both families
under **one xid-family safety decision**:

- INSERT/COPY as `ChangeBatch`,
- supported UPDATE/DELETE as `ChangeDmlBatch`.

A clean mixed transaction emits both `INSERT_BATCH` and `DML_BATCH` rows from the
same xid; an unsafe mixed transaction emits **no partial apply-safe family** (the
supported INSERT is suppressed with the poisoned family). This is the mode to use
when a transaction may mix INSERT/COPY with UPDATE/DELETE -- the separate BATCH
and DMLBATCH modes each fail closed on the other's records.
`walextract_machinebatch_stats` is the completeness gate for MACHINEBATCH.

## 2. SQL / SRF surfaces

    walextract_wal2sql(start_lsn, end_lsn, prime, sidecar)            EVENT detail (SQL/event view)
    walextract_wal2event_count(start_lsn, end_lsn, prime, sidecar)    EVENT count
    walextract_wal2batch(start_lsn, end_lsn, prime, sidecar)          BATCH detail (ChangeBatch)
    walextract_batch_stats(start_lsn, end_lsn, prime, sidecar)        BATCH strict stats / completeness gate
    walextract_wal2dmlbatch(start_lsn, end_lsn, prime, sidecar)       DMLBATCH detail (ChangeDmlBatch)
    walextract_dmlbatch_stats(start_lsn, end_lsn, prime, sidecar)     DMLBATCH strict stats / completeness gate
    walextract_wal2machinebatch(start_lsn, end_lsn, prime, sidecar)   MACHINEBATCH detail (kind = INSERT_BATCH / DML_BATCH)
    walextract_machinebatch_stats(start_lsn, end_lsn, prime, sidecar) MACHINEBATCH strict stats / completeness gate
    walextract_export_dictionary()                                    WX_SIDECAR 2 producer (R/A/I records)
    walextract_mode_selftest()                                        build/mode self-test

`walextract_wal2machinebatch` tags each row by `kind` (`INSERT_BATCH` or
`DML_BATCH`); per-kind columns are NULL where not applicable (`nrows` for an
INSERT batch; `op`/`identity_source`/`nident`/`has_new_row`/`incomplete` for a
DML batch). `walextract_machinebatch_stats` returns `insert_batches`,
`insert_rows`, `dml_batches`, `n_update`, `n_delete`, and the TOAST/incomplete
flags, and fails closed on a poisoned family like the other strict stats SRFs.

`prime` (boolean) primes the relation/identity dictionary from the live catalog;
`sidecar` (text) primes it from an exported WX_SIDECAR blob instead (see §6).

## 3. ChangeDmlBatch and the explicit old-identity rule

UPDATE/DELETE are machine-emitted only when the old identity is explicit,
decoded, and safe. `ChangeDmlBatch` carries:

    op:               UPDATE or DELETE
    identity_source:  OLD_KEY (replica identity index key) or
                      OLD_TUPLE (REPLICA IDENTITY FULL old row)
    identity:         the decoded old-identity columns
    new row:          present for a supported UPDATE; absent for DELETE
    flags:
      toast_external: an emitted value is an on-disk TOAST pointer
                      (flagged, copied verbatim, never reassembled)
      incomplete:     the identity could not be safely/fully captured

The old identity is decoded read-only from the WAL record (the replica-identity
key tuple, or the full old tuple for REPLICA IDENTITY FULL). There is no TOAST
reassembly, no TOAST table fetch, and no page-state reconstruction.

## 4. Supported UPDATE/DELETE cases

    UPDATE DEFAULT key-changed     -> OLD_KEY  + new row
    UPDATE REPLICA IDENTITY FULL   -> OLD_TUPLE + new row
    DELETE DEFAULT                 -> OLD_KEY
    DELETE REPLICA IDENTITY FULL   -> OLD_TUPLE

    moderate external identity     -> PostgreSQL flattens the old identity inline
                                      before WAL logging (ExtractReplicaIdentity
                                      calls toast_flatten_tuple); walextract
                                      decodes it inline.
    new-row external TOAST         -> flagged toast_external, copied verbatim,
                                      not reassembled.

## 5. Fail-closed UPDATE/DELETE cases

Each fail-closed case carries a precise reason; reasons are never collapsed into
a generic token.

    UPDATE key-unchanged           -> no_explicit_old_identity
    HOT UPDATE                     -> hot_update_unsupported
    REPLICA IDENTITY NOTHING       -> replica_identity_nothing
    DEFAULT without usable key     -> identity_key_missing
    oversized flattened FULL ident -> identity_incomplete
    missing identity metadata      -> identity_metadata_missing
    unknown relfilenode            -> unknown_dictionary       (global fatal)
    unsafe mixed xid               -> xid family suppressed; no partial apply-safe output

`unknown_dictionary` (and single-INSERT, two-phase, aborted-DDL, buffer
overflow, OOM) are global fatals that stop the scan. The UPDATE/DELETE
identity-unsafe reasons above are per-xid poison (see §7), not global fatals.

`identity_external_toast` exists as a defensive guard but is not naturally
reachable: PostgreSQL flattens external-TOAST identity columns inline before WAL
logging, so an on-disk TOAST pointer never appears in the old identity. The
practically reachable boundary is `identity_incomplete`, when a flattened FULL
old tuple exceeds the bounded reconstruction buffer.

## 6. Sidecar / live-prime rules

The relation/identity dictionary can be primed from the live catalog
(`prime = true`) or from an exported WX_SIDECAR blob (the `sidecar` argument).

    WX_SIDECAR 2 contains R / A / I records.
    I records carry per-relation identity metadata (replica identity + identity columns).

    live-prime and sidecar-v2 must agree for DML identity (parity).
    v1 sidecar (no I records) stays valid for INSERT/COPY, but not for UPDATE/DELETE:
        v1 sidecar DML            -> fails closed identity_metadata_missing
    v2 sidecar with a missing I record for a relation:
        that relation's DML       -> fails closed identity_metadata_missing
    unprimed unknown relfilenode  -> unknown_dictionary (global fatal)

## 7. Xid-family poison

    Any unsupported / unsafe / incomplete / identity-unsafe user operation
    poisons its xid family.

    - The first precise operation-level reason wins; later unsafe ops in the
      same xid do not overwrite it.
    - Pending INSERT/DML batches for the family are suppressed, and anything
      buffered after the poison is suppressed at COMMIT.
    - COMMIT never flushes a poisoned family as clean apply-safe output.
    - ABORT discards the family's pending batches and its poison state (no leak).
    - A clean later xid still emits normally; one poisoned xid does not poison
      another.
    - Subtransactions: an unsafe committed/released subxact poisons the whole
      family; a clean subxact emits; an unsafe rolled-back subxact does not
      poison the surviving committed family.

The central rule:

    A poisoned xid family is never emitted as a clean apply-safe transaction,
    even when other records in the same xid are individually supported.

## 8. Apply-stream completeness

This is the most important consumer-facing rule.

    Detail SRFs provide content.
    Detail SRFs can omit poisoned xid families.
    Therefore detail SRFs alone are not sufficient proof of a complete clean
    apply stream.

    Strict stats SRFs are the completeness gate.
    A range is a complete clean apply stream only if the corresponding strict
    stats SRF succeeds.

    If poisoned xid families were omitted, the strict stats SRF fails closed with
    the precise reason and reports the number of omitted families in errdetail:

        ERROR:  walextract <mode> mode stopped: <reason>
        DETAIL: N poisoned xid families omitted from this range

Recommended consumer flow:

    1. call the detail SRF to read machine rows for the range;
    2. call the matching strict stats SRF for the same range;
    3. apply only if the strict stats SRF succeeds;
    4. if the strict stats SRF fails, treat the range as incomplete / unsafe for
       apply (read the reason and the omitted-family count from errdetail).

## 9. Frozen non-goals

The following are explicitly not implemented by this gate:

    TOAST reassembly / fetch
    page-state reconstruction
    logical decoding clone
    rm_desc decoder
    SQL UPDATE/DELETE renderer
    ChangeEvent fallback
    ProGate protocol
    Arrow layout
    XLogReader work
    major performance rewrite
    forged-WAL unit coverage for identity_external_toast
