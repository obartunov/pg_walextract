\echo Use "CREATE EXTENSION walextract" to load this file. \quit

CREATE FUNCTION walextract_wal2sql(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(record_lsn pg_lsn, xid xid, db_oid oid, rel_oid oid,
              relfilenode oid, op text, relation text, complete boolean,
              reasons text[], op_text text, commit_lsn pg_lsn)
AS 'MODULE_PATHNAME', 'walextract_wal2sql'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- Reconstructs user table data from WAL: not for PUBLIC.
REVOKE EXECUTE ON FUNCTION walextract_wal2sql(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2sql(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- machine-path (ChangeBatch) coverage: per-batch detail
CREATE FUNCTION walextract_wal2batch(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(record_lsn pg_lsn, commit_lsn pg_lsn, xid xid,
              relfilenode oid, rel_oid oid, nrows int, ncols int,
              schema_missing boolean, has_external_toast boolean,
              payload_bytes bigint, null_cells bigint)
AS 'MODULE_PATHNAME', 'walextract_wal2batch'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_wal2batch(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2batch(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- machine-path single-row summary (counts, payload, peaks)
CREATE FUNCTION walextract_batch_stats(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(n_batches bigint, total_rows bigint, total_payload_bytes bigint,
              total_null_cells bigint, any_external_toast boolean,
              event_buf_peak bigint, batch_buf_peak bigint)
AS 'MODULE_PATHNAME', 'walextract_batch_stats'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_batch_stats(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_batch_stats(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- 2c machine UPDATE/DELETE path: typed-shape rows per ChangeDmlBatch (no values)
CREATE FUNCTION walextract_wal2dmlbatch(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(record_lsn pg_lsn, commit_lsn pg_lsn, xid xid,
              relfilenode oid, rel_oid oid, op text, identity_source text,
              nident int, has_new_row boolean, nnew int,
              toast_external boolean, incomplete boolean)
AS 'MODULE_PATHNAME', 'walextract_wal2dmlbatch'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_wal2dmlbatch(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2dmlbatch(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- 2c machine UPDATE/DELETE single-row summary (counts + flags; fails closed)
CREATE FUNCTION walextract_dmlbatch_stats(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(n_batches bigint, n_update bigint, n_delete bigint,
              n_with_new_row bigint, any_toast_external boolean,
              any_incomplete boolean)
AS 'MODULE_PATHNAME', 'walextract_dmlbatch_stats'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_dmlbatch_stats(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_dmlbatch_stats(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- unified machine apply stream: INSERT/COPY ChangeBatch + UPDATE/DELETE
-- ChangeDmlBatch from one scan under one xid-family decision.  Detail rows are
-- tagged by kind (INSERT_BATCH / DML_BATCH); per-kind columns are NULL where not
-- applicable.
CREATE FUNCTION walextract_wal2machinebatch(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(kind text, record_lsn pg_lsn, commit_lsn pg_lsn, xid xid,
              relfilenode oid, rel_oid oid, op text, identity_source text,
              nrows int, nident int, has_new_row boolean,
              toast_external boolean, incomplete boolean)
AS 'MODULE_PATHNAME', 'walextract_wal2machinebatch'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_wal2machinebatch(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2machinebatch(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- unified machine apply stream summary (completeness gate; fails closed on a
-- committed poisoned xid family, errdetail reports the omitted-family count)
CREATE FUNCTION walextract_machinebatch_stats(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(insert_batches bigint, insert_rows bigint, dml_batches bigint,
              n_update bigint, n_delete bigint, any_toast_external boolean,
              any_incomplete boolean)
AS 'MODULE_PATHNAME', 'walextract_machinebatch_stats'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_machinebatch_stats(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_machinebatch_stats(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- single-active mode contract selftest (no WAL access; safe for PUBLIC)
CREATE FUNCTION walextract_mode_selftest()
RETURNS text
AS 'MODULE_PATHNAME', 'walextract_mode_selftest'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- EVENT+NULL baseline (event mode, counting sink, no rendering) for benchmarking
CREATE FUNCTION walextract_wal2event_count(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false, sidecar text DEFAULT '')
RETURNS TABLE(n_events bigint, event_buf_peak bigint)
AS 'MODULE_PATHNAME', 'walextract_wal2event_count'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_wal2event_count(pg_lsn, pg_lsn, boolean, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2event_count(pg_lsn, pg_lsn, boolean, text) TO pg_read_server_files;

-- Sidecar producer: capture the live catalog as a WX_SIDECAR v0 text blob that
-- can later prime a decode of a WAL range starting at/after the snapshot LSN.
CREATE FUNCTION walextract_export_dictionary()
RETURNS text
AS 'MODULE_PATHNAME', 'walextract_export_dictionary'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_export_dictionary() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_export_dictionary() TO pg_read_server_files;
