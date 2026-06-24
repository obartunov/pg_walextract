\echo Use "CREATE EXTENSION walextract" to load this file. \quit

CREATE FUNCTION walextract_wal2sql(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false)
RETURNS TABLE(record_lsn pg_lsn, xid xid, db_oid oid, rel_oid oid,
              relfilenode oid, op text, relation text, complete boolean,
              reasons text[], op_text text, commit_lsn pg_lsn)
AS 'MODULE_PATHNAME', 'walextract_wal2sql'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- Reconstructs user table data from WAL: not for PUBLIC.
REVOKE EXECUTE ON FUNCTION walextract_wal2sql(pg_lsn, pg_lsn, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2sql(pg_lsn, pg_lsn, boolean) TO pg_read_server_files;

-- machine-path (ChangeBatch) coverage: per-batch detail
CREATE FUNCTION walextract_wal2batch(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false)
RETURNS TABLE(record_lsn pg_lsn, commit_lsn pg_lsn, xid xid,
              relfilenode oid, rel_oid oid, nrows int, ncols int,
              schema_missing boolean, has_external_toast boolean,
              payload_bytes bigint, null_cells bigint)
AS 'MODULE_PATHNAME', 'walextract_wal2batch'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_wal2batch(pg_lsn, pg_lsn, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2batch(pg_lsn, pg_lsn, boolean) TO pg_read_server_files;

-- machine-path single-row summary (counts, payload, peaks)
CREATE FUNCTION walextract_batch_stats(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false)
RETURNS TABLE(n_batches bigint, total_rows bigint, total_payload_bytes bigint,
              total_null_cells bigint, any_external_toast boolean,
              event_buf_peak bigint, batch_buf_peak bigint)
AS 'MODULE_PATHNAME', 'walextract_batch_stats'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_batch_stats(pg_lsn, pg_lsn, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_batch_stats(pg_lsn, pg_lsn, boolean) TO pg_read_server_files;

-- single-active mode contract selftest (no WAL access; safe for PUBLIC)
CREATE FUNCTION walextract_mode_selftest()
RETURNS text
AS 'MODULE_PATHNAME', 'walextract_mode_selftest'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- EVENT+NULL baseline (event mode, counting sink, no rendering) for benchmarking
CREATE FUNCTION walextract_wal2event_count(start_lsn pg_lsn, end_lsn pg_lsn, prime boolean DEFAULT false)
RETURNS TABLE(n_events bigint, event_buf_peak bigint)
AS 'MODULE_PATHNAME', 'walextract_wal2event_count'
LANGUAGE C STRICT PARALLEL UNSAFE;

REVOKE EXECUTE ON FUNCTION walextract_wal2event_count(pg_lsn, pg_lsn, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2event_count(pg_lsn, pg_lsn, boolean) TO pg_read_server_files;
