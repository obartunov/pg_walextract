\echo Use "CREATE EXTENSION walextract" to load this file. \quit

CREATE FUNCTION walextract_wal2sql(start_lsn pg_lsn, end_lsn pg_lsn)
RETURNS TABLE(record_lsn pg_lsn, xid xid, db_oid oid, rel_oid oid,
              relfilenode oid, op text, relation text, complete boolean,
              reasons text[], op_text text, commit_lsn pg_lsn)
AS 'MODULE_PATHNAME', 'walextract_wal2sql'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- Reconstructs user table data from WAL: not for PUBLIC.
REVOKE EXECUTE ON FUNCTION walextract_wal2sql(pg_lsn, pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION walextract_wal2sql(pg_lsn, pg_lsn) TO pg_read_server_files;
