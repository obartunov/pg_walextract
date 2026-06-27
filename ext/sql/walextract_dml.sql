-- UPDATE/DELETE machine identity classifier v0 (explicit-evidence policy).
-- These run at the suite's wal_level (replica), where PostgreSQL logs NO old
-- identity for UPDATE/DELETE; the machine path must therefore fail closed with a
-- precise reason and must NEVER infer identity from the new tuple. Deterministic
-- output: a classifier turns the fail-closed reason into a stable token; no
-- LSN/OID/xid reaches a result column. (The explicit-identity path -- old key /
-- old tuple present under wal_level=logical, reason dml_emit_pending -- needs a
-- logical cluster and is exercised in the runtime probe, not here.)
CREATE EXTENSION walextract;

CREATE FUNCTION dml_reason(s pg_lsn, e pg_lsn) RETURNS text AS $$
BEGIN
    PERFORM total_rows FROM walextract_batch_stats(s, e);
    RETURN 'decoded (no fail-closed)';
EXCEPTION WHEN OTHERS THEN
    RETURN regexp_replace(SQLERRM, '^walextract batch mode stopped: ', '');
END$$ LANGUAGE plpgsql;

-- DML1: non-HOT, key-UNCHANGED UPDATE. The PK is visible in the new tuple, but
-- no old identity is logged -> must fail closed no_explicit_old_identity.
-- This is the corruption-prevention case (do not trust the new-tuple key).
-- An index on the updated column forces a non-HOT update deterministically.
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE t1(id int primary key, v int);
CREATE INDEX t1_v ON t1(v);
INSERT INTO t1 VALUES (1,1),(2,2),(3,3);
UPDATE t1 SET v = v + 100 WHERE id = 1;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'DML1 update-keyunchanged' AS case, dml_reason(:'s', :'e') AS reason;

-- DML2: DELETE on a primary-key table -> no old key logged at replica level ->
-- no_explicit_old_identity.
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE t2(id int primary key, v int);
INSERT INTO t2 VALUES (1,1),(2,2);
DELETE FROM t2 WHERE id = 1;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'DML2 delete-pk' AS case, dml_reason(:'s', :'e') AS reason;

-- DML3: HOT update (unindexed column, page room) -> hot_update_unsupported.
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE t3(id int primary key, v int);
INSERT INTO t3 VALUES (1,1);
UPDATE t3 SET v = v + 1 WHERE id = 1;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'DML3 hot-update' AS case, dml_reason(:'s', :'e') AS reason;

-- DML4: pre-range table, UPDATE without sidecar/prime -> dictionary fails first.
CREATE TABLE t4(id int primary key, v int);
CREATE INDEX t4_v ON t4(v);
INSERT INTO t4 VALUES (1,1);
SELECT pg_current_wal_lsn() AS s \gset
UPDATE t4 SET v = v + 100 WHERE id = 1;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'DML4 update-unprimed' AS case, dml_reason(:'s', :'e') AS reason;

-- DML5: REPLICA IDENTITY NOTHING, DELETE -> still no old identity at replica;
-- the explicit-evidence rule fails closed no_explicit_old_identity.
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE t5(id int primary key, v int);
ALTER TABLE t5 REPLICA IDENTITY NOTHING;
INSERT INTO t5 VALUES (1,1);
DELETE FROM t5 WHERE id = 1;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'DML5 delete-ri-nothing' AS case, dml_reason(:'s', :'e') AS reason;

DROP FUNCTION dml_reason(pg_lsn, pg_lsn);
DROP EXTENSION walextract;
