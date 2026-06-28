-- 2e xid-family poison.  A user operation that is unsupported/unsafe poisons its
-- xid: the WHOLE family is suppressed (never a partial apply-safe stream), while
-- CLEAN families in the same scan still emit -- the behavior the old global stop
-- could not provide.  These run at the suite's wal_level (replica), where a user
-- UPDATE/DELETE is unsafe (no_explicit_old_identity) and a user INSERT is
-- supported in batch mode, so a mixed xid exercises poison directly.  Output is
-- deterministic: batch COUNTS only (no LSN/OID/xid), and a wrapper maps the
-- strict stats fail-closed reason to a stable token.
CREATE EXTENSION walextract;

-- clean-family batch counts from the DETAIL SRF (poisoned families omitted)
CREATE FUNCTION batch_shape(s pg_lsn, e pg_lsn)
RETURNS TABLE(batches bigint, rows bigint) AS $$
    SELECT count(*)::bigint, COALESCE(sum(nrows),0)::bigint
    FROM walextract_wal2batch(s, e, true, '')
$$ LANGUAGE sql;

-- strict stats fail-closed reason (poisoned family -> precise op reason)
CREATE FUNCTION batch_reason(s pg_lsn, e pg_lsn) RETURNS text AS $$
BEGIN
    PERFORM n_batches FROM walextract_batch_stats(s, e, true, '');
    RETURN 'clean (no fail-closed)';
EXCEPTION WHEN OTHERS THEN
    RETURN regexp_replace(SQLERRM, '^walextract batch mode stopped: ', '');
END$$ LANGUAGE plpgsql;

-- XP1: INSERT + unsafe UPDATE in ONE xid -> family poisoned -> the supported
-- INSERT is NOT emitted (0 clean batches); strict reason is the UPDATE's.
CREATE TABLE xp1(id int primary key, v int);
INSERT INTO xp1 VALUES (1,1);
SELECT pg_current_wal_lsn() AS s \gset
BEGIN;
INSERT INTO xp1 VALUES (2,2);
UPDATE xp1 SET v = v + 1 WHERE id = 1;
COMMIT;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'XP1 insert+update one xid' AS case, * FROM batch_shape(:'s', :'e');
SELECT 'XP1 reason' AS case, batch_reason(:'s', :'e') AS reason;

-- XP2: INSERT + unsafe DELETE in ONE xid -> no INSERT batch emitted.
CREATE TABLE xp2(id int primary key, v int);
INSERT INTO xp2 VALUES (1,1);
SELECT pg_current_wal_lsn() AS s \gset
BEGIN;
INSERT INTO xp2 VALUES (2,2);
DELETE FROM xp2 WHERE id = 1;
COMMIT;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'XP2 insert+delete one xid' AS case, * FROM batch_shape(:'s', :'e');

-- XP3: two xids -- first poisoned (insert+update), then a CLEAN insert-only xid.
-- The clean xid still emits: exactly 1 batch with its row, despite the earlier
-- poisoned family in the same scan.
CREATE TABLE xp3(id int primary key, v int);
INSERT INTO xp3 VALUES (1,1);
SELECT pg_current_wal_lsn() AS s \gset
BEGIN;
INSERT INTO xp3 VALUES (2,2);
UPDATE xp3 SET v = v + 1 WHERE id = 1;
COMMIT;
INSERT INTO xp3 VALUES (3,3);
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'XP3 poisoned then clean xid' AS case, * FROM batch_shape(:'s', :'e');

-- XP4: supported INSERT-only xid -> still emits normally (1 batch, 2 rows).
CREATE TABLE xp4(id int primary key, v int);
SELECT pg_current_wal_lsn() AS s \gset
INSERT INTO xp4 VALUES (1,1),(2,2);
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'XP4 supported insert-only' AS case, * FROM batch_shape(:'s', :'e');
SELECT 'XP4 reason' AS case, batch_reason(:'s', :'e') AS reason;

-- XP5: ABORTed mixed xid leaves no poison and emits nothing; a later CLEAN xid
-- still emits (1 batch).  Proves ABORT discards poison state without leak.
CREATE TABLE xp5(id int primary key, v int);
INSERT INTO xp5 VALUES (1,1);
SELECT pg_current_wal_lsn() AS s \gset
BEGIN;
INSERT INTO xp5 VALUES (2,2);
UPDATE xp5 SET v = v + 1 WHERE id = 1;
ROLLBACK;
INSERT INTO xp5 VALUES (3,3);
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'XP5 aborted mixed then clean' AS case, * FROM batch_shape(:'s', :'e');
SELECT 'XP5 reason' AS case, batch_reason(:'s', :'e') AS reason;

-- XP6: different-relation mixed xid (INSERT rel A + UPDATE rel B in one xid) ->
-- the whole family is poisoned, so rel A's INSERT is not emitted either.
CREATE TABLE xp6a(id int primary key, v int);
CREATE TABLE xp6b(id int primary key, v int);
INSERT INTO xp6b VALUES (1,1);
SELECT pg_current_wal_lsn() AS s \gset
BEGIN;
INSERT INTO xp6a VALUES (9,9);
UPDATE xp6b SET v = v + 1 WHERE id = 1;
COMMIT;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'XP6 different-relation mixed' AS case, * FROM batch_shape(:'s', :'e');

DROP FUNCTION batch_shape(pg_lsn, pg_lsn);
DROP FUNCTION batch_reason(pg_lsn, pg_lsn);
DROP EXTENSION walextract;
