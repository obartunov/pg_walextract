-- ChangeBatch Insert Coverage v0: single HEAP_INSERT accumulated into one
-- ChangeBatch per (xid, relfilenode), materialized at COMMIT through the same
-- we_batch_begin + we_batch_row path as MULTI_INSERT.  Output is deterministic:
-- a classifier turns batch outcomes into stable tokens (no LSN/OID/timing).
CREATE EXTENSION walextract;

CREATE FUNCTION b(s pg_lsn, e pg_lsn, sc text DEFAULT '') RETURNS text AS $$
DECLARE r record;
BEGIN
    SELECT n_batches, total_rows, any_external_toast INTO r
      FROM walextract_batch_stats(s, e, false, sc);
    RETURN format('batches=%s rows=%s ext=%s', r.n_batches, r.total_rows, r.any_external_toast);
EXCEPTION WHEN OTHERS THEN
    IF    SQLERRM LIKE '%unknown_dictionary%'         THEN RETURN 'fail-closed unknown_dictionary';
    ELSIF SQLERRM LIKE '%update_unsupported%'         THEN RETURN 'fail-closed update_unsupported';
    ELSIF SQLERRM LIKE '%delete_unsupported%'         THEN RETURN 'fail-closed delete_unsupported';
    ELSIF SQLERRM LIKE '%buffer_overflow%'            THEN RETURN 'fail-closed buffer_overflow';
    ELSIF SQLERRM LIKE '%single_insert_unsupported%'  THEN RETURN 'fail-closed single_insert_unsupported';
    ELSE  RETURN 'fail-closed other';
    END IF;
END$$ LANGUAGE plpgsql;

-- ===== B1: pre-range table + sidecar + 8 single INSERTs (multi-transaction) =====
CREATE TABLE b1(a int, b int);
CREATE TABLE sc1(s text);
INSERT INTO sc1 SELECT walextract_export_dictionary();
SELECT pg_switch_wal() \gset
SELECT pg_current_wal_lsn() AS s \gset
INSERT INTO b1 VALUES (1,1);
INSERT INTO b1 VALUES (2,2);
INSERT INTO b1 VALUES (3,3);
INSERT INTO b1 VALUES (4,4);
INSERT INTO b1 VALUES (5,5);
INSERT INTO b1 VALUES (6,6);
INSERT INTO b1 VALUES (7,7);
INSERT INTO b1 VALUES (8,8);
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B1' AS case, b(:'s', :'e', (SELECT s FROM sc1)) AS result;

-- ===== B2: in-range CREATE TABLE + 8 single INSERTs (learned from WAL) =====
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE b2(a int, b int);
INSERT INTO b2 VALUES (1,1);
INSERT INTO b2 VALUES (2,2);
INSERT INTO b2 VALUES (3,3);
INSERT INTO b2 VALUES (4,4);
INSERT INTO b2 VALUES (5,5);
INSERT INTO b2 VALUES (6,6);
INSERT INTO b2 VALUES (7,7);
INSERT INTO b2 VALUES (8,8);
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B2' AS case, b(:'s', :'e') AS result;

-- ===== B3: INSERT ... SELECT 500 (one transaction) =====
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE b3(a int, b int);
INSERT INTO b3 SELECT g, g FROM generate_series(1,500) g;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B3' AS case, b(:'s', :'e') AS result;

-- ===== B4: 100k single INSERTs in ONE transaction (accumulator stress) =====
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE b4(a int, b int);
DO $$ BEGIN FOR i IN 1..100000 LOOP INSERT INTO b4 VALUES (i, i); END LOOP; END $$;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B4' AS case, b(:'s', :'e') AS result;

-- ===== B5: mixed COPY + single INSERT in the SAME transaction =====
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE b5(a int, b int);
BEGIN;
COPY b5 FROM PROGRAM 'seq 1 100 | awk ''{print $1"\t"$1}''';
INSERT INTO b5 VALUES (101,101);
INSERT INTO b5 VALUES (102,102);
INSERT INTO b5 VALUES (103,103);
INSERT INTO b5 VALUES (104,104);
INSERT INTO b5 VALUES (105,105);
COMMIT;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B5' AS case, b(:'s', :'e') AS result;

-- ===== B6: external-TOAST single INSERTs (one tx) =====
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE b6(id int, big text);
ALTER TABLE b6 ALTER COLUMN big SET STORAGE EXTERNAL;
BEGIN;
INSERT INTO b6 VALUES (1, repeat('A',4000));
INSERT INTO b6 VALUES (2, repeat('B',4000));
INSERT INTO b6 VALUES (3, repeat('C',4000));
COMMIT;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B6' AS case, b(:'s', :'e') AS result;

-- ===== B7: pre-range table WITHOUT sidecar/prime -> fail-closed =====
CREATE TABLE b7(a int, b int);
SELECT pg_switch_wal() \gset
SELECT pg_current_wal_lsn() AS s \gset
INSERT INTO b7 VALUES (1,1);
INSERT INTO b7 VALUES (2,2);
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B7' AS case, b(:'s', :'e') AS result;

-- ===== B8: sidecar-primed + in-range DROP COLUMN + later single INSERT =====
CREATE TABLE b8(a int, b int, c int);
CREATE TABLE sc8(s text);
INSERT INTO sc8 SELECT walextract_export_dictionary();
SELECT pg_switch_wal() \gset
SELECT pg_current_wal_lsn() AS s \gset
INSERT INTO b8 VALUES (1,1,1);
ALTER TABLE b8 DROP COLUMN c;
INSERT INTO b8(a,b) VALUES (2,2);
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B8' AS case, b(:'s', :'e', (SELECT s FROM sc8)) AS result;

-- ===== B9: UPDATE / DELETE still explicitly fail-closed (op-specific reason) =====
-- Table is learned in-range so trust == USER and the failure is the op-specific
-- unsupported reason (not unknown_dictionary): proves UPDATE/DELETE are not
-- silently skipped nor partially decoded as INSERT.
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE b9u(a int, b int);
INSERT INTO b9u VALUES (1,1);
UPDATE b9u SET b = b + 1 WHERE a = 1;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B9-update' AS case, b(:'s', :'e') AS result;

SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE b9d(a int, b int);
INSERT INTO b9d VALUES (1,1);
DELETE FROM b9d WHERE a = 1;
SELECT pg_current_wal_lsn() AS e \gset
SELECT 'B9-delete' AS case, b(:'s', :'e') AS result;

DROP FUNCTION b(pg_lsn, pg_lsn, text);
DROP EXTENSION walextract;
