-- walextract transaction-assembler contract.
-- Physical ChangeEvents are buffered by xid and delivered only on COMMIT, in
-- WAL order; ABORT discards them; transactions still open at the end of the
-- scanned range are never delivered.  Non-deterministic values (lsn/xid) appear
-- only inside boolean checks; data relations are created inside the mined range
-- so their descriptors are present (no dictionary_missing noise).
CREATE EXTENSION walextract;

-- case 1: a committed INSERT carries a commit_lsn that is after its own data
-- record and within the scanned range.
SELECT pg_current_wal_lsn() AS s1 \gset
CREATE TABLE wxx_c (id int, v text);
BEGIN;
INSERT INTO wxx_c VALUES (1, 'a');
COMMIT;
SELECT pg_current_wal_lsn() AS e1 \gset
SELECT op, relation, complete,
       commit_lsn IS NOT NULL          AS has_commit_lsn,
       commit_lsn > record_lsn         AS commit_after_data,
       commit_lsn <= :'e1'             AS commit_in_range
  FROM walextract_wal2sql(:'s1', :'e1')
 WHERE op = 'INSERT' AND relation = 'wxx_c';

-- case 2: an INSERT followed by ROLLBACK is never delivered.  A committed
-- statement after the rollback forces the aborted INSERT + ABORT records to be
-- flushed and thus actually scanned (so this exercises discard, not mere
-- invisibility).
SELECT pg_current_wal_lsn() AS s2 \gset
CREATE TABLE wxx_r (id int);
BEGIN;
INSERT INTO wxx_r VALUES (99);
ROLLBACK;
CREATE TABLE wxx_r_flush (x int);   -- committed: flushes WAL incl. the ABORT
SELECT pg_current_wal_lsn() AS e2 \gset
SELECT count(*) AS rolled_back_rows
  FROM walextract_wal2sql(:'s2', :'e2')
 WHERE op = 'INSERT' AND relation = 'wxx_r';

-- case 3: two INSERTs in one transaction are delivered in original WAL order
-- after COMMIT, sharing a single commit_lsn.  No ORDER BY: row order is the
-- assembler's delivery order.
SELECT pg_current_wal_lsn() AS s3 \gset
CREATE TABLE wxx_o (id int);
BEGIN;
INSERT INTO wxx_o VALUES (1);
INSERT INTO wxx_o VALUES (2);
COMMIT;
SELECT pg_current_wal_lsn() AS e3 \gset
SELECT op_text
  FROM walextract_wal2sql(:'s3', :'e3')
 WHERE op = 'INSERT' AND relation = 'wxx_o';
SELECT bool_and(commit_lsn IS NOT NULL)  AS all_have_commit_lsn,
       count(DISTINCT commit_lsn)        AS distinct_commit_lsns
  FROM walextract_wal2sql(:'s3', :'e3')
 WHERE op = 'INSERT' AND relation = 'wxx_o';

-- case 4: a transaction still open at the end of the scanned range is not
-- delivered.  e4 is captured inside the transaction (after INSERT, before
-- COMMIT); the COMMIT afterwards flushes the INSERT data so it IS scanned, but
-- the commit record is past e4, so the buffered event must not be delivered.
SELECT pg_current_wal_lsn() AS s4 \gset
CREATE TABLE wxx_open (id int);
BEGIN;
INSERT INTO wxx_open VALUES (7);
SELECT pg_current_wal_lsn() AS e4 \gset
COMMIT;
SELECT count(*) AS open_rows
  FROM walextract_wal2sql(:'s4', :'e4')
 WHERE op = 'INSERT' AND relation = 'wxx_open';

-- case 5: an aborted transaction that already mutated the (non-transactional)
-- mined dictionary -- i.e. did DDL inside the rolled-back transaction -- fails
-- closed, rather than risk a later relfilenode-reuse mis-decode against the
-- aborted schema.  A committed statement after the rollback flushes the aborted
-- catalog records + ABORT so they are actually scanned.
SELECT pg_current_wal_lsn() AS s5 \gset
BEGIN;
CREATE TABLE wxx_abort_ddl (id int);
ROLLBACK;
CREATE TABLE wxx_ddl_flush (x int);   -- committed: flushes the aborted DDL + ABORT
SELECT pg_current_wal_lsn() AS e5 \gset
SELECT count(*) FROM walextract_wal2sql(:'s5', :'e5');   -- must fail closed (ERROR)

DROP TABLE wxx_c, wxx_r, wxx_r_flush, wxx_o, wxx_open, wxx_ddl_flush;
DROP EXTENSION walextract;
