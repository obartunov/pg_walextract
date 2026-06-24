-- pg_waldump parity + throughput: workload generator.
-- Each workload produces a self-contained, flushed WAL range and records
-- (workload, s0, s1, kind, note) into wx_ranges.  pg_switch_wal() forces flush
-- so aborted/open-transaction WAL is readable by both pg_waldump and walextract.
-- Run once against a fresh database before the parity/throughput drivers.
\set ON_ERROR_STOP on
DROP TABLE IF EXISTS wx_ranges;
CREATE TABLE wx_ranges(seq serial, workload text, s0 pg_lsn, s1 pg_lsn, kind text, note text);
CREATE EXTENSION IF NOT EXISTS walextract;

-- helper to append a range row
-- (inlined per workload; pg_current_wal_lsn captured via \gset)

-- pre-range data files (single-insert builders live OUTSIDE every measured range)
CREATE TABLE src30(a int, b int, c int);
INSERT INTO src30 SELECT g, g*2, g*3 FROM generate_series(1,30000) g;
COPY src30 TO '/tmp/wx_src30.dat';
CREATE TABLE src200(a int, b int, c int);
INSERT INTO src200 SELECT g, g*2, g*3 FROM generate_series(1,200000) g;
COPY src200 TO '/tmp/wx_src200.dat';
CREATE TABLE srctoast(id int, big text);
ALTER TABLE srctoast ALTER COLUMN big SET STORAGE EXTERNAL;
INSERT INTO srctoast SELECT g, repeat(md5(g::text), 120) FROM generate_series(1,200) g; -- ~3.8KB each -> external
COPY srctoast TO '/tmp/wx_srctoast.dat';

-- A: COPY bulk (MULTI_INSERT-heavy), 30k rows, CREATE in range (typed batch)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wa(a int, b int, c int);
COPY wa FROM '/tmp/wx_src30.dat';
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'A', :'s', pg_current_wal_lsn(), 'copy_bulk', 'COPY 30k MULTI_INSERT';

-- B: single INSERT stream (8 single inserts)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wb(a int);
INSERT INTO wb VALUES (1);
INSERT INTO wb VALUES (2);
INSERT INTO wb VALUES (3);
INSERT INTO wb VALUES (4);
INSERT INTO wb VALUES (5);
INSERT INTO wb VALUES (6);
INSERT INTO wb VALUES (7);
INSERT INTO wb VALUES (8);
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'B', :'s', pg_current_wal_lsn(), 'single_insert', '8 single HEAP INSERT';

-- C: INSERT...SELECT single-insert-heavy (500 single inserts)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wc(a int);
INSERT INTO wc SELECT g FROM generate_series(1,500) g;
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'C', :'s', pg_current_wal_lsn(), 'insert_select', 'INSERT..SELECT 500 single';

-- D: INSERT + ABORT (pre-existing table; pure data abort)
CREATE TABLE wd(a int);
SELECT pg_current_wal_lsn() AS s \gset
BEGIN;
INSERT INTO wd VALUES (1);
INSERT INTO wd VALUES (2);
INSERT INTO wd VALUES (3);
ROLLBACK;
SELECT pg_switch_wal() AS sw \gset
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'D', :'s', pg_current_wal_lsn(), 'insert_abort', 'single INSERT x3 + ROLLBACK';

-- E: COPY + ABORT (CREATE in range -> typed batch buffered, then discarded)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE we(a int, b int, c int);
BEGIN;
COPY we FROM '/tmp/wx_src30.dat';
ROLLBACK;
SELECT pg_switch_wal() AS sw \gset
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'E', :'s', pg_current_wal_lsn(), 'copy_abort', 'COPY 30k + ROLLBACK';

-- F: COPY + TRUNCATE same transaction (prune boundary)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wf(a int, b int, c int);
BEGIN;
COPY wf FROM '/tmp/wx_src30.dat';
TRUNCATE wf;
COMMIT;
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'F', :'s', pg_current_wal_lsn(), 'copy_truncate', 'COPY + TRUNCATE same txn';

-- G: DROP / rewrite boundary (COPY committed+delivered, then DROP)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wg(a int, b int, c int);
COPY wg FROM '/tmp/wx_src30.dat';
DROP TABLE wg;
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'G', :'s', pg_current_wal_lsn(), 'drop_boundary', 'COPY then DROP TABLE';

-- H: UPDATE / HOT_UPDATE (setup INSERT committed BEFORE range)
CREATE TABLE wh(a int, b int);
INSERT INTO wh VALUES (1, 1), (2, 2), (3, 3);
SELECT pg_current_wal_lsn() AS s \gset
UPDATE wh SET b = b + 100 WHERE a = 1;
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'H', :'s', pg_current_wal_lsn(), 'update', 'user UPDATE';

-- I: DELETE (setup INSERT committed BEFORE range)
CREATE TABLE wi(a int, b int);
INSERT INTO wi VALUES (1, 1), (2, 2), (3, 3);
SELECT pg_current_wal_lsn() AS s \gset
DELETE FROM wi WHERE a = 2;
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'I', :'s', pg_current_wal_lsn(), 'delete', 'user DELETE';

-- J: external TOAST / large values (COPY of external-stored text)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wj(id int, big text);
ALTER TABLE wj ALTER COLUMN big SET STORAGE EXTERNAL;
COPY wj FROM '/tmp/wx_srctoast.dat';
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'J', :'s', pg_current_wal_lsn(), 'external_toast', 'COPY 200 rows ~3.8KB external';

-- K: open transaction at range end (flush while open; range ends before COMMIT)
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wk(a int, b int, c int);
BEGIN;
COPY wk FROM '/tmp/wx_src30.dat';
SELECT pg_switch_wal() AS sw \gset
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'K', :'s', pg_current_wal_lsn(), 'open_tx', 'COPY in open txn at range end';
COMMIT;

-- L: DDL + INSERT same transaction (descriptor learned in-txn -> typed batch)
SELECT pg_current_wal_lsn() AS s \gset
BEGIN;
CREATE TABLE wl(a int, b int, c int);
COPY wl FROM '/tmp/wx_src30.dat';
COMMIT;
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'L', :'s', pg_current_wal_lsn(), 'ddl_insert', 'CREATE + COPY same txn';

-- A-bulk: COPY capacity probe (200k) -- event path expected to fail-closed on cap
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wabulk(a int, b int, c int);
COPY wabulk FROM '/tmp/wx_src200.dat';
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'A-bulk', :'s', pg_current_wal_lsn(), 'copy_capacity', 'COPY 200k MULTI_INSERT';

-- M: pre-existing NORMAL table; CREATE before range, COPY inside range.
-- Unknown to the dictionary unless primed from live catalogs.
CREATE TABLE wm(a int, b int, c int);
INSERT INTO wm SELECT g, g*2, g*3 FROM generate_series(1,30000) g;
COPY wm TO '/tmp/wx_wm.dat';
TRUNCATE wm;
SELECT pg_switch_wal() AS sw \gset
SELECT pg_current_wal_lsn() AS s \gset
COPY wm FROM '/tmp/wx_wm.dat';
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'M', :'s', pg_current_wal_lsn(), 'prerange_copy', 'pre-existing table COPY 30k (CREATE before range)';

-- N: pre-existing EXTERNAL-TOAST table; CREATE before range, COPY inside range.
CREATE TABLE wn(id int, big text);
ALTER TABLE wn ALTER COLUMN big SET STORAGE EXTERNAL;
INSERT INTO wn SELECT g, repeat(md5(g::text), 120) FROM generate_series(1,200) g;
COPY wn TO '/tmp/wx_wn.dat';
TRUNCATE wn;
SELECT pg_switch_wal() AS sw \gset
SELECT pg_current_wal_lsn() AS s \gset
COPY wn FROM '/tmp/wx_wn.dat';
INSERT INTO wx_ranges(workload,s0,s1,kind,note)
  SELECT 'N', :'s', pg_current_wal_lsn(), 'prerange_toast', 'pre-existing external-TOAST COPY 200 (CREATE before range)';

SELECT workload, kind, s0, s1, pg_wal_lsn_diff(s1,s0) AS wal_bytes FROM wx_ranges ORDER BY seq;
