-- Rewrite/Truncate/Drop boundary detection from the COMMIT drop-list
-- (xl_xact_parsed_commit.xlocators).  A dropped relfilenode must invalidate the
-- mined relfilenode->relid/name mapping and surface an explicit forensic
-- boundary event; walextract must never decode later records through the stale
-- mapping.  Non-deterministic values (lsn/xid/relfilenode, toast names) appear
-- only inside counts/booleans.
CREATE EXTENSION walextract;

-- TRUNCATE: table created in-range so its descriptor is known; the TRUNCATE
-- COMMIT drop-list invalidates the old relfilenode.
SELECT pg_current_wal_lsn() AS s \gset
CREATE TABLE wx_tr(id int);
INSERT INTO wx_tr VALUES (1),(2);
TRUNCATE wx_tr;
INSERT INTO wx_tr VALUES (3);
SELECT pg_current_wal_lsn() AS e \gset

-- one explicit forensic boundary for wx_tr, with the old relation name captured
SELECT op, relation, complete, array_to_string(reasons,',') AS reasons
  FROM walextract_wal2sql(:'s',:'e')
 WHERE 'rewrite_or_drop_boundary' = ANY(reasons);

-- pre-TRUNCATE rows still decode complete; the post-TRUNCATE row is NOT decoded
-- as a complete wx_tr insert (old relfilenode invalidated, new one unmapped).
SELECT count(*) AS complete_wx_tr_inserts
  FROM walextract_wal2sql(:'s',:'e')
 WHERE op = 'INSERT' AND relation = 'wx_tr' AND complete;
SELECT count(*) >= 1 AS post_boundary_row_not_stale_decoded
  FROM walextract_wal2sql(:'s',:'e')
 WHERE op = 'INSERT' AND relation IS NULL AND NOT complete
   AND 'dictionary_missing' = ANY(reasons);

-- DROP: dropped relfilenode surfaces a boundary tagged with the old name.
SELECT pg_current_wal_lsn() AS s2 \gset
CREATE TABLE wx_drop(id int);
INSERT INTO wx_drop VALUES (1);
DROP TABLE wx_drop;
SELECT pg_current_wal_lsn() AS e2 \gset
SELECT op, relation, complete, array_to_string(reasons,',') AS reasons
  FROM walextract_wal2sql(:'s2',:'e2')
 WHERE 'rewrite_or_drop_boundary' = ANY(reasons);

-- VACUUM FULL: a heap rewrite drops heap + toast + toast index relfilenodes;
-- each tracked drop is a boundary.  wx_rw must be among them.
SELECT pg_current_wal_lsn() AS s3 \gset
CREATE TABLE wx_rw(id int, v text);
INSERT INTO wx_rw VALUES (1,'a');
VACUUM FULL wx_rw;
INSERT INTO wx_rw VALUES (2,'b');
SELECT pg_current_wal_lsn() AS e3 \gset
SELECT count(*) AS rewrite_boundaries,
       bool_or(relation = 'wx_rw') AS has_wx_rw
  FROM walextract_wal2sql(:'s3',:'e3')
 WHERE 'rewrite_or_drop_boundary' = ANY(reasons);

-- same-transaction INSERT then TRUNCATE: the row destroyed in the same xact
-- must NOT be delivered as an apply candidate (boundary decided before
-- delivery, not after).
SELECT pg_current_wal_lsn() AS s4 \gset
CREATE TABLE wx_same_tx_tr(id int);
BEGIN;
INSERT INTO wx_same_tx_tr VALUES (1);
TRUNCATE wx_same_tx_tr;
COMMIT;
SELECT pg_current_wal_lsn() AS e4 \gset
SELECT count(*) AS same_tx_tr_complete_inserts
  FROM walextract_wal2sql(:'s4',:'e4')
 WHERE op = 'INSERT' AND relation = 'wx_same_tx_tr' AND complete;
SELECT count(*) AS same_tx_tr_boundaries
  FROM walextract_wal2sql(:'s4',:'e4')
 WHERE 'rewrite_or_drop_boundary' = ANY(reasons) AND relation = 'wx_same_tx_tr';

-- same-transaction INSERT then DROP: same rule.
SELECT pg_current_wal_lsn() AS s5 \gset
CREATE TABLE wx_same_tx_drop(id int);
BEGIN;
INSERT INTO wx_same_tx_drop VALUES (1);
DROP TABLE wx_same_tx_drop;
COMMIT;
SELECT pg_current_wal_lsn() AS e5 \gset
SELECT count(*) AS same_tx_drop_complete_inserts
  FROM walextract_wal2sql(:'s5',:'e5')
 WHERE op = 'INSERT' AND relation = 'wx_same_tx_drop' AND complete;
SELECT count(*) AS same_tx_drop_boundaries
  FROM walextract_wal2sql(:'s5',:'e5')
 WHERE 'rewrite_or_drop_boundary' = ANY(reasons) AND relation = 'wx_same_tx_drop';

-- same-transaction INSERT, TRUNCATE, INSERT: row 1 (pre-TRUNCATE) must not be
-- an apply candidate; row 2 may degrade (new relfilenode unmapped).  Assert no
-- complete apply-candidate INSERT bound to the relation, and a boundary exists.
SELECT pg_current_wal_lsn() AS s6 \gset
CREATE TABLE wx_same_tx_after(id int);
BEGIN;
INSERT INTO wx_same_tx_after VALUES (1);
TRUNCATE wx_same_tx_after;
INSERT INTO wx_same_tx_after VALUES (2);
COMMIT;
SELECT pg_current_wal_lsn() AS e6 \gset
SELECT count(*) AS same_tx_after_complete_inserts
  FROM walextract_wal2sql(:'s6',:'e6')
 WHERE op = 'INSERT' AND relation = 'wx_same_tx_after' AND complete;
SELECT count(*) AS same_tx_after_boundaries
  FROM walextract_wal2sql(:'s6',:'e6')
 WHERE 'rewrite_or_drop_boundary' = ANY(reasons) AND relation = 'wx_same_tx_after';

-- Option A precision: a transaction may drop/rewrite X but still write to a
-- live table Y in the same transaction.  Only X's rows are suppressed; Y's
-- survive.  (Tables are created in-range so walextract tracks them.)
SELECT pg_current_wal_lsn() AS s7 \gset
CREATE TABLE wx_live_y(id int);
CREATE TABLE wx_dead_x(id int);
BEGIN;
INSERT INTO wx_live_y VALUES (1);
INSERT INTO wx_dead_x VALUES (1);
DROP TABLE wx_dead_x;
COMMIT;
SELECT pg_current_wal_lsn() AS e7 \gset
SELECT count(*) AS same_tx_live_y_inserts
  FROM walextract_wal2sql(:'s7',:'e7')
 WHERE op = 'INSERT' AND relation = 'wx_live_y' AND complete;
SELECT count(*) AS same_tx_dead_x_inserts
  FROM walextract_wal2sql(:'s7',:'e7')
 WHERE op = 'INSERT' AND relation = 'wx_dead_x' AND complete;
SELECT count(*) AS same_tx_dead_x_boundaries
  FROM walextract_wal2sql(:'s7',:'e7')
 WHERE 'rewrite_or_drop_boundary' = ANY(reasons) AND relation = 'wx_dead_x';

DROP TABLE wx_tr, wx_rw, wx_same_tx_tr, wx_same_tx_after, wx_live_y;
DROP EXTENSION walextract;
