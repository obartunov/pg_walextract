-- walextract ChangeBatch (machine path) contract.
-- Non-deterministic values (lsn/xid/oid) are never selected; assertions use
-- counts, byte totals, and explicit fail-closed errors only.  pg_switch_wal()
-- forces flush so aborted/open-transaction WAL is readable within a range.
CREATE EXTENSION walextract;

-- single-active mode contract: installing one sink clears the other.
SELECT walextract_mode_selftest();

-- schema_missing relation: created BEFORE any mined range, so its descriptor is
-- never learned inside the range -> batch has ncols=0 and fabricates nothing.
CREATE TABLE wxb_pre (a int, b int);

-- case A: COPY of an all-fixed (int4 x3) relation -> ONE ChangeBatch.
-- payload is exactly nrows*ncols*4 = 60 (== event-mode raw bytes); no nulls.
SELECT pg_current_wal_lsn() AS a0 \gset
CREATE TABLE wxb_fix (a int, b int, c int);
COPY wxb_fix (a, b, c) FROM stdin;
1	10	100
2	20	200
3	30	300
4	40	400
5	50	500
\.
SELECT pg_current_wal_lsn() AS a1 \gset
SELECT nrows, ncols, schema_missing, has_external_toast, payload_bytes, null_cells
  FROM walextract_wal2batch(:'a0', :'a1');
SELECT n_batches, total_rows, total_payload_bytes, total_null_cells, any_external_toast,
       batch_buf_peak > 0 AS batch_peak_positive
  FROM walextract_batch_stats(:'a0', :'a1');

-- case B: mixed fixed + varlena(text) + NULLs.  null_cells controlled exactly:
-- row2 t NULL, row3 n NULL => 2 null cells across 3 logical cols.
SELECT pg_current_wal_lsn() AS b0 \gset
CREATE TABLE wxb_mix (id int, t text, n int);
COPY wxb_mix (id, t, n) FROM stdin;
1	abc	11
2	\N	22
3	xyz	\N
\.
SELECT pg_current_wal_lsn() AS b1 \gset
SELECT nrows, ncols, schema_missing, has_external_toast, null_cells,
       payload_bytes > 0 AS payload_nonzero
  FROM walextract_wal2batch(:'b0', :'b1');

-- (dropped-column / descriptor-evolution coverage is deferred to P2A.1
--  descriptor-shape hardening: the current P2A path decodes live columns at
--  correct offsets (verified), so this is not a silent-loss/corruption case.)

-- case D: pre-range relation is UNKNOWN to the dictionary (unprimed) -> machine
-- mode fails closed with unknown_dictionary (never guesses unknown as user data).
SELECT pg_current_wal_lsn() AS d0 \gset
COPY wxb_pre (a, b) FROM stdin;
7	8
9	10
\.
SELECT pg_current_wal_lsn() AS d1 \gset
SELECT nrows, ncols, schema_missing, payload_bytes
  FROM walextract_wal2batch(:'d0', :'d1');

-- case E: single user INSERT -> batch mode now accumulates it into one
-- ChangeBatch (insert coverage v0); event mode is unchanged (decodes the same
-- INSERT).  The in-range CREATE makes wxb_ev a learned USER relation, so the
-- single INSERT is trusted and delivered as 1 batch at COMMIT.
SELECT pg_current_wal_lsn() AS e0 \gset
CREATE TABLE wxb_ev (a int, b int);
INSERT INTO wxb_ev VALUES (1, 2);
SELECT pg_current_wal_lsn() AS e1 \gset
SELECT op, relation, complete
  FROM walextract_wal2sql(:'e0', :'e1')
 WHERE op = 'INSERT' AND relation = 'wxb_ev';
SELECT n_batches FROM walextract_batch_stats(:'e0', :'e1');

-- case G: user UPDATE in batch mode -> FAIL CLOSED.
SELECT pg_current_wal_lsn() AS g0 \gset
UPDATE wxb_fix SET c = c + 1 WHERE a = 1;
SELECT pg_current_wal_lsn() AS g1 \gset
SELECT n_batches FROM walextract_batch_stats(:'g0', :'g1');

-- case H: user DELETE in batch mode -> FAIL CLOSED.
SELECT pg_current_wal_lsn() AS h0 \gset
DELETE FROM wxb_fix WHERE a = 2;
SELECT pg_current_wal_lsn() AS h1 \gset
SELECT n_batches FROM walextract_batch_stats(:'h0', :'h1');

-- case I: ABORT discards buffered batches.  Table is CREATEd+committed in range
-- (typed batch), the COPY runs in an aborted txn; pg_switch_wal flushes so the
-- COPY+ABORT are readable.  Aborted txn mutates no catalog -> discard, 0 batches.
SELECT pg_current_wal_lsn() AS i0 \gset
CREATE TABLE wxb_ab (a int, b int);
BEGIN;
COPY wxb_ab (a, b) FROM stdin;
1	2
3	4
\.
ROLLBACK;
SELECT pg_switch_wal() AS i_sw \gset
SELECT pg_current_wal_lsn() AS i1 \gset
SELECT n_batches FROM walextract_batch_stats(:'i0', :'i1');

-- case J: COPY + TRUNCATE + COMMIT -> batch pruned at the rewrite boundary.
SELECT pg_current_wal_lsn() AS j0 \gset
CREATE TABLE wxb_tr (a int, b int);
BEGIN;
COPY wxb_tr (a, b) FROM stdin;
1	2
3	4
\.
TRUNCATE wxb_tr;
COMMIT;
SELECT pg_current_wal_lsn() AS j1 \gset
SELECT n_batches FROM walextract_batch_stats(:'j0', :'j1');

-- case K: open transaction at range end -> not delivered, freed at teardown.
-- pg_switch_wal flushes the COPY while the txn is still open; the range ends
-- before COMMIT, so the buffered batch is never delivered.
SELECT pg_current_wal_lsn() AS k0 \gset
CREATE TABLE wxb_open (a int, b int);
BEGIN;
COPY wxb_open (a, b) FROM stdin;
1	2
3	4
\.
SELECT pg_switch_wal() AS k_sw \gset
SELECT pg_current_wal_lsn() AS k1 \gset
SELECT n_batches FROM walextract_batch_stats(:'k0', :'k1');
COMMIT;
