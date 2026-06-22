-- pg_waldump-source-oracle v0 -- WAL generator (marker-only, deterministic).
-- Emits, per workload:  @@WL <name> / @@S <lsn> / @@E <lsn>
-- The driver replays each [S,E] through pg_waldump and walextract.
-- Tables are created inside their own range so descriptors are present.
\pset format unaligned
\pset tuples_only on
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS walextract;

\echo @@WL wx_i_insert
SELECT '@@S ' || pg_current_wal_lsn();
CREATE TABLE wx_i(id int, v text);
INSERT INTO wx_i SELECT g,'v'||g FROM generate_series(1,1000) g;
SELECT '@@E ' || pg_current_wal_lsn();

\echo @@WL wx_ud_updel
SELECT '@@S ' || pg_current_wal_lsn();
CREATE TABLE wx_ud(id int primary key, v text);
INSERT INTO wx_ud VALUES (1,'a'),(2,'b');
UPDATE wx_ud SET v='aa' WHERE id=1;
DELETE FROM wx_ud WHERE id=2;
SELECT '@@E ' || pg_current_wal_lsn();

\echo @@WL wx_ctas
SELECT '@@S ' || pg_current_wal_lsn();
CREATE TABLE wx_ctas AS SELECT g AS id, 'v'||g AS v FROM generate_series(1,100) g;
SELECT '@@E ' || pg_current_wal_lsn();

\echo @@WL wx_alt
SELECT '@@S ' || pg_current_wal_lsn();
CREATE TABLE wx_alt(id int);
BEGIN; ALTER TABLE wx_alt ADD COLUMN v text; INSERT INTO wx_alt VALUES (1,'x'); COMMIT;
SELECT '@@E ' || pg_current_wal_lsn();

\echo @@WL wx_toast
SELECT '@@S ' || pg_current_wal_lsn();
CREATE TABLE wx_toast(id int, v text);
INSERT INTO wx_toast SELECT 1, repeat('x',200000);
SELECT '@@E ' || pg_current_wal_lsn();

\echo @@WL wx_sv
SELECT '@@S ' || pg_current_wal_lsn();
CREATE TABLE wx_sv(id int);
BEGIN; INSERT INTO wx_sv VALUES (1); SAVEPOINT s; INSERT INTO wx_sv VALUES (2);
ROLLBACK TO s; INSERT INTO wx_sv VALUES (3); COMMIT;
SELECT '@@E ' || pg_current_wal_lsn();

\echo @@WL wx_tr_truncate
SELECT '@@S ' || pg_current_wal_lsn();
CREATE TABLE wx_tr(id int);
INSERT INTO wx_tr VALUES (1),(2);
TRUNCATE wx_tr;
INSERT INTO wx_tr VALUES (3);
SELECT '@@E ' || pg_current_wal_lsn();
