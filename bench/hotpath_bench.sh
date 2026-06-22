#!/usr/bin/env bash
# HotPath v0: SQL-mode vs event-mode (MINE_EVENT_MODE=1) over an identical WAL
# range per workload.  Lab-controlled safe start (CHECKPOINT quiet point); NOT a
# proof of arbitrary-LSN apply safety.  All paths come from the environment.
set -eu
PGBIN=${PGBIN:-$(pg_config --bindir 2>/dev/null || echo /usr/bin)}
PGHOST=${PGHOST:-/tmp}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}
PGDATA=${PGDATA:?set PGDATA}
WALDIR=${WALDIR:-$PGDATA/pg_wal}
WX_BIN=${WX_BIN:?set WX_BIN to the bench-built pg_walextract}
PSQL="$PGBIN/psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -qAt -v ON_ERROR_STOP=1"
export MINE_PGDATA=$PGDATA
export MINE_DBOID=$($PSQL -c "select oid from pg_database where datname=current_database()")

minrun(){ local em=$1 s=$2 e=$3 best= ; for r in 1 2 3; do
   local pfx; if [ "$em" = "1" ]; then pfx="env MINE_EVENT_MODE=1"; else pfx="env -u MINE_EVENT_MODE"; fi
   local a=$(date +%s%N); $pfx "$WX_BIN" -q -p "$WALDIR" -s "$s" -e "$e" >/dev/null 2>/tmp/be; local b=$(date +%s%N)
   local ms=$(echo "scale=1;($b-$a)/1000000"|bc); best=$( [ -z "$best" ] && echo $ms || echo "if($ms<$best)$ms else $best"|bc); done
   echo "$best|$(grep -ho 'BENCH .*' /tmp/be|tail -1)"; }
wl(){ local name=$1 sql=$2
  $PSQL -c CHECKPOINT >/dev/null; local s=$($PSQL -c "select pg_current_wal_lsn()")
  $PSQL -c "$sql" >/dev/null; local e=$($PSQL -c "select pg_current_wal_lsn()"); $PSQL -c CHECKPOINT >/dev/null
  local sqlr=$(minrun sql "$s" "$e"); local evr=$(minrun 1 "$s" "$e")
  echo "### $name"
  echo "  SQL  : ${sqlr%%|*} ms | ${sqlr#*|}"
  echo "  EVENT: ${evr%%|*} ms | ${evr#*|}"
}
# clean slate before AND after (re-runnable)
$PSQL -c "DROP TABLE IF EXISTS h3,h6,hs,hu" >/dev/null
wl W3_wide "BEGIN;CREATE TABLE h3(c1 int,c2 int,c3 int,c4 int,c5 int,c6 int,c7 int,c8 int,c9 int,c10 int,c11 int,c12 int,c13 int,c14 int,c15 int,c16 int,c17 int,c18 int,c19 int,c20 int,t1 text,t2 text);INSERT INTO h3 SELECT g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,'aaaa'||g,'bbbb'||g FROM generate_series(1,5000) g;COMMIT;"
wl W6_longtext "BEGIN;CREATE TABLE h6(id int,t text);ALTER TABLE h6 ALTER COLUMN t SET STORAGE PLAIN;INSERT INTO h6 SELECT g,repeat('x',3500) FROM generate_series(1,3000) g;COMMIT;"
wl Wsmall "BEGIN;CREATE TABLE hs(id int,v int);INSERT INTO hs SELECT g,g*2 FROM generate_series(1,20000) g;COMMIT;"
wl Wunknown "BEGIN;CREATE TABLE hu(id int,n numeric);INSERT INTO hu SELECT g,(g+0.5)::numeric FROM generate_series(1,8000) g;COMMIT;"
$PSQL -c "DROP TABLE IF EXISTS h3,h6,hs,hu" >/dev/null
