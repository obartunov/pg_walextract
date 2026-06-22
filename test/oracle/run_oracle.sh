#!/bin/bash
# pg_waldump-source-oracle v0 driver.
# Replays each workload's WAL range through pg_waldump and walextract, side by side.
#
# Product code must not parse pg_waldump text.  This harness parses pg_waldump
# output only as a test/oracle artifact, to expose PostgreSQL rmgr/rm_identify
# names side by side with walextract's structured output.
#
# Env (no hardcoded paths/ports):
#   PGBIN     dir holding psql/pg_waldump  (default: dirname of `which psql`)
#   PGDATA    cluster data dir (for pg_wal) (required)
#   PGDATABASE/PGHOST/PGPORT/PGUSER         standard libpq
# Usage: PGBIN=... PGDATA=... PGUSER=postgres ./run_oracle.sh
set -euo pipefail
PGBIN="${PGBIN:-$(dirname "$(command -v psql)")}"
: "${PGDATA:?set PGDATA to the cluster data dir}"
PSQL="$PGBIN/psql"; WALDUMP="$PGBIN/pg_waldump"; WAL="$PGDATA/pg_wal"
DB="${PGDATABASE:-oracle_probe}"
HERE="$(cd "$(dirname "$0")" && pwd)"

"$PSQL" -X -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB" >/dev/null
out=$("$PSQL" -X -d "$DB" -f "$HERE/workloads.sql" 2>&1)

names=(); starts=(); ends=(); cur=""
while read -r line; do
  case "$line" in
    "@@WL "*)  cur="${line#@@WL }"; names+=("$cur") ;;
    "@@S "*)   starts+=("${line#@@S }") ;;
    "@@E "*)   ends+=("${line#@@E }") ;;
  esac
done <<< "$out"

printf '%-16s | %-44s | %s\n' "workload" "pg_waldump rmgr/type (oracle)" "walextract op:count"
printf '%s\n' "-------------------------------------------------------------------------------------------------"
for i in "${!names[@]}"; do
  wl="${names[$i]}"; s="${starts[$i]}"; e="${ends[$i]}"
  wd=$("$WALDUMP" -p "$WAL" -s "$s" -e "$e" 2>/dev/null \
        | sed -E 's/.*rmgr: ([A-Za-z0-9]+).*desc: ([A-Z_+]+).*/\1.\2/' \
        | sort | uniq -c | awk '{printf "%s(%s) ",$2,$1}')
  wx=$("$PSQL" -X -tA -d "$DB" -F: \
        -c "SELECT op||':'||count(*) FROM walextract_wal2sql('$s'::pg_lsn,'$e'::pg_lsn) GROUP BY op ORDER BY op" \
        | paste -sd' ' -)
  printf '%-16s | %-44s | %s\n' "$wl" "$wd" "$wx"
done
"$PSQL" -X -d postgres -c "DROP DATABASE IF EXISTS $DB" >/dev/null
