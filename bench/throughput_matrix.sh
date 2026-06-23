#!/usr/bin/env bash
# Machine-throughput matrix: pg_waldump vs walextract event vs batch.
# Reuses test/oracle workloads + engine. Env as in pg_waldump_parity.sh.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE="$HERE/../test/oracle"
PGBIN="${PGBIN:-/usr/local/pgsql/bin}"
PGPORT="${PGPORT:-5440}"; PGHOST="${PGHOST:-/tmp}"; PGUSER="${PGUSER:-postgres}"
PGDATA="${PGDATA:-/home/pgdata}"; PARITY_DB="${PARITY_DB:-parity}"
PSQL="$PGBIN/psql"; WALDUMP="$PGBIN/pg_waldump"

"$PSQL" -p "$PGPORT" -h "$PGHOST" -U "$PGUSER" -d postgres -X -q \
  -c "DROP DATABASE IF EXISTS $PARITY_DB" -c "CREATE DATABASE $PARITY_DB"
"$PSQL" -p "$PGPORT" -h "$PGHOST" -U "$PGUSER" -d "$PARITY_DB" -X -q -f "$ORACLE/workloads_matrix.sql" >/dev/null
python3 "$ORACLE/parity_matrix.py" --mode performance \
  --port "$PGPORT" --host "$PGHOST" --user "$PGUSER" --db "$PARITY_DB" \
  --pgdata "$PGDATA" --waldump "$WALDUMP" --psql "$PSQL"
