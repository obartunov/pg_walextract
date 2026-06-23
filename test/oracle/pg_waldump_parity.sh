#!/usr/bin/env bash
# pg_waldump parity harness: correctness/coverage table.
# Oracle = pg_waldump (record classification). Compares structured classes, not text.
# Env: PGBIN PGPORT PGHOST PGUSER PGDATA PARITY_DB  (sensible defaults below)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGBIN="${PGBIN:-/usr/local/pgsql/bin}"
PGPORT="${PGPORT:-5440}"; PGHOST="${PGHOST:-/tmp}"; PGUSER="${PGUSER:-postgres}"
PGDATA="${PGDATA:-/home/pgdata}"; PARITY_DB="${PARITY_DB:-parity}"
PSQL="$PGBIN/psql"; WALDUMP="$PGBIN/pg_waldump"

"$PSQL" -p "$PGPORT" -h "$PGHOST" -U "$PGUSER" -d postgres -X -q \
  -c "DROP DATABASE IF EXISTS $PARITY_DB" -c "CREATE DATABASE $PARITY_DB"
"$PSQL" -p "$PGPORT" -h "$PGHOST" -U "$PGUSER" -d "$PARITY_DB" -X -q -f "$HERE/workloads_matrix.sql" >/dev/null
python3 "$HERE/parity_matrix.py" --mode correctness \
  --port "$PGPORT" --host "$PGHOST" --user "$PGUSER" --db "$PARITY_DB" \
  --pgdata "$PGDATA" --waldump "$WALDUMP" --psql "$PSQL"
