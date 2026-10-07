#!/usr/bin/env bash
# Registers (or updates) one Debezium outbox connector per service from outbox-connector.json.
# PUT /connectors/<name>/config is idempotent, so this is safe to re-run.
# A service's outbox table must exist first (created by its Flyway migrations on startup).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SERVICES="${SERVICES:-order inventory payment}"
DIR="$(cd "$(dirname "$0")" && pwd)"
PG_USER="${POSTGRES_USER:-nexus}"

for svc in $SERVICES; do
  has_outbox=$(docker exec nexus-postgres psql -U "$PG_USER" -d "${svc}_db" -tAc \
    "select to_regclass('public.outbox') is not null" 2>/dev/null || echo f)
  if [ "$has_outbox" != "t" ]; then
    echo "skip ${svc}-outbox: ${svc}_db has no outbox table yet (start the ${svc} service once to run migrations)"
    continue
  fi
  sed "s/__SERVICE__/${svc}/g" "$DIR/outbox-connector.json" |
    curl -sf -X PUT -H 'Content-Type: application/json' --data @- \
      "$CONNECT_URL/connectors/${svc}-outbox/config" > /dev/null
  echo "registered ${svc}-outbox"
done
