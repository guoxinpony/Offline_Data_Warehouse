#!/usr/bin/env bash
set -e

FLAG=/app/superset_home/.db_initialized

if [ ! -f "$FLAG" ]; then
  superset db upgrade

  superset fab create-admin \
    --username "${ADMIN_USERNAME:-admin}" \
    --firstname "Superset" \
    --lastname "Admin" \
    --email "${ADMIN_EMAIL:-admin@superset.com}" \
    --password "${ADMIN_PASSWORD:-admin}" || true

  superset init
  touch "$FLAG"
fi

exec /usr/bin/run-server.sh  