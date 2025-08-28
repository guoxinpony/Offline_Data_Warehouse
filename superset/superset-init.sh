#!/usr/bin/env bash
set -e

FLAG=/app/superset_home/.post_init_done

# 1) Migration - run every time you start up to ensure the table structure is up to date
superset db upgrade

# 2) first time: creat admin
if [ ! -f "$FLAG" ]; then
  superset fab create-admin \
    --username "${ADMIN_USERNAME:-admin}" \
    --firstname "Superset" \
    --lastname "Admin" \
    --email "${ADMIN_EMAIL:-admin@superset.com}" \
    --password "${ADMIN_PASSWORD:-admin}" || true

  superset init
  touch "$FLAG"
fi

# 3) start web service
exec /usr/bin/run-server.sh
