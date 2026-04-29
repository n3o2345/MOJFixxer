#!/bin/sh
set -e

DATA_DIR="/app"
DEFAULTS_DIR="/opt/moj-defaults"

mkdir -p "$DATA_DIR/logs"

# Seed defaults only if the target file does not exist yet.
# This preserves user edits across container restarts while ensuring
# a fresh volume gets sensible starting files.
for f in channels.txt config.json; do
    if [ ! -f "$DATA_DIR/$f" ]; then
        echo "[entrypoint] Seeding $DATA_DIR/$f from defaults"
        cp "$DEFAULTS_DIR/$f" "$DATA_DIR/$f"
    fi
done

exec "$@"
