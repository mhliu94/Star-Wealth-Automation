#!/usr/bin/env bash
# Direct API commands using the same private profile as the Kafka listener.
set -euo pipefail
umask 077
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${STARWEALTH_CONFIG_DIR:-$HOME/star-configs}"
PYTHON="${STARWEALTH_PYTHON:-$SCRIPT_DIR/.venv/bin/python}"
cd -- "$SCRIPT_DIR"
exec "$PYTHON" -u -m starwealth \
    --profile "${STARWEALTH_PROFILE:-$CONFIG_DIR/star.session.json}" \
    --credentials "${STARWEALTH_CREDENTIALS:-$CONFIG_DIR/star.credentials.json}" \
    --journal "${STARWEALTH_JOURNAL:-$CONFIG_DIR/orders.sqlite3}" "$@"
