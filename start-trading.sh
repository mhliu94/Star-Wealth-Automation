#!/usr/bin/env bash
# KTrader listener. No credentials are embedded or sourced as shell code.
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${STARWEALTH_PYTHON:-$SCRIPT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
    printf '%s\n' "Python environment missing. Run: python3 -m venv $SCRIPT_DIR/.venv" >&2
    printf '%s\n' "Then install: $SCRIPT_DIR/.venv/bin/python -m pip install -e '$SCRIPT_DIR[kafka]'" >&2
    exit 1
fi
cd -- "$SCRIPT_DIR"
exec "$PYTHON" -u -m starwealth.ktrader "$@"
