#!/bin/bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
ROOT=${GRVT_DEPLOY_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}
DATA=${GRVT_DATA_DIR:-$ROOT/data}
: "${HOME:=$(getent passwd "$(id -u)" | cut -d: -f6)}"
export HOME
exec timeout --kill-after="${GRVT_KILL_AFTER:-30s}" "${GRVT_RUN_TIMEOUT:-6h}" \
    "$ROOT/.venv/bin/python" -m grvt_data.runtime \
    "$ROOT/.venv/bin/grvt-data" daily --data-dir "$DATA" --datasets all "$@"
