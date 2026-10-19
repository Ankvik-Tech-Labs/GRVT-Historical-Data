#!/bin/bash
set -euo pipefail
export HOME=/path/to/home
export PATH=/usr/local/bin:/usr/bin:/bin
# Stable deployment paths, independent of cron's cwd and shell activation.
ROOT=${GRVT_DEPLOY_ROOT:-/path/to/grvt-checkout}
DATA=${GRVT_DATA_DIR:-/path/to/grvt-data}
# runtime registers/reaps independent worker groups before timeout's final KILL.
exec timeout --kill-after="${GRVT_KILL_AFTER:-30s}" "${GRVT_RUN_TIMEOUT:-6h}" \
    "$ROOT/.venv/bin/python" -m grvt_data.runtime \
    "$ROOT/.venv/bin/grvt-data" daily --data-dir "$DATA" --datasets all "$@"
