#!/bin/bash
set -euo pipefail
export HOME=/home/avik
export PATH=/usr/local/bin:/usr/bin:/bin
# Stable deployment paths, independent of cron's cwd and shell activation.
ROOT=${GRVT_DEPLOY_ROOT:-/home/avik/GRVT-Historical-Data}
DATA=${GRVT_DATA_DIR:-/srv/grvt}
# runtime registers/reaps independent worker groups before timeout's final KILL.
exec timeout --kill-after="${GRVT_KILL_AFTER:-30s}" "${GRVT_RUN_TIMEOUT:-6h}" \
    "$ROOT/.venv/bin/python" -m grvt_data.runtime \
    "$ROOT/.venv/bin/grvt-data" daily --data-dir "$DATA" --datasets trades "$@"
