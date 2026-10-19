#!/bin/bash
set -euo pipefail
ROOT=${GRVT_DEPLOY_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}
DATA=${GRVT_DATA_DIR:-$ROOT/data}
EXPECTED_HOSTNAME=${GRVT_EXPECTED_HOSTNAME:?set GRVT_EXPECTED_HOSTNAME for this deployment}
[[ $(hostname) == "$EXPECTED_HOSTNAME" ]] || { echo 'refusing: live hostname does not match GRVT_EXPECTED_HOSTNAME' >&2; exit 1; }
ZONE=$(timedatectl show -p Timezone --value)
[[ $ZONE == UTC || $ZONE == Etc/UTC ]] || { echo 'refusing: live system timezone must be UTC' >&2; exit 1; }
USER_LOGROTATE=0
if [[ ${1:-} == --user-logrotate && $# == 1 ]]; then
    USER_LOGROTATE=1
elif [[ $# != 0 ]]; then
    echo 'usage: install.sh [--user-logrotate]' >&2
    exit 2
fi
[[ -x "$ROOT/.venv/bin/grvt-data" && -x "$ROOT/scripts/run-daily.sh" ]] || { echo 'deployment venv/wrapper missing' >&2; exit 1; }
mkdir -p "$DATA"/{state,logs}
TEMP=$(mktemp -d)
trap 'rm -rf "$TEMP"' EXIT
if ! crontab -l > "$TEMP/old" 2> "$TEMP/error"; then
    if ! grep -qi 'no crontab' "$TEMP/error"; then cat "$TEMP/error" >&2; exit 1; fi
    : > "$TEMP/old"
fi
BACKUP="$DATA/state/crontab-$(date -u +%Y%m%dT%H%M%S)-$$.backup"
cp "$TEMP/old" "$BACKUP"
awk '
$0 == "# BEGIN grvt-data-daily" { if (inside) exit 2; inside=1; next }
$0 == "# END grvt-data-daily" { if (!inside) exit 2; inside=0; next }
!inside { print }
END { if (inside) exit 2 }
' "$TEMP/old" > "$TEMP/unmanaged"
cat > "$TEMP/new" <<CRON
# BEGIN grvt-data-daily
30 0 * * * $ROOT/scripts/run-daily.sh --datasets all >> $DATA/logs/daily.log 2>&1
*/5 * * * * $ROOT/.venv/bin/grvt-data status --check --datasets all >> $DATA/logs/status.log 2>&1
CRON
if [[ $USER_LOGROTATE == 1 ]]; then
    [[ -x /usr/sbin/logrotate ]] || { echo 'logrotate missing' >&2; exit 1; }
    cat > "$TEMP/logrotate.conf" <<LOGROTATE
$DATA/logs/daily.log $DATA/logs/status.log {
    weekly
    rotate 8
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    su $(id -un) $(id -gn)
}
LOGROTATE
    install -m 600 "$TEMP/logrotate.conf" "$DATA/state/grvt-data.logrotate"
    echo "45 0 * * * /usr/sbin/logrotate --state $DATA/state/logrotate.status $DATA/state/grvt-data.logrotate" >> "$TEMP/new"
fi
echo "# END grvt-data-daily" >> "$TEMP/new"
cat "$TEMP/unmanaged" >> "$TEMP/new"
crontab "$TEMP/new"
echo "Installed GRVT cron block; backup: $BACKUP; lock timeout exit: 75"
if [[ $USER_LOGROTATE == 1 ]]; then
    echo "Installed user-owned log rotation; state: $DATA/state/logrotate.status"
else
    echo "Install log rotation separately using the deployment data directory."
fi
