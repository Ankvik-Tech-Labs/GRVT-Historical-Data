#!/bin/bash
set -euo pipefail
# Run only after Task5's measured disk/runtime gate. This does not start Docker.
[[ $(hostname) == deployment-host ]] || { echo 'refusing: live hostname must be deployment-host' >&2; exit 1; }
ZONE=$(timedatectl show -p Timezone --value)
[[ $ZONE == UTC || $ZONE == Etc/UTC ]] || { echo 'refusing: live system timezone must be UTC' >&2; exit 1; }
ROOT=/path/to/grvt-checkout
DATA=/path/to/grvt-data
USER_LOGROTATE=0
if [[ ${1:-} == --user-logrotate && $# == 1 ]]; then
    USER_LOGROTATE=1
elif [[ $# != 0 ]]; then
    echo 'usage: install-deployment-host.sh [--user-logrotate]' >&2
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
# Refuse malformed/nested managed blocks rather than deleting unrelated entries.
awk '
$0 == "# BEGIN grvt-data-daily" { if (inside) exit 2; inside=1; next }
$0 == "# END grvt-data-daily" { if (!inside) exit 2; inside=0; next }
!inside { print }
END { if (inside) exit 2 }
' "$TEMP/old" > "$TEMP/unmanaged"
# Put our block before unrelated environment assignments (especially CRON_TZ),
# so their original scope stays intact and cannot move our UTC firing time.
cat > "$TEMP/new" <<'CRON'
# BEGIN grvt-data-daily
30 0 * * * /path/to/grvt-checkout/scripts/run-daily.sh --datasets all >> /path/to/grvt-data/logs/daily.log 2>&1
*/5 * * * * /path/to/grvt-checkout/.venv/bin/grvt-data status --check --datasets all >> /path/to/grvt-data/logs/status.log 2>&1
CRON
if [[ $USER_LOGROTATE == 1 ]]; then
    [[ -x /usr/sbin/logrotate ]] || { echo 'logrotate missing' >&2; exit 1; }
    install -m 600 "$ROOT/scripts/grvt-data.logrotate" "$DATA/state/grvt-data.logrotate"
    echo "45 0 * * * /usr/sbin/logrotate --state $DATA/state/logrotate.status $DATA/state/grvt-data.logrotate" >> "$TEMP/new"
fi
echo '# END grvt-data-daily' >> "$TEMP/new"
cat "$TEMP/unmanaged" >> "$TEMP/new"
crontab "$TEMP/new"
echo "Installed GRVT cron block; backup: $BACKUP; lock timeout exit: 75"
if [[ $USER_LOGROTATE == 1 ]]; then
    echo "Installed user-owned log rotation; state: $DATA/state/logrotate.status"
else
    echo "Install log rotation separately: sudo install -m 644 $ROOT/scripts/grvt-data.logrotate /etc/logrotate.d/grvt-data"
fi
