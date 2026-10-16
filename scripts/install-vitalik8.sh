#!/bin/bash
set -euo pipefail
# Run only after Task5's measured disk/runtime gate. This does not start Docker.
[[ $(hostname) == vitalik8 ]] || { echo 'refusing: live hostname must be vitalik8' >&2; exit 1; }
ZONE=$(timedatectl show -p Timezone --value)
[[ $ZONE == UTC || $ZONE == Etc/UTC ]] || { echo 'refusing: live system timezone must be UTC' >&2; exit 1; }
ROOT=/home/avik/GRVT-Historical-Data
DATA=/home/avik/.local/share/grvt-historical-data
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
30 0 * * * /home/avik/GRVT-Historical-Data/scripts/run-daily.sh >> /home/avik/.local/share/grvt-historical-data/logs/daily.log 2>&1
*/5 * * * * /home/avik/GRVT-Historical-Data/.venv/bin/grvt-data status --check >> /home/avik/.local/share/grvt-historical-data/logs/status.log 2>&1
# END grvt-data-daily
CRON
cat "$TEMP/unmanaged" >> "$TEMP/new"
crontab "$TEMP/new"
echo "Installed GRVT cron block; backup: $BACKUP; lock timeout exit: 75"
echo "Install log rotation separately: sudo install -m 644 $ROOT/scripts/grvt-data.logrotate /etc/logrotate.d/grvt-data"
