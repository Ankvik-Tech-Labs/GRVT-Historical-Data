# GRVT Historical Data

Recoverable daily public trade downloads for GRVT perpetual instruments, including
equities, continuous replayable full L2 depth at 100 ms, and verified cumulative
daily GitHub Release archives.

Use Python 3.11 or later with an isolated environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest tests/test_trades.py -q
ruff check src tests
```

To download a completed UTC day locally (keyless production API):

```python
from datetime import date
from pathlib import Path
import json
import time

from grvt_data.api import GrvtAPI
from grvt_data.config import Config
from grvt_data.trades import day_universe, download_trades

config = Config(data_dir=Path('data'))
api = GrvtAPI(config, deadline=time.monotonic() + 3600)
try:
    day = date(2026, 10, 15)  # choose a completed day within GRVT's retention
    names = day_universe(config.data_dir, day, api.instruments(active=True))
    result = download_trades(api, config.data_dir, day, names)
    print(json.dumps(result, indent=2))
    if not result['complete']:
        raise RuntimeError(result['failures'])
finally:
    api.close()
```

The client limits requests to 2 per second, requests at most 1000 trades per page,
and retries transport failures, 429, and 5xx up to four times with exponential
backoff and Retry-After. A monotonic deadline bounds HTTP timeouts and is checked
between streamed chunks and pages. The later daily supervisor provides hard
process deadlines for CPU work too.

Each instrument/window has `data/trades/YYYY-MM-DD/<instrument>/<start>-<end>/`
containing durable `pages/*.json`, `progress.json`, and finalized `trades.parquet`.
The page envelope commits its rows and next cursor together before the progress
file advances. Re-run the same download to resume the saved cursor, reconcile
an orphan durable page after a crash, or reuse completed output. Partial temporary
files never advance progress. Cursor rejection or detected ignored cursors restart
once per attempt while deduplicating existing rows by instrument/trade ID.
A repeated/non-progressing cursor or exhausted retries leaves explicit failure
and preserves completed pages; it never certifies partial coverage.

The API's time bounds are inclusive; output is filtered exactly to `[00:00 UTC,
next 00:00 UTC)`. Both integer nanosecond timestamps and all decimal strings retain
precision. Completion requires bounded pagination exhaustion and atomic Parquet
finalization; empty days produce typed zero-row Parquet. Observed request bounds,
page/row counts and exhaustion are recorded per instrument. A current-day activation
query requires explicit `end_ns` and produces a separate provisional window;
a later full-day call starts from midnight in a distinct checkpoint directory.
The downloader waits until the query bound plus a configurable 30-second indexing
buffer before querying; activation coverage remains provisional.

Discovery records written by the depth collector use
`data/state/discovery/YYYY-MM-DD.json` with shape
`{"instruments": [{"instrument": "AAOI_USDT_Perp", ...}]}`. The downloader unions
that day's retained metadata with current discovery, keeping delisted instruments.
Malformed existing records raise rather than silently shrink the day universe.
The instrument endpoint silently truncates at its limit without a cursor; the
client requests the documented maximum (100000), and refuses saturated responses.

Production inspection evidence and its limitations are in
[tests/fixtures/trades](tests/fixtures/trades/README.md). Runtime data, virtualenvs,
credentials and generated archives are ignored by Git. No service, schedule or
release is installed by this package.

Continuous capture and its Unix rotate-now control client are documented in
[the depth capture contract](docs/depth-capture.md). Run the collector with
`asyncio.run(collect_depth(config))`; it discovers all active perpetuals, including
equities, without streaming credentials. Closed CRC-framed compressed segments,
self-contained checkpoints and explicit outages support independent replay.

Sealing, publication recovery, cumulative manifests and verified-only cleanup are
documented in [the archive publication contract](docs/archive-publication.md).

The host CLI and deployment scripts are available; installation and activation
remain separate operator steps after the measured Task 5 disk/runtime gate.

```bash
/home/avik/GRVT-Historical-Data/.venv/bin/grvt-data depth --data-dir /home/avik/.local/share/grvt-historical-data
/home/avik/GRVT-Historical-Data/.venv/bin/grvt-data daily
/home/avik/GRVT-Historical-Data/.venv/bin/grvt-data publish --date 2026-10-15
/home/avik/GRVT-Historical-Data/.venv/bin/grvt-data status
/home/avik/GRVT-Historical-Data/.venv/bin/grvt-data status --check
```

All commands accept `--data-dir`; `daily` and `publish` accept `--repo`.
The default data root is `/home/avik/.local/share/grvt-historical-data`.
`daily` derives pending past dates from durable work, attempts yesterday first,
then the backlog oldest first, and exits 1 if any attempt fails or is incomplete.
A terminal date incurs no download, seal, worker or release write. `publish --date`
retries existing sealed/unfinished revisions manually; cron calls only `daily`.
Unfinished revisions resume their original bytes before any new revision is sealed.

Both publishing commands acquire `state/run-daily.lock` once. Contention waits
60 seconds by default (`--lock-wait-seconds`), prints a UTC `lock held, skipping
run` line and exits 75. Workers inherit the actual descriptor, as do mutating
`gh` descendants, preserving exclusion if their parent dies. Each date has a
provisional 3600-second hard monotonic deadline (`--per-date-deadline-seconds`),
with whole-group TERM then KILL within 5 seconds, descendant reaping and a durable
`per_date_runs` outcome before the next date. The next lock owner recovers stale
`running` attempts as `interrupted`; even a crash before spawn remains pending
after UTC rollover. Keep the 60-minute value provisional until Task 5 measures a
full download/seal/upload cycle and sets a budget with margin.

For a current-day activation proof, first drain `daily` until no past dates remain,
then run `grvt-data rotate-now`. It prints the complete receipt followed by the
actual `cutoff_received_ns` as a numeric integer on its final line. Pass that
integer verbatim, without another rotation:

```bash
grvt-data daily --allow-today --end-time-ns 1792100000123456789
```

The example integer is illustrative; use your real receipt. ISO date strings,
values outside today's UTC window, future cutoffs and values differing from
`state/depth-checkpoint.json` are refused. The flags must appear together. This
path targets exactly today, refuses any pending past date, waits the configured
30-second indexing grace before actual trade queries, and never performs a
second rotation. Retrying the same sealed cutoff resumes/reconciles that revision.
The proof remains provisional; the retry queue requires a subsequent full-day
trade query. Trade failures return nonzero even when valid partial bytes commit.
All date calculations explicitly use UTC, independently of the shell timezone.

`status` reads local state only and takes no publication lock. It separates the
collector's apply-loop heartbeat from quiet instrument updates; WebSocket
ping/pong (20s interval/timeout) controls session readiness, and market silence
alone is not a failure. Disk fields expose free bytes, total bytes and their
fraction, with healthy/warning/critical/hard_stop tiers (20% free warning, greater
of 10% total or 10 GiB critical, 2 GiB hard stop). Retry oldest age is a plain field.
Release counts are cached **actual last remote observations**, with timestamps
and the 1000-asset cap; no cached observation means unknown, and a local-only
snapshot cannot detect outside edits made since the observation. `status --check`
atomically writes `state/status-health.json` and appends one JSON line to
`logs/status.log`; it does not also print that same line into cron's redirect.
No external notification or alerting is wired. In the critical-disk branch only,
`--check` tries the same publication lock without waiting, recovers stale runs,
and requests verified cleanup with bounded remote verification. Busy cleanup is
skipped. Unverified data is never removed for headroom.

Prepare the fixed checkout and its isolated venv on **vitalik8**:

```bash
cd /home/avik/GRVT-Historical-Data
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
mkdir -p /home/avik/.local/share/grvt-historical-data/{state,logs}
export GRVT_UID=$(id -u) GRVT_GID=$(id -g)
docker compose -f docker/compose.yml build
```

Run these activation commands only after the measured rollout gate passes:

```bash
export GRVT_UID=$(id -u) GRVT_GID=$(id -g)
docker compose -f docker/compose.yml up -d
scripts/install-vitalik8.sh
sudo install -m 644 scripts/grvt-data.logrotate /etc/logrotate.d/grvt-data
```

The installer checks the target's live `hostname` is `vitalik8` and live
`timedatectl` timezone is `UTC` or `Etc/UTC`. The host cron daemon's timezone
schedules the daily 00:30 run; no shared `CRON_TZ` assignment is added. It backs up
the existing crontab under `state/crontab-*.backup` and replaces only the named
`grvt-data-daily` block, preserving unrelated entries. The managed block goes
first so an unrelated later `CRON_TZ` assignment keeps its original scope without
changing GRVT's UTC schedule. The second entry runs
`status --check` every five minutes. Restore scheduling with
`crontab /absolute/path/to/state/crontab-TIMESTAMP-PID.backup` after inspecting the
backup; this restores all entries in that snapshot. Remove the named block to
stop GRVT scheduling while preserving unrelated entries.

The daily wrapper sets HOME/PATH explicitly and uses absolute venv paths. Its
outer `timeout --kill-after=30s 6h` runs a Linux child-subreaper supervisor, which
registers independently grouped workers before allowing them to start, forwards
TERM, escalates to KILL within 25 seconds and reaps adopted descendants. Ordinary
CLI exits propagate through the wrapper; GNU timeout reports 124 on expiry.
There is no wrapper/cron `flock`. `GRVT_DEPLOY_ROOT`, `GRVT_DATA_DIR`,
`GRVT_RUN_TIMEOUT`, `GRVT_KILL_AFTER` and `GRVT_WRAPPER_GRACE_SECONDS` allow isolated
operator/test runs; production uses the documented defaults, with the supervisor
grace kept below timeout's kill-after interval.

The collector container mounts only `/data`, runs as the configured host UID/GID,
uses `unless-stopped`, a 60-second graceful stop and heartbeat health check, and
receives no host HOME or GitHub credentials. Hard-stop pause/backoff markers are
checked by the collector before subscribing on each restart. Host-side `gh`
authentication alone publishes releases. Stop capture gracefully with
`docker compose -f docker/compose.yml stop`; inspect with `docker compose -f
docker/compose.yml ps` and `grvt-data status`. Preserve the data root, including
SQLite/WAL, receipt/discovery/outage state and retry queue across updates.
Logrotate keeps eight weekly compressed rotations of daily/status logs. For
restoring committed archive bytes, follow the exact chunk/hash reconstruction in
[the archive publication contract](docs/archive-publication.md).
