# GRVT Historical Data

Daily historical **trade ticks only** for all active GRVT perpetual instruments,
including equities, with retained canonical Parquet under `/srv/grvt` and verified
GitHub Release archives built from the same ticks. Order-book depth is disabled
for this setup. Earlier depth captures, state, and published assets are preserved
as historical evidence; new tick-only revisions emit no new depth assets.

The repository is private. Published/non-draft releases require authorized
repository access; `draft=false` does not change repository visibility.

## Current deployment

The stable checkout is `/home/avik/GRVT-Historical-Data` on **vitalik8**, with its
isolated `.venv`. The canonical data and runtime root is **`/srv/grvt`**, owned by
`avik:data`, mode `2770`. The original root at
`/home/avik/.local/share/grvt-historical-data` remains preserved after a locked,
checksum-verified copy. No GitHub credentials are stored in the data root or a
collector container; host-side `gh` owns publication authentication.

The user changed scope to ticks only on 2026-10-16 after measuring the depth
storage requirement. Reviewed revision `ae8d843` passed 193 tests in the stable
environment. Its live tick-only revision-2 proof retained all 184 canonical files,
verified their archive hashes, and passed the storage gate. The UTC schedule was
installed at 20:33 UTC. The depth container remains stopped. Do not start the
legacy depth profile for this deployment.

The later requested historical/incremental OHLCV expansion for GRVT and Apex is
separate work and is **not yet deployed** in this snapshot. The current schedule
explicitly selects ticks. The [coverage audit](docs/research/2026-10-16-market-data-gap-audit.md)
records actual Apex gaps and GRVT historical-candle API evidence.

## Canonical ticks and retention

```text
/srv/grvt/
  trades/YYYY-MM-DD/INSTRUMENT/START_NS-END_NS/
    trades.parquet       # retained canonical ticks
    progress.json        # durable window/coverage metadata
    pages/               # transient resumable REST envelopes
  staging/YYYY-MM-DD/revNN/  # transient immutable build bytes
  state/publication.sqlite  # revision/attempt ledger, with WAL
  state/retry-queue.json
  logs/daily.log
  logs/status.log
```

Completed UTC days use `[00:00 UTC, next 00:00 UTC)` exchange timestamps. A current
partial-day proof has a different end bound and remains provisional; its later
full-day download uses a separate complete-day directory. Consumers should select
the complete-day window rather than the newest provisional window. Numeric
strings and integer nanosecond timestamps retain precision. Typed zero-row
Parquet represents a successfully exhausted empty instrument/day.

Canonical `trades.parquet` is retained locally, including after publication and
for previously mixed revisions. Successful commit cleanup removes only eligible
transient page envelopes and staging bytes; it preserves canonical Parquet,
progress, discovery, retry work, SQLite/WAL, and any historical unpublished depth.
No new fixed retention period is imposed on canonical ticks. Archive assets stay
immutable; corrections add numbered revisions and never overwrite prior assets.

Yesterday's initial canonical data was restored from its authenticated,
SHA-256-verified release: **184 Parquet files, 4,253,137 bytes**, for 2026-10-15.
`trades/2026-10-15/canonical-index.json` maps these files to their verified source
manifest and hashes. This operator-owned migration index describes that restore;
future dates use their deterministic full-day bounds and progress metadata.
Canonical files are group-readable by `data`.

## Operate

Verified commands for the installed tick-only version:

```bash
cd /home/avik/GRVT-Historical-Data
.venv/bin/grvt-data daily --data-dir /srv/grvt --datasets trades
.venv/bin/grvt-data publish --date 2026-10-15 --data-dir /srv/grvt --datasets trades
.venv/bin/grvt-data status --data-dir /srv/grvt --datasets trades
.venv/bin/grvt-data status --check --data-dir /srv/grvt --datasets trades
```

`daily` downloads pending past dates, yesterday first, then oldest backlog. A
terminal full day is a no-op. It unions current discovery with retained daily
metadata, preserving delisted instruments. `publish --date` resumes existing
sealed work without inventing a revision. Frozen revision dataset scope controls
recovery, so old mixed work retains its original meaning.

For a same-day tick-only proof, pass an explicit current-day numeric nanosecond
bound together with `--allow-today`; no depth rotation or receipt is required.
Drain pending past dates first. The downloader waits the configured 30-second
indexing grace before actual queries, records original page-fetch provenance,
and queues a later **full-day** re-download. Partial-day tick coverage remains
provisional despite the grace period. Use the actual cutoff chosen for that run;
never reuse an illustrative timestamp.

Both publication commands hold `state/run-daily.lock` once across parent,
workers, and mutating `gh` descendants. Contention logs a UTC `lock held, skipping
run` line and exits 75 after the configured wait (default 60 seconds). Each date
has a configurable 3600-second hard worker-process deadline with bounded group
termination and durable attempt outcome. Verified pages/parts survive retries;
stale running attempts become interrupted under the next lock owner. The wrapper
adds a six-hour outer timeout and reaps worker descendants.

`status --check` writes local `state/status-health.json` and appends to
`logs/status.log`. Tick-only status explicitly marks the depth collector disabled.
It reports filesystem capacity/free space, revisions, actual cached release asset
counts and observation times, and retry `oldest_age_seconds`. It sends no external
notification. Critical-disk verified cleanup runs before new daily work and never
prunes canonical Parquet or unverified data.

## Schedule and logs

The reviewed tick-only proof and storage checks passed. The installer verifies
live hostname `vitalik8` and system timezone UTC, backs up the whole crontab, and
replaces only its named block while preserving unrelated entries:

```bash
scripts/install-vitalik8.sh --user-logrotate
```

The installed UTC entries are daily ticks at **00:30**, local status every five
minutes, and user-owned logrotate at **00:45**. There is no cron/wrapper `flock`;
the CLI owns locking. `TZ=` on a command does not change cron's firing timezone,
so the live daemon timezone check is required. The managed block precedes
unrelated environment assignments to preserve their scope.

The user-owned logrotate config is `/srv/grvt/state/grvt-data.logrotate`, with
state `/srv/grvt/state/logrotate.status`. Daily invocation honors weekly rotation,
keeps eight rotations, compresses older logs, and affects only daily/status logs.
Restore an inspected crontab backup with `crontab /absolute/path/to/backup`.

The activation backup is
`/srv/grvt/state/crontab-20261016T203330-218729.backup`. Unrelated entries were
verified byte-for-byte unchanged. A real concurrent publish/wrapper run logged a
UTC skip line and returned 75; a terminal wrapper invocation returned 0. Status
reports depth disabled and the actual observed seven assets on the October 16
release. The next full UTC-day run and its full-day tick supersession remain
future observations.

No persistent tick collector container is required: GRVT's bounded REST history
supports daily historical download. The optional legacy depth service is behind
Compose profile `depth` and is not part of this setup.

## Development and detailed contracts

```bash
uv venv --python 3.13 .venv
.venv/bin/python -m ensurepip
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
```

The REST client uses at most two requests per second, at most 1000 trades/page,
bounded exponential retries for transport/429/5xx failures, and shrinking call
budgets. Durable page envelopes commit rows and cursor together; replay
reconstructs deduplication and query provenance before advancing. Repeated or
ignored cursors cannot certify partial coverage as complete. Malformed retained
universe records and saturated instrument discovery fail closed.

See [tick-only operation](docs/tick-only.md),
[archive and recovery details](docs/archive-publication.md), and
[historical depth protocol](docs/depth-capture.md). The latter describes optional
legacy behavior, not the current deployed scope. Production inspection evidence
is retained in [trade fixtures](tests/fixtures/trades/README.md). Runtime data,
credentials, virtualenvs, and generated archives stay outside Git.
