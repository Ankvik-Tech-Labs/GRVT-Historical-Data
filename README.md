# GRVT Historical Data

Daily historical **trade ticks, funding, all native TRADE/MARK/INDEX/MID candles,
full and mini tickers, and market reference data** for GRVT, retained under
`/srv/grvt` and published as verified GitHub Release archives from the same bytes.
Order-book depth is excluded. Earlier depth captures and published assets remain
historical evidence; this setup creates no new depth assets.

The October 18 expansion is described in [public dataset contracts](docs/grvt-public-data.md).
Historical funding/candles continue from durable cursors; ticker/reference data
are current observations with explicit timestamps, not fabricated past snapshots.

The repository is private. Published/non-draft releases require authorized
repository access; `draft=false` does not change repository visibility.

## Current deployment

**October 18:** all-public product `3091b3c` is deployed. October 17 revision 6
contains funding, ticker, reference and all four candle price types alongside
existing ticks/OHLCV. Authenticated restoration verified 16,051 new-family entries,
all 736 protected tick hashes and all 39 previous asset identities. The corrected
continuation exited 0; 246 tests and independent integrity review passed. Daily
and six continuation cron entries now use `--datasets all`. Funding/candles have reserved time shares so deep legacy funding cannot starve
candle collection. Added histories are partially filled and continue from durable cursors; daily observed snapshots are
complete for the current 184-market universe. Depth remains stopped.


The stable checkout is `/home/avik/GRVT-Historical-Data` on **vitalik8**, with its
isolated `.venv`. The canonical data and runtime root is **`/srv/grvt`**, owned by
`avik:data`, mode `2770`. The original root at
`/home/avik/.local/share/grvt-historical-data` remains preserved after a locked,
checksum-verified copy. No GitHub credentials are stored in the data root or a
collector container; host-side `gh` owns publication authentication.

The following October 16 evidence describes the earlier trade/OHLCV baseline;
October 18 all-public rollout evidence is recorded separately in the deployment guide.
Reviewed baseline product `0d8c1cf` was deployed. The first combined cycle completed on
2026-10-16 in 1,411.89 seconds, published October 15 revision 2, and verified all
2,024 canonical candle files against authenticated release downloads. All 184
instruments × 11 native intervals completed recent pagination. The accelerated
resume then exhausted all 2,024 start-zero historical API queries and published
revision 3: **11,025 canonical files, 1,542,632 rows**, including 1,435 derived
calendar-month partitions. Another 367 incomplete/open month records remain
unavailable. Exhaustion describes the returned API history; it does not certify
gap-free exchange history or equal historical depth across intervals. All 368 existing canonical tick hashes were
preserved. Depth remains stopped.

See [live deployment evidence](docs/deployment-vitalik8.md),
[OHLCV contracts](docs/grvt-ohlcv.md), and the
[coverage audit](docs/research/2026-10-16-market-data-gap-audit.md).

## Canonical ticks and retention

```text
/srv/grvt/
  trades/YYYY-MM-DD/INSTRUMENT/START_NS-END_NS/
    trades.parquet       # retained canonical ticks
    progress.json        # durable window/coverage metadata
    pages/               # transient resumable REST envelopes
  ohlcv/INTERVAL/INSTRUMENT/YYYY-MM.parquet  # retained canonical candles
  funding/AGGREGATION/INSTRUMENT/YYYY-MM.parquet
  ohlcv_extra/TYPE/INTERVAL/INSTRUMENT/YYYY-MM.parquet
  ticker/snapshots/OBSERVED_DAY/ROUTE/INSTRUMENT.json
  metadata/snapshots/OBSERVED_DAY/ROUTE/INSTRUMENT.json
  state/market/            # added-dataset pages, cursors, snapshots and coverage
  state/ohlcv/             # retained original candle query evidence
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

Commands for the all-public version:

```bash
cd /home/avik/GRVT-Historical-Data
.venv/bin/grvt-data daily --data-dir /srv/grvt --datasets all
.venv/bin/grvt-data publish --date 2026-10-15 --data-dir /srv/grvt --datasets all
.venv/bin/grvt-data status --data-dir /srv/grvt --datasets all
.venv/bin/grvt-data status --check --data-dir /srv/grvt --datasets all
```

`daily` downloads pending past dates, yesterday first, then oldest backlog. A
terminal full day with all selected histories exhausted is a no-op. Partial
OHLCV or additional public histories on current yesterday are re-enqueued; retained completed ticks are reused. It unions current discovery with retained daily
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
`logs/status.log`. Combined status explicitly marks the depth collector disabled.
It reports filesystem capacity/free space, revisions, actual cached release asset
counts and observation times, and retry `oldest_age_seconds`. It sends no external
notification. Critical-disk verified cleanup runs before new daily work and never
prunes canonical Parquet or unverified data.

## Schedule and logs

The combined proof and measured storage gate passed. Installed UTC entries:

- **00:30**: ticks, candles, funding and observed ticker/reference snapshots, then bounded history.
- **03:10, 07:10, 11:10, 15:10, 19:10, 23:10**: accelerated history using
  `--ohlcv-recent-pages 1 --ohlcv-backfill-pages 4096 --ohlcv-budget-seconds 1800`.
- Every five minutes: local combined status; **00:45**: user-owned logrotate.

All publication jobs share the CLI publication lock and the 3600-second per-date
hard deadline. The first combined worker used 1,411.89 seconds. A live concurrent
wrapper returned 75 and logged its UTC skip. No collector container is required.
The six backfill runs advance candle and funding cursors; they do not imply that all exchange
history is already available. Fully exhausted current-yesterday work becomes a
no-op. Source envelopes remain retained, so storage and immutable snapshot sizes
grow with history. The 1000-asset release cap continues to fail closed.

The installer replaces the base GRVT block and checks hostname/timezone. The live
installation additionally includes the bounded history entry above. To reproduce
that full live schedule, use the verified local deployment helper:

```bash
.venv/bin/python /srv/grvt/deployment-evidence/install-all-public-cron.py
```

It holds `/home/avik/.local/state/data-collector-crontab.lock` across fresh read,
backup, installer, history-entry merge, install, and byte-for-byte verification of
all unrelated entries. Running `scripts/install-vitalik8.sh --user-logrotate`
alone installs only daily/status/logrotate and removes the extra history entry;
use the full helper for this deployment. Other cron writers must use the same
shared lock. Live hostname is `vitalik8`, timezone `Etc/UTC`, cron daemon active.
The verified backup is
`/srv/grvt/state/crontab-20261016T211016-1087781.backup`; Apex and GMX schedules
were preserved. The managed block precedes unrelated environment assignments.

Logrotate config `/srv/grvt/state/grvt-data.logrotate` uses user-owned state
`/srv/grvt/state/logrotate.status`: weekly rotation, eight copies, compressed,
scoped to daily/status logs. Subsequent full-day releases have run successfully; each rollout records its
actual latest verified release in the deployment evidence.

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

See [tick-only override](docs/tick-only.md), [OHLCV collection](docs/grvt-ohlcv.md),
[archive and recovery details](docs/archive-publication.md), and
[historical depth protocol](docs/depth-capture.md). The latter describes optional
legacy behavior, not the current deployed scope. Production inspection evidence
is retained in [trade fixtures](tests/fixtures/trades/README.md). Runtime data,
credentials, virtualenvs, and generated archives stay outside Git.
