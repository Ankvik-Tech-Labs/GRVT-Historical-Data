## Reviewed continuation and recurring setup — October 18 08:03 UTC

Reviewed product `2027a0641df5403f5b462d13d6cab9a56d466ffa` completed a real
continuation in **251.95 seconds, exit 0**, with zero failures. Its 256 history
requests reused all ticker/reference observations and the original completed
trade report exactly. No completed tick window was downloaded again.

Current [October 17 release](https://github.com/Ankvik-Tech-Labs/GRVT-Historical-Data/releases/tag/data-2026-10-17) is revision 3 with active manifest
SHA `c382bb021a250124f8550a2129f79e181eab690059b7489b1b061780352b3956` and **20 immutable assets**. Authenticated restore
verified **6,254** new-family entries and matched all
retained source hashes. All 12 previously public assets and **736** protected
tick hashes remain unchanged. Canonical/snapshot data is data-group readable.

| Added family | Actual progress at this revision |
|---|---|
| Funding | 525/736 recent queries; 227 returned historical queries exhausted; 2,980 canonical files |
| Extra TRADE/MARK/INDEX/MID candles | 69/11,224 recent queries certified under the corrected overlap policy; 95 historical queries exhausted; 1,307 canonical files |
| Full/mini tickers | 368/368 current observations |
| Market reference | 188/188 current snapshots, including 194 registry records |

Historical coverage remains partial. Legacy short-overlap progress is deliberately
not certified until repair, so recent-count comparisons across policy versions are
not monotonic; canonical bytes and original history evidence remain retained.
Added source/evidence bytes: **151,070,318**. Measured snapshot input including
existing candles is 1,090,213,721 bytes; four-copy staging reserve 4,360,854,884 bytes
plus the 2 GiB hard stop fits 687,527,981,056 free bytes: **PASS**.

At **08:03:14 UTC** the shared cron lock covered a fresh read, backup, replacement
and exact read-back. Only three GRVT dataset arguments changed to `--datasets all`:
00:30 daily, six history slots 03:10/07:10/11:10/15:10/19:10/23:10, and */5 status.
00:45 logrotate and all text outside GRVT's marker, including Apex/GMX, are byte
identical. Backup: `/srv/grvt/state/crontab-all-public-20261018T080314638143Z.backup`. Reproducible helper:
`/srv/grvt/deployment-evidence/install-all-public-cron.py`. Actual `status --check`
exited 0. Depth remains stopped; no book endpoint or subscription was enabled.

A separately monitored bounded seed advances the remaining saved funding/candle
queries using up to eight 30-minute stages; ordinary scheduled continuation owns
any work remaining afterward. Each iteration publishes its verified partial
snapshot before the next. This is not a claim of complete or gap-free history.
Portable dataset contract: [all public data](grvt-public-data.md). Detailed local
proofs: `release-proof-rev03.json`, `cron-proof.json`, `storage-proof.json`, and
`historical-seed-progress.json` in `/srv/grvt/deployment-evidence/all-public-20261018`.

# October 18 all-public extension

The expanded collector adds funding, full/mini tickers, reference snapshots and
all remaining native candle views. See [dataset contracts](grvt-public-data.md).
The first actual all-public cycle ran October 18 07:45–07:56 UTC in 664.90 seconds.
It published October 17 revision 2 on the existing private release, preserving all
four earlier asset IDs/digests. Active manifest SHA:
`86c8feb408d5afa26919899eae18ff74bc1893ec16f8558bf5aa273d80e48990`.

Authenticated download independently verified all four new dataset archives and
3,926 entries, compared all retained source hashes, and preserved 736 canonical
tick hashes. New observed snapshots: 368 full/mini tickers and 188 reference
objects (including the complete 194-record registry). Funding visited all 184
active native-interval markets; total funding history coverage was 341/736 recent
queries and 161/736 returned historical queries exhausted. Additional candles were
100/11,224 recent and 65/11,224 historical queries exhausted, across all four price
types. Canonical additions were 2,048 funding and 423 additional candle files.
Retained new source/evidence bytes total 90,775,259. Coverage is explicitly partial.

The first run's final request reached the configured 600-second stage limit;
its partial revision was correctly committed/remote-verified/pruned, but the
old handler recorded that expected limit as a failure/exit 1. Reviewed correction
`2027a06` distinguishes soft stage limits from the enforced parent deadline.
Reviewed long-candle correction `b01c69e` uses duration-aware overlap and repairs
saved legacy states; snapshot JSON also receives data-group mode 0660. Full
245-test suite and source/tests lint+format pass; the independent reviewer approved
both fixes and independently ran all 27 market cases and extra recovery probes.
A subsequent bounded continuation verifies the corrected runtime before scheduling.

The October 16 sections below remain the original verified baseline.

# Verified vitalik8 deployment — 2026-10-16 UTC

## Completion supplement — 21:40 UTC

The bounded accelerator finished successfully at21:38:46UTC in1782.057597s,
without a tick-download phase:3187additional historical pages,0recentpages.
All2024native start-zero queries reached actual API pagination exhaustion and all
recent queries remained complete; zero errors. This supersedes partial-history
and unavailable-all-months statements in the initial snapshot below. It is API
query exhaustion, not guaranteed complete/gap-free exchange history: historical
returned lower bounds differ by interval (1m2026-10-15 versus1d2024-12-06).

Current October15 revision3 pointer SHA
`29dbe4e60630d2afb3152b145d8c03dab6e8829cc86c6f777a109120d444bcd4`,
body explicitly `OHLCV: api-exhausted`. Two OHLCV assets:

| Asset suffix | Bytes | SHA256 |
|---|---:|---|
|rev03-part01.tar.gz|91,939,599|`6f9fc448a8119130c631746ce116460b567c8f8498f03ef06cbf3ccd8091cd60`|
|rev03-part02.tar.gz|6,349,749|`a64bc1f7da23ecf8079519ee05f2ed73fb4a9715c48560107d8ee36050d07e99`|

Both names start `grvt-ohlcv-2026-10-15-`. Authenticated restore checked all22621
archive entries and all11025canonical Parquet files (98,497,611bytes/1,542,632rows)
against retained bytes and0660:data permissions. All368protectedtick hashes remain
unchanged. Monthly coverage now has1435derived partitions across all184instruments;
367missing/open/incomplete month records remain unavailable. Older immutable
revision2 remains valid as its original partial snapshot.

The exhausted installed wrapper was then actually invoked: exit0 in0.603seconds,
no output, unchanged revision ledger and no new collection/publication. There is
no active benchmark or deployment worker left. The existing six bounded history
cron entries therefore skip exhausted current-yesterday work; next ordinary day
advances fresh overlap. No cron/config/product changes accompanied this supplement.
Free space after authenticated restore is751.197GB. Detailed proofs:
`ohlcv-backfill-completion-summary.json`, `ohlcv-release-proof.json`,
`ohlcv-exhausted-production-noop.json`, `ohlcv-canonical-interval-bounds.json` in
`/srv/grvt/deployment-evidence`.

## Initial combined snapshot and deployment history


Reviewed product `0d8c1cf7b0c9a9213a964ba7c1077108503c53d1` is deployed from
`/home/avik/GRVT-Historical-Data` with its isolated Python3.13 environment.
Implementation218 tests passed; independent review approved; deployment reran25
OHLCV cases successfully. Source is on draft [PR1](https://github.com/Ankvik-Tech-Labs/GRVT-Historical-Data/pull/1).
Main remains unmerged. This records a configured, verified pipeline; the initial snapshot below preceded the completed accelerator supplement above.

## Canonical data and integrity

`/srv/grvt` belongs to avik:data (1002:1009), mode2770. Canonical Parquet is0660,
with setgid parent directories. The locked migration verified2566files and
3,326,768,995bytes; original home-directory data remains preserved.

- Ticks: `trades/YYYY-MM-DD/INSTRUMENT/START_NS-END_NS/trades.parquet`.
  All368 pre-existing canonical hashes remained unchanged through the first combined
  cycle. October15 full-day184files/4,253,137bytes and October16 provisional184files/
  3,481,989bytes remain retained.
- OHLCV: `ohlcv/INTERVAL/INSTRUMENT/YYYY-MM.parquet`. First verified snapshot:
  **2024files,16,920,788bytes,361,376rows**, all184instruments ×11native intervals.
  Every canonical file was restored from the authenticated release and SHA-verified
  against local bytes, ownership and mode. All10433 archive entries were checked.
- Candle query envelopes/cursors and canonical files remain retained. Only
  committed, remotely verified transient staging/pages are eligible for cleanup.
  Frozen release bytes remain immutable while later canonical corrections advance.
- Legacy depth remains stopped since20:18:00UTC; its historical releases/state and
  unreserved41,163,147-byte continuation remain preserved. No new depth asset.

Recent queries completed for all2024series after2208pages;128historical pages
were also collected, with zero errors. Historical start bound is0; no series had
exhausted its historical pagination at this snapshot. The12th interval, true UTC
calendar1mo, exposes184 coverage records but has no populated canonical file yet:
October is open and daily history is incomplete. Missing bars remain unavailable.
API exhaustion will describe pagination, not guaranteed gap-free exchange history.

## Measured publication

[October15 release](https://github.com/Ankvik-Tech-Labs/GRVT-Historical-Data/releases/tag/data-2026-10-15)
ID415299363 now has combined revision2. The repository stays private; authorized
GitHub access is required. Published means non-draft, not anonymous access.

Manifest SHA:
`d4b8b536ac7e3c4168c4d9fe45462c802ebff141f749947d3f8e83d581513383`.
OHLCV asset `grvt-ohlcv-2026-10-15-rev02-part01.tar.gz`,15,215,347bytes,SHA
`19b8b14db7024566f571e55f18ab117064bbdb4181c830b2a0a721c23d686f83`.
Body says full-day ticks and partial historical OHLCV. Earlier assets remain.
The new revision added3assets (ticks,OHLCV,manifest); total6against1000cap.
Six similar backfill publications add18assets per current-day tag, plus daily
publication; snapshots may grow/shard, so1000cap remains a fail-closed limit.

Actual first combined cycle1411.888734s: ticks132.720259s,candles1259.847384s,
seal5.600600s,publish/verify10.654481s,cleanup1.923279s. Hard per-date3600s leaves
2188.11s observed margin. One-time tick re-download happened because legacy
frozen report paths referenced already-pruned original home files; restored
canonical files were protected. The resulting report uses retained `/srv` paths.

[October16 revision2](https://github.com/Ankvik-Tech-Labs/GRVT-Historical-Data/releases/tag/data-2026-10-16)
remains the earlier tick-only provisional proof. A completed October16 UTC-day
supersession is a future run; no such completion is claimed here.

Measured first combined staging172,203,096bytes, retained canonical/query sample
129,651,834bytes. Three working sets plus measured tick reserve require
1,072,371,142additional bytes versus376,303,349,760half-free at verification:
**storage gate PASS**. This covers measured working/reserve sets, not indefinite
history growth. Raw candle evidence is retained and cumulative immutable snapshots
will grow; source compaction is deferred.

## Schedule and ongoing backfill

Installed21:10:16UTC, live hostnamevitalik8/timezoneEtc/UTC, cron active:

| UTC time | Work |
|---|---|
|00:30|`run-daily.sh --datasets trades,ohlcv`|
|03:10,07:10,11:10,15:10,19:10,23:10|same wrapper plus `--ohlcv-recent-pages 1 --ohlcv-backfill-pages 4096 --ohlcv-budget-seconds 1800`|
|every5minutes|`grvt-data status --check --datasets trades,ohlcv`|
|00:45|user-owned logrotate, weekly8compressed policy|

Actual contention against the active backfill returned75 with UTC skip; status
check succeeded with depth disabled. All writers use the CLI publication lock.
Crontab mutation held `/home/avik/.local/state/data-collector-crontab.lock` through
fresh read/backup/merge/install/re-read; every outside-GRVT byte was identical,
including Apex00:00/08:30 and GMX03:15. Backup:
`/srv/grvt/state/crontab-20261016T211016-1087781.backup`.

Accelerated production resume started21:09:04UTC, shell
`f14c0580-2aa5-4eb9-9049-429f75a41001`, parentPID1059446/worker1059709. It entered
collect_ohlcv directly with **no tick-download phase**. Early state progressed
from128to298historical pages while preserving all2024recent completion flags.
The stage ends around21:39UTC; parent hard date budget bounds it around22:09UTC.
A monitored persistent shell delivers progress/completion and deployment owns the
subsequent release/hash supplement. This second snapshot is not yet claimed
published or verified in this report. No third benchmark is required.

The observed first128historical pages exhausted0series. Unknown venue depth and
market-specific gaps make a defensible full-history completion date unavailable.
Six bounded daily runs accelerate resumable history; oldest observed time is
progress, not evidence of the beginning of exchange history. The current-yesterday
queue becomes a no-op after full progress exhaustion; later ordinary days still
advance fresh overlap. Future cron firings and eventual exhaustion remain unobserved.

The live full-schedule installer is
`/srv/grvt/deployment-evidence/install-combined-cron.py`; it wraps the tracked base
installer and adds the bounded history entry under the same shared lock. The base
installer alone replaces the block without that extra entry. Runtime proofs,
telemetry, protected hashes, authenticated restores and cleanup inventories remain
in `/srv/grvt/deployment-evidence`; credentials are not stored there.
