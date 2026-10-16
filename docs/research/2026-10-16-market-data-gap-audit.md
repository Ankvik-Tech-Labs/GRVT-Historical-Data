# Apex coverage and GRVT historical candle audit

Observed on vitalik8, 2026-10-16 around 20:30 UTC. This is read-only evidence and a
proposed implementation boundary, not a claim that OHLCV or Apex changes deployed.
GRVT's approved tick-only deployment proceeds separately. No GMX audit is included;
that work has a separate owner.

## Apex: released data versus canonical local data

The [current upstream release](https://github.com/tradingstrategy-ai/Apex-Historical-Data/releases/tag/data-2026-10-16)
was published at 02:14:15 UTC with 42 assets totaling 2,535,367,442 bytes. Actual
assets include candle archives for **1m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d,
1w, and calendar month**, funding, ticker snapshots, Freqtrade exports for supported
higher intervals, and coverage metadata. Its coverage describes 87 symbols,
1,042 series and 100,221,949 candles. Neither its actual asset list nor its coverage
shard index includes ticks or depth; the generic body table naming ticks is not
evidence that those assets exist on this release.

All 18 currently listed releases were inspected with pagination. The newest
actual tick-bearing release is [2026-10-14](https://github.com/tradingstrategy-ai/Apex-Historical-Data/releases/tag/data-2026-10-14):

| Tick asset | Bytes | API SHA-256 digest |
|---|---:|---|
| `apex-trades-part1of2.tar.gz` | 310,461,059 | `0119b883063286318a271924ce0891826a977f31ef07c3243e76a55fc046f03e` |
| `apex-trades-part2of2.tar.gz` | 438,408,481 | `589dbe0f0ba41a69fc226f3fbc03e3a0d68ea9d440fa3429cb9400b2149bb233` |

These total 748,869,540 bytes. Older tick-bearing releases exist; their increasing
snapshot sizes are not proof that the newest snapshot exhaustively contains every
older row. A future importer must inspect coverage and reconcile keys/provenance
rather than concatenate duplicate snapshots or infer completeness from size.

The live avik crontab runs `/srv/central-reporter-checkout/scripts/refresh_apex_srv_data.sh`
at **02:45 and 10:45 UTC**. That wrapper delegates to `scripts.apex_release_sync`.
At inspected central-reporter revision
`b364b5e5d845b60f90f496c9c633b1b9d1a3c6f3`, its `TIMEFRAMES` map selects only
**1m, 15m, 1h, 4h, 1d**, plus funding shards. Its resolver does not select trades,
ticker, or the remaining seven released candle intervals. Writes are atomic,
data-group mode 0660; merge keeps existing-only rows and lets release rows win
same-timestamp conflicts. It retains primary files and performs a separately
locked local mirror after freshness checks.

Actual `/srv/apex/futures` has **903 files / 2,326,192,582 bytes**: 129 files each
for the five candle intervals, hourly funding, and derived hourly mark. The extra
symbols versus latest release coverage are consistent with retained history;
no deletion is proposed. No tick/trade directory exists under `/srv/apex`.
`raw/` and `snapshots/` are separate historical caches, not an established tick
release intake. No Apex product or cron was changed in this audit.

The [current upstream release workflow](https://github.com/tradingstrategy-ai/Apex-Historical-Data/blob/c1602f057b22b68caebde8109c269fcdc769143e/.github/workflows/release-data.yml)
schedules 02:00 UTC and selects `klines,funding,ticker`; default release retention
is 14 days. Its separate live trade publisher is a distinct pipeline. Therefore
“get all release data under /srv” needs a canonical tick intake and the missing
candle/ticker classes, not another copy of the existing five-timeframe updater.
Do not alter the established consumer workflow before defining those additions.

## GRVT: official candle API and bounded live evidence

The [market data API](https://api-docs.grvt.io/market_data_api/#candlestick)
provides POST `/full/v1/kline`. It returns OHLC prices, base and quote volumes,
trade counts, and nanosecond open/close times. The [request contract](https://api-docs.grvt.io/schemas/api_candlestick_request/)
defines backward cursor pagination, a maximum page size of 1000, explicit time
bounds, and exhaustion at the requested start or available retention boundary.
No finite candle-retention duration was established by this audit.

The [interval enum](https://api-docs.grvt.io/schemas/candlestick_interval/)
offers minute intervals 1/3/5/15/30, hour intervals 1/2/4/6/8/12, day intervals
1/3/5, and week intervals 1/2/3/4. The [type enum](https://api-docs.grvt.io/schemas/candlestick_type/)
distinguishes traded, mark, index and mid prices. A four-week candle is not a
calendar-month candle. Calendar months require a documented aggregation from
closed daily bars if matching Apex's monthly series.

Exactly three small, keyless production requests were made through the existing
bounded/rate-limited client (25-second overall budget, no retries):

1. BTC traded 1m bars for 2026-10-15 00:00–00:05 UTC, limit 2: two descending
   open times, with a next cursor.
2. The same request with that cursor: one distinct older bar, then empty cursor.
   The three observed opens are 00:04, 00:03 and 00:02; no rows for 00:00/00:01
   were returned. One returned row has zero volume and zero trades. Do not invent
   missing candles or treat row count as a completeness proof.
3. BTC traded 1d bar for 2025-10-15, limit 1: one valid historical bar, no next
   cursor. This proves at least that old point exists, not exhaustive history.

A returned 00:04 bar closes at 00:05 even though the query end was 00:05 minus one
nanosecond. Historical selection must therefore enforce the chosen open-time
partition and complete-bar close bound explicitly, rather than assuming every
returned bar is closed at the query cutoff.

**Load-bearing start-bound finding:** current retained BTC instrument metadata
says `create_time=1790754389133825661` (2026-09-30), and the earliest current
universe creation timestamps are also around that date. The live 2025-10-15 candle
contradicts using this metadata as the oldest historical bound. Treat creation
metadata as annotation; a venue migration is a possible explanation, not a proven
cause. Initial all-available backfill should start at API bound **0**, or omit
`start_time` per the contract, and continue backward to actual cursor exhaustion.
Never truncate to the current instrument's creation metadata.

## Minimal next work

- **GRVT historical/incremental OHLCV:** canonical partitioned Parquet under
  `/srv/grvt`, lossless decimals and int64 timestamps, keys `(instrument, type,
  interval, open_time)`. Keep distinct backfill and forward watermarks; atomically
  checkpoint pages/cursor. Prioritize fresh closed bars, then bounded resumable
  backward history to exhaustion. Merge overlap deterministically without
  shrinking existing history; distinguish gaps, unfinished work and exhaustion.
  Publish immutable verified candle assets from the same canonical data. Native
  requested intervals plus separately labeled calendar-month aggregation can
  match Apex's actual released set; unsupported type/interval combinations must
  be explicit rather than silently substituted.
- **Apex canonical extension:** inventory coverage/asset references across retained
  releases, verify digest before extraction, and ingest available trade ticks with
  primary-key deduplication and durable source provenance. Add the seven missing
  native candle intervals and ticker class while preserving the existing locked
  five-timeframe/funding consumer and every existing canonical file. Treat absent
  latest tick assets as a freshness/coverage gap, not as proof ticks are complete.

Raw API/list/workflow/probe evidence is under `/srv/grvt/deployment-evidence/`:
`apex-latest-release.json`, `apex-release-list.json`, `apex-latest-coverage.json`,
`apex-current-release-workflow.yml`, and `grvt-kline-bounded-probe.json`.
