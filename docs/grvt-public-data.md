# GRVT public market datasets — October 18 expansion

`--datasets all` collects public market data while excluding order-book depth.
Private account, payment and trading APIs are outside this market archive. Existing
published assets and old mixed revisions keep their frozen meaning.

| Family | Coverage and source | Canonical location below `/srv/grvt` |
|---|---|---|
| Executions | Full UTC-day `trade_history`, all discovered instruments | `trades/DAY/INSTRUMENT/START-END/trades.parquet` |
| Trade candles | Existing 11 native intervals and derived UTC calendar month | `ohlcv/INTERVAL/INSTRUMENT/YYYY-MM.parquet` |
| Additional candles | Remaining 7 TRADE intervals; all 18 MARK, INDEX and MID intervals | `ohlcv_extra/TYPE/INTERVAL/INSTRUMENT/YYYY-MM.parquet` |
| Funding | FUNDING_INTERVAL, ONE_HOURLY, FOUR_HOURLY, EIGHT_HOURLY | `funding/AGGREGATION/INSTRUMENT/YYYY-MM.parquet` |
| Tickers | Full ticker and mini: prices, open interest, funding estimates, volume, best quotes and every other returned field | `ticker/snapshots/OBSERVED_DAY/ROUTE/INSTRUMENT.json` |
| Reference | all_instruments, currency, supported_assets, collateral assets, per-instrument margin rules | `metadata/snapshots/OBSERVED_DAY/ROUTE/INSTRUMENT.json` |

Native intervals are 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h,
1d, 3d, 5d, 1w, 2w, 3w, 4w. Four weeks is not a calendar month. Calendar
months in the original TRADE dataset require every closed daily bar; incomplete
months remain unavailable. All five documented instrument kinds are discovered;
current production discovery returns 184 perpetual instruments. Funding queries
apply to perpetual and stable-perpetual kinds. Closed/delisted retained instrument
metadata remains part of the historical trade universe.

## Meaning and provenance

Funding rates retain the API's percentage-point strings. No decimal conversion
is invented. Historical `funding_interval_hours=0` is preserved as unspecified
legacy cadence; it is not replaced with eight hours. Market numerics retain
source strings and timestamps remain integer nanoseconds. Canonical histories
also retain complete `record_json` and actual page `observed_at_ns`; unknown source
fields survive normalization.

Funding and candles query from start zero with reverse cursors until actual API
exhaustion. Bounds remain fixed while paginating. Recent funding queries overlap two days; candle overlap is the larger of two days
and twice the native interval duration. Legacy short-overlap states repair from
the original history boundary, including already-exhausted states.
Monthly files union by instrument/series/event time, and newer observations win
corrections. A replay of an old page cannot undo a later correction. Inclusive
server bounds are locally filtered; unfinished bars are excluded from canonical
closed candles but their raw responses remain available. Exhaustion certifies only
returned pagination, not gap-free history, equal retention across intervals or the
absence of older exchange data. Instrument creation metadata is not a safe
historical lower bound.

Ticker and reference APIs are daily **observations**, not historical backfills.
Their actual observation day/time is explicit even when published alongside the
previous UTC day's ticks. A ticker's single best quote remains part of its payload;
no `/book` request, depth ladder or depth WebSocket is enabled. Latest executions
are covered by the historical trade tape rather than a duplicate recent-trade
snapshot API call. Public funding rates are distinct from private funding payments.

## Durability and scheduling

Raw page envelopes are written before canonical replacement and cursor advance.
A failed merge replays the durable page without another HTTP request. Cursor
rejection starts a new generation and retains the rejected generation's evidence.
Snapshot and page hashes are checked on reuse. Canonical files are atomic Parquet and snapshot JSON
mode 0660 under the data-group setgid tree. Evidence and progress live in
`state/market/pages` and `state/market/series`; `state/market/status.json` exposes
counts by aggregation/price type, actual observed ranges and partial failures.

The CLI's default is `all`. Explicit `trades`, `trades,ohlcv` and legacy
`trades,depth` modes remain available for frozen recovery. `all` expands to
`trades,ohlcv,funding,ticker,metadata,ohlcv_extra`; new revisions freeze this list.
The scheduled daily and six bounded continuation jobs use this mode. Completed
canonical trade windows are reused while other histories advance.

Defaults are 2200 additional-data requests and a 1500-second stage, further
bounded by the supervised per-date deadline with time reserved for publication.
Daily snapshot requests consume this budget once; durable round-robin offsets
resume funding and additional candle series across runs. The funding quota is at
least the instrument count or one third of the request budget, with unused quota
available to candles. A bounded initial seed can raise the request budget without
raising the hard worker deadline:

```bash
scripts/run-daily.sh --datasets all --ohlcv-recent-pages 1 \
  --ohlcv-backfill-pages 4096 --market-page-budget 10000 \
  --market-budget-seconds 1800
.venv/bin/grvt-data status --data-dir /srv/grvt --datasets all
```

An expected stage-time limit records partial coverage rather than a false API
failure; the actual parent worker deadline remains a fatal deadline. Partial
successful work is archived with explicit coverage and retried; it never
certifies the remaining series complete. Independent failures do not discard
other datasets. Requests, hashing and worker lifetime obey the existing deadline.
New work pauses at the hard free-space threshold. Before freezing snapshots the
sealer additionally reserves four copies of their measured input footprint plus
the hard-stop allowance for concurrent frozen/chunk/archive bytes.

## Release contract

Every revision includes a cumulative manifest with a `market` section, separate
funding, ticker, metadata and ohlcv-extra archive parts, and dataset coverage JSON.
The existing trades/OHLCV parts remain included. Source paths, sizes, hashes,
coverage clocks and exact query evidence are frozen before sealing is journaled;
resuming a revision never rereads later mutable canonical corrections. Every new
asset is remotely hash-verified before the public/non-draft release body points
to its manifest. Previous assets are not overwritten. The repository stays private.

Verified transient staging and eligible tick pages are pruned after publication;
canonical files, market raw pages and progress remain local. Raw evidence is
currently retained and snapshotted, so snapshots grow with history; compaction is
not implemented. Disk guards, bounded workers and the 1000-asset cap remain active.

## Verification

Offline tests cover every added view, raw precision and unknown fields, actual
legacy funding zero intervals, both documented response envelopes, repeated and
rejected cursors, inclusive bounds, correction ordering, durable crash replay,
snapshot freeze/resume, SHA-verified publication and cleanup, disk refusal,
partial scheduling, and reuse of an already-published trade day. The production
fixture in `tests/fixtures/market` is bounded evidence, not a complete history.
A temporary BTC-only live smoke exercised all four funding views and all four
price types at 3m, with zero errors after retaining the legacy zero interval.

Source: [GRVT Market Data API](https://api-docs.grvt.io/market_data_api/).
