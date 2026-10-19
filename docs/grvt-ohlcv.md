# Incremental historical OHLCV task brief

Implement retained GRVT TRADE candles alongside trades. The CLI defaults to
`--datasets trades,ohlcv`; explicit trades and legacy trades,depth remain valid.
No deployment, cron installation, depth collection, funding or ticker work.

Source: https://api-docs.grvt.io/market_data_api/#candlestick
The API reverses from a frozen end bound toward start_time=0. Instrument
create_time is metadata only: live evidence shows older candles. Never fill gaps
or discard native zero-volume bars. Twelve Apex-compatible intervals are exposed:
1m,5m,15m,30m,1h,2h,4h,6h,12h,1d,1w and derived calendar1mo.

Canonical files: `<data-dir>/ohlcv/<interval>/<instrument>/YYYY-MM.parquet`.
Keys are instrument,type,interval,open_time; ns use int64 and numeric values retain
strings. Read/merge one bounded month at a time. New observations replace matching
keys atomically, comparing the original page query-start timestamp so replay of
an older durable page cannot undo a newer correction. Query envelopes precede canonical writes and cursor checkpoints;
replay after interruption is idempotent. Frozen query bounds, actual request start,
raw response hashes and schema/key version remain durable.

Daily ticks run first. Recent closed candles (two bars or two days overlap) precede
bounded historical work. Global page and wall budgets prevent old backfill from
monopolizing the worker; per-series cursor state and a rotating schedule resume
next invocation. Exhaustion certifies the API query only, never gap-free bars.
Calendar months require every UTC daily bar and a fully closed month; missing days
remain unknown. Progress reports expose every interval even before any data exists.

Each combined daily invocation may publish a new immutable revision containing a
cumulative canonical OHLCV snapshot, source envelopes and progress. Existing archive
sharding/hash verification/public-pointer commit machinery is reused; earlier
assets remain referenced. Canonical OHLCV, query evidence and small progress state
are retained locally. Budget-limited snapshots explicitly say partial; only actual
start0 cursor exhaustion marks historical pagination exhausted. Tick three-month
availability is independent from OHLCV history.

## Budgets and operation

Default `--ohlcv-recent-pages 4096 --ohlcv-backfill-pages 128
--ohlcv-budget-seconds 1800`. Recent work gets up to 80% of stage time and
historical work gets the remainder. The stage preserves up to 600 seconds (at
least one third when less remains) of the worker's remaining deadline for
snapshot sealing/publication. Each API request inherits its stage deadline.
184 instruments × 11 native intervals require at least 2024 requests; at 2 rps
that is 1012 seconds before extra minute pages/retries. Budgets are limits, not
promises of daily completion. Read snapshot progress before claiming freshness.

`grvt-data daily --datasets trades,ohlcv --data-dir <data-dir>` resumes progress.
Only current yesterday is re-enqueued for partial OHLCV; old snapshots remain
immutable historical observations. Completed ticks are reused from their frozen
report/canonical files, even after transient trade pages were verified and pruned.
No redundant trade API download is needed to advance OHLCV.

Query generations preserve rejected-cursor evidence and restart the same frozen
bounds on the next invocation. Each generation's envelopes are retained locally
and in the snapshot so every observed value remains auditable. This first version
retains raw candle envelopes; it does not yet compact verified source evidence.
Each combined revision freezes copies before committing its request, and later
resumes from those exact copies even after canonical monthly corrections.
