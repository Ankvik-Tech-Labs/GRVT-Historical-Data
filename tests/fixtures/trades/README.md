# Production REST inspection, 2026-10-16 UTC

`production_probe_2026-10-16.json` is the complete request/response evidence for
11 read-only production requests, with local request-start/response-receipt Unix
nanoseconds. All requests used POST JSON against `https://market-data.grvt.io/full/v1/`,
a Mozilla-compatible User-Agent (the default urllib agent was rejected by the edge),
and at least 0.51 seconds between requests. No credentials were supplied.

The published contract is https://api-docs.grvt.io/market_data_api/ — filtered
instruments exposes `limit` (max 100000), no cursor/offset; trade history exposes
`limit` (max 1000), `start_time`, `end_time` and reverse-chronological `cursor`.

Observed:

- Instruments with limit 2 return 2 rows and **no pagination field**; the maximum
  limit returns 184. A saturated instrument response must fail closed.
- AAOI equity trade history succeeds. BTC cursor pagination advances to older
  trades, retaining distinct IDs at identical execution timestamps.
- Both time bounds are inclusive: at known execution timestamp `t`, `[t,t]`,
  `[t,t+1]`, `[t-1,t]` contain the execution; `[t+1,t+2]` returns an empty result
  and empty `next`. Product queries cover both boundaries and locally retain
  exactly `[start,end)`.
- The invalid literal cursor `invalid` is silently ignored, returning recent
  trades. Product recovery therefore checks stable page IDs and reverse time
  progress as well as repeated cursor strings, with at most one restart per attempt.
- Between the initial BTC lookup and the final poll, new execution `207195872-1`
  (`event_time=1792172495331521241`) became visible through history by local receipt
  `1792172503157717385`. The observed visibility upper bound is **7.826196144 s**.
  It includes polling, network, and clock error; it is one observation, not a bound
  on future indexing lag. It is comfortably below the actual 1800-second margin
  between yesterday's end and the 00:30 UTC job (not 24 hours plus 30 minutes).

Reproduce the boundary experiment by requesting two recent BTC trades, choosing
an execution timestamp `t`, then POSTing each of the four windows above with
`instrument=BTC_USDT_Perp`, decimal nanosecond strings and `limit=2`. Poll recent
history at at most 2 requests/second until an execution newer than the initial
lookup appears; subtract its event timestamp from local response receipt time.

`product_equity_day_2026-10-15.json` records the product API's real full-day AAOI
run: 412 rows, 1 durable page, terminal empty cursor, complete typed Parquet,
1.3869 seconds including discovery (184 instruments). The earliest execution
is 00:05:00.591488459 UTC, demonstrating that a bounded complete day does not
need an execution exactly at midnight. Its `/tmp` path is observational evidence,
not a committed data artifact or deployment configuration.
