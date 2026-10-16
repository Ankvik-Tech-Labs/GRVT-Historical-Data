# GRVT Historical Data

Recoverable daily public trade downloads for GRVT perpetual instruments, including
equities, and continuous replayable full L2 depth at 100 ms. Release publication
is a subsequent task.

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
Same-day publication must wait beyond the observed indexing lag before sealing.

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
