# GRVT Historical Data

Collect and publish GRVT public market data: trades, funding, all supported
TRADE/MARK/INDEX/MID candle views, ticker observations, and market reference
data. Order-book depth is excluded from the current collection scope. Existing
historical depth assets remain immutable.

The `--datasets all` mode includes `trades`, `ohlcv`, `ohlcv_extra`, `funding`,
`ticker`, and `metadata`. Public release assets are published only after their
hashes are verified. Dataset contracts and coverage limits are documented in
[`docs/grvt-public-data.md`](docs/grvt-public-data.md).

## Data layout

The `--data-dir` option selects the local data root. Its default is `./data` (or
the value of `GRVT_DATA_DIR`). A deployment can set that directory independently
of the source checkout.

```text
<data-dir>/
  trades/YYYY-MM-DD/INSTRUMENT/START_NS-END_NS/trades.parquet
  ohlcv/INTERVAL/INSTRUMENT/YYYY-MM.parquet
  ohlcv_extra/TYPE/INTERVAL/INSTRUMENT/YYYY-MM.parquet
  funding/AGGREGATION/INSTRUMENT/YYYY-MM.parquet
  ticker/snapshots/OBSERVED_DAY/ROUTE/INSTRUMENT.json
  metadata/snapshots/OBSERVED_DAY/ROUTE/INSTRUMENT.json
  state/                         # cursors, coverage, retries and SQLite ledger
  staging/YYYY-MM-DD/revNN/      # temporary immutable publication bytes
  logs/                          # local runtime logs
```

Daily trade partitions use complete UTC days; provisional same-day windows remain
separate and are marked in coverage metadata. Prices and sizes retain source
strings, and timestamps retain integer nanosecond precision. Canonical data and
small progress state remain local after verified publication; cleanup removes
only eligible transient pages and staging bytes.

## Run locally

```bash
uv venv --python 3.13 .venv
.venv/bin/python -m ensurepip
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests

.venv/bin/grvt-data daily --datasets all --data-dir "${GRVT_DATA_DIR:-./data}"
.venv/bin/grvt-data status --data-dir "${GRVT_DATA_DIR:-./data}" --datasets all
```

`daily` resumes durable pages and cursors, prioritizes yesterday, and publishes
partial progress with explicit coverage when bounded work remains. Fully exhausted
work becomes a no-op until new observations arrive. API pagination exhaustion does
not claim gap-free exchange history. The host's `gh` authentication is used for
GitHub publication; credentials are not copied into the data directory or a
container.

## Deployment template

`scripts/install.sh` installs a managed UTC cron block while preserving unrelated
entries. Set `GRVT_DEPLOY_ROOT`, `GRVT_DATA_DIR`, and
`GRVT_EXPECTED_HOSTNAME` for the target environment before running it. The
installer verifies the live hostname and timezone, backs up the existing crontab,
and can generate user-owned log rotation with `--user-logrotate`. Editing the
script does not install a schedule. `scripts/run-daily.sh` derives the checkout
from its own location and accepts the runtime data directory through environment
configuration.

See [`docs/tick-only.md`](docs/tick-only.md), [`docs/grvt-ohlcv.md`](docs/grvt-ohlcv.md),
[`docs/archive-publication.md`](docs/archive-publication.md), and
[`docs/depth-capture.md`](docs/depth-capture.md) for collection and recovery
contracts. Depth documentation covers legacy opt-in behavior only.
