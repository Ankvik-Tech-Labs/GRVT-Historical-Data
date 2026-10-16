# GRVT Daily Tick and Depth Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Await user plan review and execution-method selection before implementation.

**Goal:** Collect all active GRVT perpetuals on vitalik8 and publish verified daily tick and replayable depth archives to `Ankvik-Tech-Labs/GRVT-Historical-Data`.

**Architecture:** A persistent WebSocket collector records depth with sequence validation and UTC checkpoints. A daily job downloads bounded trade-history pages, finalizes Parquet partitions, and publishes checksummed assets through the host's authenticated `gh` CLI. Persistent storage and publishing state survive collector and host restarts.

**Tech Stack:** Python >=3.11, requests, websockets, pyarrow, Click; pytest and Ruff; Docker Compose; cron and `flock`; GitHub CLI.

**Spec:** [Approved design](../specs/2026-10-16-grvt-daily-archive-design.md).

## Global Constraints

- All active GRVT perpetual instruments, including crypto and equities; automatically discover additions.
- Actual public executions; full aggregated L2 order book via `v1.book.d`, default 100 ms publishing interval.
- At 00:30 UTC, download the previous UTC day through all history pages.
- One release per capture date: `data-YYYY-MM-DD`.
- Split archives below 1 GiB; GitHub releases retained indefinitely.
- Keep at least seven days of verified uploaded local partitions; preserve unpublished data.
- Preserve numeric strings and nanosecond timestamps; expose incomplete coverage.
- Streaming container receives no GitHub credentials. Publish from the host.

## Review Focus

- A market delisted before the daily download must remain in that day's collection universe; Task 1 tests the union with retained discovery records.
- Empty books and identical timestamps must remain replayable; Task 2 tests empty snapshots and orders by receipt index.
- A restart during UTC rollover must not publish an open or truncated segment; Task 2 tests recovery and Task 3 tests finalization refusal.
- A permission/rate-limit error during a later tick page must not certify a partial download as complete; Task 1 tests resumption and failure state.
- A crash after some release uploads must not expose a complete-looking archive or delete local data; Task 3 tests draft resumption and retention gating.

## File structure and shared interfaces

| Path | Responsibility |
|---|---|
| `src/grvt_data/config.py` | Paths, endpoints, retry/rate defaults, immutable configuration |
| `src/grvt_data/api.py` | Bounded/retried public REST requests and pagination |
| `src/grvt_data/trades.py` | UTC trade-history download and per-instrument completion |
| `src/grvt_data/storage.py` | Atomic state and segment I/O, explicit Parquet schemas |
| `src/grvt_data/depth.py` | Snapshot/delta validation, book state, reconnect and checkpoints |
| `src/grvt_data/archive.py` | Closed-day finalization, manifest and archive parts |
| `src/grvt_data/publish.py` | Draft release upload, remote hash verification and retention |
| `src/grvt_data/cli.py` | `grvt-data depth`, `daily`, `publish`, `status` commands |
| `docker/` | Collector image and Compose deployment |
| `scripts/` | Idempotent install and locked daily wrapper |
| `tests/` | Offline protocol/storage/publishing tests; opt-in live smoke |

Data root: `/home/avik/.local/share/grvt-historical-data`; deployment checkout: `/home/avik/GRVT-Historical-Data`.

Shared shapes: `Instrument = dict[str, object]`, `Trade = dict[str, object]`, `DepthEvent = dict[str, object]`; validated parsers preserve wire strings. `Config` exposes `data_dir: Path`, `repo: str`, `depth_rate_ms: int = 100`, production REST and WS URLs. Daily trade partitions use exchange UTC timestamps; raw depth partitions use receive UTC date, retaining exchange timestamps, session UUID and monotonic receipt index. Each daily book checkpoint describes the state immediately before its day's deltas.

### Task 1: Recoverable daily trade downloads and instrument inventory

**Files:** Create `pyproject.toml`, `.gitignore`, `src/grvt_data/{__init__,config,api,storage,trades}.py`, `tests/test_trades.py`, `tests/fixtures/`. Modify `README.md` with install and local tick-download instructions.

**Interfaces:**
- `GrvtAPI.instruments(*, active: bool) -> list[Instrument]` and `GrvtAPI.trade_pages(instrument: str, start_ns: int, end_ns: int, cursor: str = "") -> Iterator[dict]`.
- `download_trades(api: GrvtAPI, data_dir: Path, day: date, instruments: list[str], *, end_ns: int | None = None) -> dict` returns counts and failures; it persists progress only after durable page writes.
- `day_universe(data_dir: Path, day: date, discovered: list[Instrument]) -> list[str]` merges current and previously observed instruments for the date.
- `write_json_atomic(path: Path, payload: dict) -> None` and `write_parquet_atomic(path: Path, rows: list[dict], schema: pa.Schema) -> None` in storage.

- [ ] **Step 1: Write failing pagination and durability tests.** `test_two_pages_keep_same_timestamp_distinct_ids` asserts three distinct trade IDs survive, a duplicate ID is collapsed, decimals remain strings, and the last nanosecond of the day is retained while the next day's first nanosecond is excluded. `test_later_page_failure_resumes_without_false_completion` asserts a 429/503 followed by exhausted retries preserves cursor/progress and marks the instrument incomplete. `test_delisted_market_remains_in_day_universe` asserts retained metadata keeps yesterday's delisted instrument. Add empty-day and repeated-cursor tests; repeated cursors fail explicitly.
- [ ] **Step 2: Run `python -m pytest tests/test_trades.py -q`; confirm tests fail because the package/interfaces do not exist.**
- [ ] **Step 3: Implement these interfaces.** Use keyless production `/full/v1/instruments` and `/full/v1/trade_history`, 1000-row pages, explicit UTC nanosecond bounds, conservative 2 requests/second and bounded exponential retries honoring Retry-After. Inspect actual instrument pagination rather than assume one response contains the universe. Stage page segments atomically and deduplicate on `(instrument, trade_id)` during finalization. Configure explicit Arrow string/int64/bool schemas. Ignore runtime data, credentials, virtualenvs and generated archives in Git.
- [ ] **Step 4: Run `python -m pytest tests/test_trades.py -q` and `ruff check src tests`; require pass.**
- [ ] **Step 5: Commit the task-scoped files as `feat: add resumable GRVT daily trade downloads`.**

### Task 2: Continuous replayable depth capture

**Files:** Create `src/grvt_data/depth.py`, `tests/test_depth.py`, `tests/test_storage.py`; extend `storage.py` for append-only segments, crash recovery and checkpoint state.

**Interfaces:**
- `DepthBook.apply(message: dict, session_id: str, received_ns: int) -> list[DepthEvent]` validates continuity and raises `SequenceGap` when a new snapshot is required. Constructor: `DepthBook(instrument: str)`.
- `DepthBook.checkpoint(received_ns: int) -> DepthEvent` returns a full local snapshot with provenance.
- `SegmentWriter(data_dir: Path, dataset: str)`, `.append(row: dict) -> None`, `.close_day(day: date) -> None`, `.recover() -> list[dict]` in storage.
- `async collect_depth(config: Config) -> None` performs discovery, subscriptions, capture and graceful shutdown; it owns no GitHub state.

- [ ] **Step 1: Write failing replay tests.** `test_snapshot_deltas_delete_replay` starts with bids `100:2`, applies `100:0` and `99:3`, and asserts only `99:3` remains. `test_sequence_break_requires_snapshot` accepts the subscription ACK's first-sequence value after sequence-zero snapshot, ignores duplicate messages, and rejects an invalid predecessor. `test_empty_snapshot_and_equal_timestamps` retains an empty snapshot and orders updates by receipt index. `test_midnight_checkpoint_and_restart` replays the next day independently and verifies recovery of a truncated final raw segment records an outage rather than claiming continuity. Test failed ACK, stalled socket, and an added instrument.
- [ ] **Step 2: Run `python -m pytest tests/test_depth.py tests/test_storage.py -q`; verify relevant failures.**
- [ ] **Step 3: Implement these interfaces.** Use `/ws/full`, `v1.book.d`, selectors `<instrument>@100`; start with groups of at most 20 instruments per connection and honor actual subscription errors. Use gateway sequence semantics, subscription ACK values and predecessor checks. Record raw envelopes before normalized output; preserve empty snapshots. Track a book per instrument, rotate closed UTC segments and write synthetic checkpoints atomically. Refresh discovery at least daily, with explicit connect/disconnect, gap and low-space records. On reconnect create a new session and wait for a fresh snapshot. Flush durable writes within one second; exclude `.open` segments from publishers. Bound memory/queues and fail/reconnect explicitly on overflow.
- [ ] **Step 4: Run the Task 2 tests and lint; require pass.**
- [ ] **Step 5: Commit as `feat: capture continuous GRVT depth with replay checkpoints`.**

### Task 3: Verified daily archives and GitHub Releases

**Files:** Create `src/grvt_data/{archive,publish}.py`, `tests/test_archive.py`, `tests/test_publish.py`.

**Interfaces:**
- `finalize_day(config: Config, day: date, trade_report: dict, *, partial_until_ns: int | None = None) -> Path` returns the completed manifest path; it reads only closed depth segments and includes explicit failures/gaps.
- `publish_day(config: Config, day: date) -> dict` creates/resumes a draft release, verifies uploads by re-downloading and hashing, then publishes.
- `prune_verified(config: Config, now: datetime) -> list[Path]` removes only uploaded/verified partitions older than seven days; remote releases are never pruned.

- [ ] **Step 1: Write failing archive and publisher tests.** Assert both dataset asset names, deterministic SHA-256 and manifest coverage, partitioning below 1 GiB (inject a smaller test threshold), refusal to finalize an open day, and missing instrument status. `test_interrupted_upload_resumes_draft` injects failure on the second upload and requires draft status, intact local data and idempotent resumption. `test_hash_mismatch_blocks_publish_and_prune` requires an error and preserved local data. Test corrections cannot replace an existing verified asset with fewer records, and pruning uses per-partition verification state.
- [ ] **Step 2: Run `python -m pytest tests/test_archive.py tests/test_publish.py -q`; confirm intended failures.**
- [ ] **Step 3: Implement the interfaces.** Normalize closed rows to Parquet, split by instrument/segment before tar compression, measure final asset sizes and further split if necessary. Name assets `grvt-trades-<date>-partNN.tar.gz` and `grvt-depth-<date>-partNN.tar.gz`. Include raw depth segments, normalized data, checkpoints, instrument metadata and outage events. Invoke `gh` with argument arrays and body files; do not interpolate shell text or log credentials. Use a filesystem lock and an atomic publication journal. Serialize draft uploads, verify all expected assets against the manifest, and publish partial days only with explicit coverage labels. Keep incomplete tick dates in a persistent retry queue; reconcile them before treating later successful dates as pipeline health.
- [ ] **Step 4: Run Task 3 tests and lint; require pass.**
- [ ] **Step 5: Commit as `feat: publish verified GRVT daily release archives`.**

### Task 4: CLI, scheduling and persistent deployment

**Files:** Create `src/grvt_data/cli.py`, `docker/Dockerfile`, `docker/compose.yml`, `scripts/run-daily.sh`, `scripts/install-vitalik8.sh`, `.github/workflows/test.yml`, `tests/test_cli.py`; update `README.md`.

**Interfaces:** `grvt-data depth`, `grvt-data daily --date YYYY-MM-DD [--partial-until <UTC-ISO8601>]`, `grvt-data publish --date YYYY-MM-DD`, `grvt-data status`; all accept `--data-dir` and publisher commands accept `--repo`. The daily command defaults to yesterday UTC, downloads pending tick dates, finalizes and publishes. Failed downloads/publication produce a nonzero exit even when valid partial data is retained.

- [ ] **Step 1: Write failing CLI orchestration tests.** Inject API, storage and publisher fakes and assert the selected date/universe, failed-date queue, and nonzero status on partial tick failure. Test `status` reports collector heartbeat separately from market activity and identifies pending uploads. Deployment script review checks idempotent named cron block replacement and preservation of existing cron lines.
- [ ] **Step 2: Run `python -m pytest tests/test_cli.py -q`; confirm relevant failures.**
- [ ] **Step 3: Implement commands and runtime scripts.** Expose the package entry point. Mount only the data root into the collector container; use restart `unless-stopped`, graceful shutdown and a heartbeat health check. Run host publication in the repo's virtualenv through a `flock` wrapper at `30 0 * * *` with `CRON_TZ=UTC` and explicit PATH/HOME. Verify cron daemon support for timezone handling on this host. Back up crontab before replacement and preserve all other entries. Ensure collector/container UID and host publisher can share data. Rotate logs. Document paths, start/stop/status, manual retries and restore steps. CI runs offline tests and Ruff.
- [ ] **Step 4: Run the full offline suite, Ruff, shell syntax checks and `docker compose -f docker/compose.yml config`; require pass.**
- [ ] **Step 5: Commit as `feat: add GRVT collector deployment and daily scheduling`.**

### Task 5: Deploy and prove end-to-end operation on vitalik8

**Files:** Update `README.md` with verified operational commands; append deployment evidence to the Traycer design artifact. Runtime files stay outside Git.

**Interfaces:** Consumes the CLI and deployment scripts from Task 4. Produces a running full-universe depth collector, installed daily schedule, and a verified release containing both datasets.

- [ ] **Step 1: Run final offline validation against the exact intended deployment revision and inspect the diff for credentials/runtime data.**
- [ ] **Step 2: Update the stable deployment checkout safely to the reviewed implementation revision, install its isolated Python environment, build and start the collector, then install the daily cron block.** Check for unrelated checkout edits and existing GRVT processes before replacement. The user's original instruction authorizes this deployment and GitHub Release publication.
- [ ] **Step 3: Capture production data and verify subscription acceptance across the full universe.** Measure incoming bytes, disk growth, active instrument coverage and replay validity. Simulate a collector restart and confirm a new snapshot/outage record; verify restart after Docker restart policy activation without restarting unrelated containers. Record actual storage measurements, not extrapolations presented as guarantees.
- [ ] **Step 4: Build and publish an explicitly partial activation-day release using closed capture segments and ticks bounded to the observed interval.** Use `daily --date <activation-date> --partial-until <UTC-ISO8601>`: rotate the active depth segment, select a durable cutoff, pass it as `end_ns` to the downloader and `partial_until_ns` to finalization; never bypass the closed-day guard silently. Download its manifest and both dataset assets, verify SHA-256 and replay depth from the included snapshot. Ensure the next scheduled run can add the rest of the activation day without losing verified records.
- [ ] **Step 5: Run the installed wrapper in a controlled manual invocation and verify cron/service configuration, permissions, lock behavior and logs.** If a complete scheduled day has not elapsed, report that future run as pending observation; do not claim it already succeeded. Document the release URL, revision, runtime status, coverage and storage findings in the artifact.
- [ ] **Step 6: Perform the required code-review workflow, fix confirmed findings, rerun invalidated checks, and commit final documentation as `docs: record verified GRVT collector operation`.**

## Execution handoff

Plan ready for review. **Recommend native execution**: the tasks share inventory, storage, sequence and publication state, so implementing them in one session reduces interface handoff overhead. The user must choose native or subagent-driven execution before implementation under the writing-plans workflow. No product code or deployment changes have been made during planning.
