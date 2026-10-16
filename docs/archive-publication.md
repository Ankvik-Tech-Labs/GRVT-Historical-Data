# Daily archive and publication contract

Archive and publisher helpers are unlocked library calls. The CLI holds the one
publication lock across its parent, workers and every mutating `gh` subprocess.
The worker sets `GRVT_PUBLICATION_LOCK_FD`; the publisher passes that actual
numeric descriptor with `pass_fds`, including when streaming a remote asset.
Authentication belongs to host-side `gh`. The collector needs no GitHub access.

```python
from datetime import date
from grvt_data.archive import seal_revision, resume_revision, pending_dates
from grvt_data.publish import publish_revision

# trade_report comes from download_trades, after the query-bound indexing grace.
bundle = seal_revision(config, date(2026, 10, 15), trade_report,
                       deadline=monotonic_deadline)
result = publish_revision(config, date(2026, 10, 15), 1,
                          deadline=monotonic_deadline)
```

`seal_revision` refuses a second unfinished revision. Query the ledger and call
`resume_revision(config, day, revision, deadline=...)` for an existing `sealing`
or `sealed` row before downloading or sealing anything else. Resuming a committed
and pruned bundle is unnecessary: publication reconciliation uses its ledger
records and the remote assets. `publish_revision` accepts `sealed` or `committed`
rows; it cannot finish a `sealing` row.

`state/publication.sqlite` uses WAL and FULL synchronization. `revisions` retains
frozen requests (including discovery/recovery provenance), cumulative manifests,
sealed journals, commit proof and pruning state. A conservative `commit_attempted`
flag is persisted before public transitions and protects ambiguous publication
history from orphan deletion even if an outside edit later reverts the pointer. `segments` reserves each exact
`.frames` identity once and checkpoints completed normalization. `parts` records
finished byte identities and each remote verification. `per_date_runs` records
attempts independently of publication. Helpers `start_date_run`,
`finish_date_run`, and `recover_date_runs` are in `storage`; recovery is allowed
only after acquiring the global lock. Parent code writes `running` before spawn,
and an outcome before attempting another date. A failed/interrupted attempted
nonterminal date stays pending even if no worker or checkpoint ever existed.
Terminal dates remain no-ops; new closed segments or recovery journals reopen
one. Today is excluded from automatic `pending_dates`.

## Activation and coverage

Call `await depth_control(config.data_dir, 'rotate_now')` before downloading the
activation window. This shared client supports receipts larger than 64 KiB.
Pass the returned `cutoff_received_ns` as `end_ns` to `download_trades` and
`seal_revision(..., allow_today=True, end_ns=cutoff)`. The sealer validates the
persisted `state/depth-checkpoint.json` receipt. It reserves only closed identities
for that date, including earlier closed segments whose **every receipt index**
is at/before `cutoff_receipt_index`. `.open` files and later operations are excluded
even if their wall clocks moved backwards. No segment is timestamp-sliced.

The default `trade_indexing_lag_seconds=30` is an operational buffer above the
single observed 7.826196144-second visibility sample, not a venue completeness
guarantee. `download_trades` waits until `end_ns + grace` before querying, with
shrinking deadline checks; ordinary yesterday-at-00:30 calls incur no wait. The
report's `query_started_at_ns` is the earliest relevant **actual page-fetch start**,
never the retry attempt's clock sample. Each durable page envelope carries
`query_provenance: {query_started_at_ns, indexing_grace_seconds}` sampled before
advancing the lazy API iterator (a conservative lower bound including rate limits
and network retries). Instrument page/checkpoint state aggregates earliest start,
minimum applied `indexing_grace_seconds`, `queried_pages`, and
`query_provenance_complete`. Replay rebuilds these aggregates from page envelopes;
a newer progress checkpoint cannot overwrite their provenance. Complete-cache
reuse and partial cursor resume preserve earlier page timing and grace.
The report aggregates the earliest instrument start and minimum applied grace;
`requested_indexing_grace_seconds` separately records this attempt's configuration.
Missing evidence makes the aggregate timestamp/grace null and completeness false.
An empty instrument universe has no actual query and null timing.

The sealer validates every relevant instrument's original window, page count,
provenance completeness and earliest query against its configured bound, then
checks the aggregate agrees. Resuming a `sealing` revision applies this gate to
its frozen request too. Missing legacy provenance or too-early cached queries
**refuse certification**, even after waiting or fetching later pages; the fix
never silently relabels old data, drops old pages or invents query timestamps.
Pagination `complete` alone does not mean indexing provenance is certifiable.
A genuinely fresh query is required to replace an ineligible cache; an operator
must preserve existing uncommitted data/ownership when arranging that download.
Valid durable pages retain Task 1's cursor resume and complete-cache behavior. Activation remains provisional even after the
wait: the bound approximates exchange time with a receive-clock sample. Before
publication, `state/retry-queue.json` durably requires a subsequent full-day tick
download, and keeps that date pending after midnight without needing more depth.

Trades partition by exchange event time; depth partitions by true apply-loop
`received_ns`, preserving socket `wire_received_ns` and monotonic receipt indices.
Raw frames retain complete wire data. Normalized Parquet has typed
`type/instrument/received_ns/receipt_index` columns plus lossless `record_json`
for snapshots, deltas, checkpoints and outages. This keeps nested levels, numeric
strings, full per-instrument `pending_outage`, sessions and evolving provenance.
Discovery metadata and recovery journals (including journals with no publishable
frames) are included. True outage starts, conservative
`coverage_start_received_ns`, `clock_rollback` and `receipt_order_uncertain` survive.
The manifest exposes per-segment/instrument bounds, checkpoints and uncertainty.
Missing depth is unavailable, captured depth is conservatively partial or
uncertain; interval unions never upgrade uncertain coverage to complete.
Tick completeness requires the correct full-day window, every expected
instrument, successful bounded pagination exhaustion and durable finalization.

## Sealed format and restoring bytes

`staging/YYYY-MM-DD/revNN/` contains dataset assets named
`grvt-{trades,depth}-YYYY-MM-DD-revNN-partMM.tar.gz`,
`manifest-revNN.json`, and a local `journal.json` written **before any upload**.
The journal freezes asset SHA-256 and sizes. Once a part is checkpointed it is
validated and reused; neither upload retries nor pre-journal resume rebuild it.
Segment checkpoints and a post-journal/pre-ledger crash similarly resume.

PyArrow is pinned to 26.0.0; Parquet uses Zstandard level 3, version 2.6, data page
1.0, dictionary/statistics enabled and 65536-row groups. Trade rows are sorted by
Task 1; normalized depth retains frame/receipt order. Tar names are sorted, with
fixed metadata; gzip uses empty filename, mtime 0 and OS byte 255. Final data
parts are measured against `archive_part_bytes` (default 1 GiB minus one byte).
Large raw and Parquet files are split into ordered byte chunks before packaging;
manifest `depth.files` / `trades.files` records original file SHA-256, size, path
and optional ordered `chunks` paths. Extract the named parts, concatenate those
chunks **within that revision's namespace**, then verify the original file hash.
For raw depth, concatenate `segment_records[].raw_chunks` in listed order to
restore the exact `.frames` stream; `storage.read_segment` replays it strictly.
Manifest assets also fail closed if they reach 1 GiB. No mutable `manifest.json`
exists.

Each manifest is a cumulative index. `depth.assets` and `depth.segments` union
all earlier segments. Tick assets remain listed forever; each asset's
`instrument_coverage` marks older coverage `superseded_by: N` when revision N
successfully downloads that instrument. `trades.instruments` describes the latest
attempt, including failures. A failed replacement does not supersede that
instrument's earlier bytes. `files` and `segment_records` retain all reconstruction
information, so following the latest public body pointer alone enumerates the
day, without consulting an earlier manifest or deleted local data.
A correction adds a revision and new assets. To repartition already-pruned depth,
restore its published files first and retain those remote assets; the automatic
sealer reserves only newly closed segment identities and does not invent a
replacement for an already-reserved identity.

## Publication, recovery and cleanup

Publisher routing observes the actual release as absent, draft or public.
Absent creates one draft; draft resumes it; public uploads new, unreferenced
assets. All cumulative referenced dataset assets and the new manifest must have
verified size and hash before the body changes. A supported `sha256:` API digest
avoids downloading; otherwise `gh api` binary stdout is streamed into SHA-256
without a second local asset copy. Calls respect the worker deadline; large
upload/hash commands may use the remaining per-date budget. The CLI process-group
deadline also bounds CPU work and mutating descendants.

The body pointer is exactly:

```text
Active revision: N, manifest-revNN.json, sha256 <64 hex characters>
```

Only a **public** release with this verified pointer is committed. A draft body
is prepared first and publishing commits it; for an existing public release the
body PATCH commits it. Final remote state is re-read before writing the ledger.
A retry after either remote transition succeeded reconciles the local row without
a duplicate draft or redundant body PATCH. Unknown/foreign public pointers fail
closed; old committed retries never move a newer pointer backwards.

Only a mismatched orphan belonging to the current uncommitted revision, with no
public reference/history, may be deleted and reuploaded from the same sealed
bytes. Previously committed or currently referenced bytes are never repaired or
deleted. The release cap is 1000 assets; over-cap publication fails before remote
writes and never deletes data for headroom. This is documented in
[GitHub's release limits](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases).
The [release-assets API](https://docs.github.com/en/rest/releases/assets) documents
the digest field. Read-only product probes for both digest and streamed fallback
are recorded in `tests/fixtures/publish/github-readonly-2026-10-16.json`.

`prune_verified(config, now, deadline=...)` rechecks the latest public body pointer
and deletes only exact inputs/staging of ledger commits with all parts verified.
Old revisions retain historical commit proof as the cumulative pointer advances.
There is no age buffer. `.open`, unreserved segments, uncommitted bundles,
checkpoints, discovery/outage records, retry queue, SQLite/WAL and logs remain.
Successful trade Parquet and its sealed raw page envelopes may be removed;
progress checkpoints stay. Inputs reused by an unfinished revision remain.
Deletion is idempotent if a worker dies before recording `pruned`.

`prune_if_critical(config, now, deadline=...)` calls the same policy only when
Task 2's disk status requests cleanup. The frequent status supervisor must hold
(or try to acquire) the publication lock for this mutating helper; no publisher
credentials belong in the collector. The caller owns this ahead-of-daily trigger.
`retry_status` reports retry dates and `oldest_age_seconds`, with no notification
mechanism. No service, cron, public proof release or deployment is installed by
these library modules.
