# Continuous depth capture contract

The collector uses keyless public `v1.book.d` at `@100` on
`wss://market-data.grvt.io/ws/full`, including equity perpetuals. It discovers
active perpetuals every 60 seconds by default and opens groups of at most 20
selectors. Newly discovered instruments join on additional connections. Existing
connections keep capturing when disk pressure pauses universe growth.

```python
import asyncio
from pathlib import Path
from grvt_data.config import Config
from grvt_data.depth import collect_depth

asyncio.run(collect_depth(Config(data_dir=Path('data'))))
```

Cancellation, Ctrl-C and SIGTERM close the sockets, record shutdown outages and
seal current segments. The collector holds `state/depth.lock` independently of
the later publication lock. No streaming credentials are read or supplied.

## Ordered data and clocks

`received_ns` is the true local wall-clock sample when the serialized apply loop
receives a queued envelope. `wire_received_ns` is the earlier socket-read sample;
raw records preserve both. Depth partitions use UTC `received_ns`, declared as
`partition_clock: "received_ns"`; trades use exchange time. Wall clocks can move
backward. Neither timestamp alone defines ordering or rotation ownership.

`receipt_index` increases per serialized operation. A raw envelope and its
normalized event share the operation's index; frame position orders raw before
normalization. Outages for multiple peers may also share an operation index.
Durable index range reservations in `state/depth-receipt-index.json` prevent
index reuse across restarts, including after published segments have been pruned.
Index gaps are valid and do not mean missing market data. Venue sequence numbers,
within a connection session, validate market-data continuity.

Each connection mints its own UUID session when opened. The matching subscription
ACK must accept every requested selector and give its sequence bounds. Each
selector then needs its own sequence-`"0"` snapshot, including an empty snapshot.
The first delta equals that ACK's `first_sequence_number`; later deltas equal
`last_sequence + 1`. Snapshot zero never enters that arithmetic. Zero-size levels
remove prices. Numeric strings retain their original representations.

Any gap, failed/partial ACK, pre-snapshot delta, handshake failure, queue overflow
or socket failure reconnects the entire connection and records an outage for all
its instruments. All affected books become `ready: false` immediately when the
apply owner detects failure, before a slow socket close or reconnect backoff.
That invalid session rejects further feed/ACK messages until a new connection
opens. The reconnect has a new session and discards old counters.
Repeated failures wait 1, 2, 4, ... seconds, capped at 300 seconds; only a run lasting
at least 60 seconds resets this schedule. ACK plus snapshots have a 30-second
handshake deadline. WebSocket ping/pong (20-second interval and timeout) judges
ongoing liveness; quiet instruments have no message-silence timeout.

## Segment format and recovery

Closed files are `raw/depth/YYYY-MM-DD/<received_ns>-<uuid>.frames`. Active files
have `.open` instead. Publishers enumerate **only `.frames`**. Rotation and clock
rollback can create several segments under one date; filenames are identities,
not an ordering substitute for receipt indices.

Every frame consists of an 8-byte big-endian header (`uint32 compressed_length`,
`uint32 CRC32(compressed_payload)`) followed by that many bytes. The payload is
one independently zlib-compressed UTF-8 JSON object. Compressed and decoded limits
are each 64 MiB. `storage.read_segment(path)` is a strict streaming reader.

Every segment's first frame has `type: "checkpoint"`, its receipt index and
partition clock, and `checkpoints`, keyed by instrument. Each book checkpoint
contains `session_id`, `first_sequence`, `last_sequence` (null until first delta),
`ready`, `last_update_received_ns`, `last_update_receipt_index`, local book
`receipt_index`, bids and asks. `last_update_receipt_index` is the global operation
index of the latest applied update, distinct from the local book update count.
`ready: false` explicitly requires a subsequent ACK/snapshot anchor. Each
unavailable instrument also embeds its complete `pending_outage` inside its book
checkpoint. This includes instruments recovered before any connection is opened.
A segment opened during backoff or after midnight therefore carries unresolved
coverage and cannot claim a valid book even if the previous segment was pruned.
Only that instrument's fresh snapshot clears its pending outage. A new day's checkpoint represents
state **before** its first raw envelope is applied. A mid-day checkpoint belongs
to the newly opened segment after the cutoff. An external state file is never
required to replay a self-contained segment.

Following frames are `raw`, `snapshot`, `delta`, `outage`, or `day_boundary`.
`raw` stores `direction`, `session_id`, `wire_received_ns`, and `raw_text` for live
wire JSON (`message` dictionaries are also supported by fixture callers).
Malformed JSON is durably retained before the protocol error is raised.
Normalized snapshots/deltas carry instrument, event time, original sequence and
previous-sequence fields, session, both sides, receipt time and receipt index.
Each append flushes and fsyncs before returning, including raw before normalization;
there is no in-memory durability timer. The queue holds at most 256 operations,
the WebSocket reader queue 32 frames, and incoming messages at most 8 MiB.
Books have a 100,000-level-per-side bound.

Startup scans leftover `.open` files, keeps frames through the last valid CRC,
journals an explicit `crash_recovery` outage **before** truncation, and seals the
valid prefix. An empty/corrupt first frame yields no empty publishable segment.
Even an intact `.open` prefix records interrupted coverage. Recovery journals
live at `state/outages/<segment-stem>.json`, recording discarded bytes, affected
instruments, segment identity and an `instrument_outages` map keyed by full
instrument name. Each entry contains its own `start_received_ns` and
`start_receipt_index`: recovery seeds these from the embedded pending outage or
checkpoint's last valid update, then advances them only for durable normalized
`snapshot`/`delta` frames for that instrument. Raw, control and peer frames never
advance another instrument's start. The journal's top-level `start_received_ns`
is only a conservative aggregate summary; consumers use `instrument_outages`
for per-instrument coverage. Retrying recovery preserves existing discarded-tail
evidence and per-instrument bounds, including a crash after truncation but before
rename. Older journals without the map are upgraded from the valid prefix while
retaining the original discarded-byte evidence. Sealing must include these durable
journals, even if a crash prevented copying the outage into a subsequent segment.
On a fresh session, its first snapshot anchors replay. Missing coverage from UTC
midnight (or from the last valid state on reconnect/recovery) is explicitly an
outage, never assumed backfilled.

Outage closure on a fresh snapshot preserves each instrument's true last-valid
`start_received_ns`/`start_receipt_index` and adds `end_received_ns` and
`end_receipt_index`. Repeated reconnect failures retain the original pending
start even when the new connection has not received a snapshot. Wall-clock
rollback can make an actual start timestamp greater than the end timestamp;
operation indices still determine chronology. Recovery and pending outages
carry `clock_rollback` and `coverage_start_received_ns`, a conservative minimum
of observed clock samples since the last valid update, without replacing the
true start sample. `receipt_order_uncertain` flags malformed/regressed index
order. Consumers must retain these uncertainty flags and treat affected coverage
as unavailable/uncertain, never infer completeness from an empty/reversed wall-
clock interval or substitute the aggregate journal start for instrument bounds.

## Rotate-now and downstream ownership

Use the shared client, which handles full-universe receipts larger than
asyncio's default 64 KiB line limit:

```python
from grvt_data.depth import depth_control
receipt = await depth_control(Path('data'), 'rotate_now')
print(receipt['cutoff_received_ns'])
```

The Unix socket `data/control.sock` accepts one JSON line
`{"command":"rotate_now"}` or `{"command":"status"}` per connection. Additional
fields, including caller-supplied cutoffs, are rejected. Socket mode is `0600`.
Requests join the same bounded queue as envelope application. At rotation, the
collector closes the current segment, samples its actual clock, takes consistent
book checkpoints and atomically persists `state/depth-checkpoint.json`.

The JSON receipt (`storage.RotateReceipt.to_dict()`) has:

| Field | Meaning |
|---|---|
| `cutoff_received_ns` | Actual wall-clock sample after segment closure; numeric int64 ns for the provisional trade-bound approximation |
| `cutoff_receipt_index` | Last operation index owned by this cutoff; use with identities for ordering despite wall-clock rollback |
| `closed_segments` | Data-root-relative identities of segments closed **by this rotation**; earlier already-closed segments remain separately discoverable |
| `checkpoints` | Full per-instrument state and session/sequence provenance for continuing the next segment |
| `partition_clock` | Always `received_ns` |

A queued envelope read before rotation but applied afterward belongs to a new
segment and a higher receipt index, even if either clock is earlier than the
cutoff. Sealing reserves exact closed segment identities; it must never reopen,
slice, or assign ownership from timestamps alone. Previously closed same-window
segments are selected by their identities/receipt-index coverage as well.
Rotation retains the live session and counters; a real reconnect always requires
a new session and fresh ACK/snapshot. `status` returns disk tier and book readiness,
session and last sequence. The collector also writes `state/depth-status.json`.

## Discovery and disk contracts

Discovery writes `state/discovery/YYYY-MM-DD.json`, exactly
`{"instruments": [<full metadata dictionaries>]}`. Refreshes union the day's
metadata so an instrument disappearing later remains in that day's universe.
Task 1's `day_universe` consumes this unchanged contract.

`storage.disk_status` exposes `tier`, free/total bytes and free fraction,
`pause_universe_growth`, and `cleanup_requested`. Below 20% free of total the
tier warns. Below the greater of 10% total or 10 GiB it becomes critical, writes
`state/cleanup-request.json`, and pauses additions until free space exceeds the
warning line. The publication/CLI task owns verified pruning; the collector
never deletes unpublished bytes. Below 2 GiB it records outages, flushes/closes,
writes `state/depth-pause.json` and raises `DiskHardStop`. Startup honors the
persisted `retry_after_ns` before subscribing and repeats disk checks/backoff
while still below the hard-stop line. This uses the same 1..300-second schedule
as protocol reconnects. No deployment or service restart policy is installed here.
