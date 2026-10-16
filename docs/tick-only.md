# Tick-only collection

For explicit tick-only operation, select `--datasets trades` and
`--data-dir /srv/grvt`. The daily/publication/status CLI, wrapper and newly generated
cron entries now select `trades,ohlcv`; see [OHLCV collection](grvt-ohlcv.md).
An already installed cron stays unchanged until the deployment owner reruns the
installer. The library retains its historical combined default:
`Config(datasets=("trades",))` selects tick-only collection;
`Config(datasets=("trades", "depth"))` selects legacy combined archives.

At 00:30 UTC, daily downloads the previous complete UTC day for all active
instruments plus retained metadata for that day. Discovery is persisted before
downloading and included in the trade archive. Exchange-time bounds, numeric
strings, pagination evidence and the 30-second query indexing grace retain their
existing contracts.

Canonical Parquet stays at:

```text
/srv/grvt/trades/YYYY-MM-DD/INSTRUMENT/START_NS-END_NS/trades.parquet
```

A window has one canonical file per instrument; atomic replacement writes the
file. Partial activation and the eventual full day use separate bounded windows.
Use the committed manifest's per-instrument coverage and supersession records
to select the current window rather than concatenating every retained file.
Parquet schemas retain int64 event-time nanoseconds and string prices/sizes.
New Parquet files are atomically installed with mode 0660; the storage root's
setgid directories supply the shared consumer group.

GitHub releases remain the second output. After verifying uploaded bytes and the
public manifest pointer, cleanup removes page envelopes and staging bundles.
Canonical Parquet and small progress/discovery/publication state remain locally.
This retention also applies when pruning older mixed revisions, protecting
canonical files restored from their verified releases.

Tick-only revisions freeze `datasets: ["trades"]` in their durable sealing request.
Retries honor that selection even if the caller changes configuration. They
reserve no depth segments and produce no new depth assets or recovery journals.
An inherited depth manifest section remains unchanged, and all previously
published assets remain immutable. Unreserved depth-only leftovers do not enqueue
new work in tick-only mode; unfinished revisions, date attempts and full-day tick
retries continue to run. A verified terminal full-day publication is a no-op.

For a provisional same-day publication, use an exact numeric exchange-time bound:

```bash
grvt-data daily --datasets trades --allow-today --end-time-ns 1792152000000000000
```

Choose a bound within the current UTC day and at or before the current time.
No depth process, socket or rotate receipt is required. This always queues the
full-day tick retry and retains the usual indexing grace and query provenance.
Ordinary daily must first drain pending older dates.

Status reports the depth collector as disabled in tick-only mode, ignoring old
depth heartbeats. Docker Compose's depth service is opt-in through the
`depth` profile; its default service selection starts no depth collector.
The installer generates the named 00:30 daily and five-minute status entries,
with optional 00:45 user log rotation, preserves unrelated cron timezone scope,
and backs up the previous crontab. Running the installer is a separate operational
action; editing these scripts does not install cron.
