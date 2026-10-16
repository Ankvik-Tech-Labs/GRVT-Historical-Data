"""Immutable cumulative archives. Only exact, closed frame identities are owned.

The caller owns the publication lock and process deadline. SQLite stores recovery
boundaries; journal.json describes the final bytes, and is never a second ledger.
"""

import copy
import gzip
import hashlib
import json
import math
import shutil
import tarfile
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa

from .config import Config, remaining_seconds
from .storage import (
    _atomic,
    publication_db,
    read_segment,
    write_json_atomic,
    write_parquet_atomic,
)
from .trades import window_bounds

DEPTH_SCHEMA = pa.schema(
    [
        pa.field("type", pa.string()),
        pa.field("instrument", pa.string()),
        pa.field("received_ns", pa.int64()),
        pa.field("receipt_index", pa.int64()),
        # Lossless JSON keeps nested checkpoints, original numeric strings, session
        # provenance and evolving outage fields without Arrow inferring lossy types.
        pa.field("record_json", pa.string(), nullable=False),
    ]
)


def sha256(path, deadline=None):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024**2):
            remaining_seconds(deadline)
            digest.update(chunk)
    return digest.hexdigest()


def bundle_path(config, day, revision):
    return config.data_dir / "staging" / str(day) / f"rev{revision:02d}"


def retry_queue(config):
    path = config.data_dir / "state" / "retry-queue.json"
    return json.loads(path.read_text()) if path.exists() else {"dates": {}}


def retry_status(config):
    entries = retry_queue(config)["dates"]
    oldest = min((e["created_at_ns"] for e in entries.values()), default=None)
    return {
        "dates": entries,
        "oldest_age_seconds": max(0, (time.time_ns() - oldest) / 1e9)
        if oldest is not None
        else None,
    }


def _retry(config, day, required):
    queue = retry_queue(config)
    if required:
        queue["dates"].setdefault(
            str(day),
            {"created_at_ns": time.time_ns(), "reason": "full_day_ticks_required"},
        )
    else:
        queue["dates"].pop(str(day), None)
    write_json_atomic(config.data_dir / "state" / "retry-queue.json", queue)


def pending_dates(config: Config, today: date) -> set[date]:
    """Pure local candidate discovery; remote reconciliation belongs in worker."""
    db = publication_db(config)
    try:
        revisions = list(db.execute("SELECT * FROM revisions ORDER BY revision"))
        attempted = {
            date.fromisoformat(r[0])
            for r in db.execute("SELECT DISTINCT day FROM per_date_runs")
        }
        reserved = {r[0] for r in db.execute("SELECT identity FROM segments")}
        known_journals = {
            json.dumps(j, sort_keys=True)
            for row in revisions
            for j in json.loads(row["request_json"]).get("recovery_journals", [])
        }
    finally:
        db.close()
    pending = {date.fromisoformat(s) for s in retry_queue(config)["dates"]}
    known, latest = set(), {}
    for row in revisions:
        day = date.fromisoformat(row["day"])
        known.add(day)
        latest[day] = row
        if row["status"] != "committed":
            pending.add(day)
    if config.depth_enabled:
        for path in (config.data_dir / "raw" / "depth").glob("*/*.frames"):
            if str(path.relative_to(config.data_dir)) not in reserved:
                pending.add(date.fromisoformat(path.parent.name))
        for path in (config.data_dir / "state" / "outages").glob("*.json"):
            journal = json.loads(path.read_text())
            if json.dumps(journal, sort_keys=True) not in known_journals:
                parent = Path(journal["segment"]).parent
                if parent.parent == Path("raw/depth"):
                    pending.add(date.fromisoformat(parent.name))
    for day, row in latest.items():
        if (
            row["manifest_json"]
            and not json.loads(row["manifest_json"])["trades"]["full_day_complete"]
        ):
            pending.add(day)
    for day in attempted:
        row = latest.get(day)
        terminal = (
            row is not None
            and row["status"] == "committed"
            and row["remote_verified"]
            and row["manifest_json"]
            and json.loads(row["manifest_json"])["trades"]["full_day_complete"]
        )
        if not terminal:
            pending.add(day)
    yesterday = today - timedelta(days=1)
    if yesterday not in known | attempted:
        pending.add(yesterday)
    if config.ohlcv_enabled:
        row = latest.get(yesterday)
        snapshot = (
            json.loads(row["manifest_json"]).get("ohlcv", {})
            if row and row["manifest_json"]
            else {}
        )
        if snapshot.get("status") != "api-exhausted":
            pending.add(yesterday)
    return {d for d in pending if d < today}


def _validate_request(config, day, report, end_ns, allow_today):
    start, end = window_bounds(day)
    today = datetime.now(UTC).date()
    if day >= today and not allow_today:
        raise ValueError("open day requires allow_today")
    if allow_today and (
        day != today or type(end_ns) is not int or not start < end_ns < end
    ):
        raise ValueError("activation needs today and an in-day end_ns")
    if end_ns is not None and not allow_today:
        raise ValueError("end_ns requires allow_today")
    cutoff = end_ns if allow_today else end
    if (report["day"], report["start_ns"], report["end_ns"]) != (
        str(day),
        start,
        cutoff,
    ):
        raise ValueError("trade report window identity mismatch")
    _validate_query_provenance(config, report, start, cutoff)
    receipt = None
    if allow_today and config.depth_enabled:
        receipt = json.loads(
            (config.data_dir / "state" / "depth-checkpoint.json").read_text()
        )
        if receipt["cutoff_received_ns"] != end_ns:
            raise ValueError("end_ns must match durable rotate_now receipt")
    return receipt


def _validate_query_provenance(config, report, start, cutoff):
    query_ns = report.get("query_started_at_ns")
    if "query_started_at_ns" not in report:
        raise ValueError("trade report lacks actual query-start evidence")
    if config.trade_indexing_lag_seconds < 0:
        raise ValueError("negative indexing grace")
    relevant = [
        s
        for s in report["instruments"].values()
        if s.get("pages", 0) or s.get("complete")
    ]
    for state in relevant:
        started, grace = (
            state.get("query_started_at_ns"),
            state.get("indexing_grace_seconds"),
        )
        if (
            state.get("query_provenance_complete") is not True
            or type(started) is not int
            or type(grace) not in (int, float)
            or not math.isfinite(grace)
            or grace < 0
            or state.get("queried_pages") != state.get("pages")
            or not state.get("pages")
            or (state.get("start_ns"), state.get("end_ns")) != (start, cutoff)
        ):
            raise ValueError("trade instrument lacks durable query-start provenance")
        if started - cutoff < int(config.trade_indexing_lag_seconds * 1e9):
            raise ValueError(
                "trade instrument query falls inside measured indexing lag window"
            )
    if relevant and (
        report.get("query_provenance_complete") is not True
        or query_ns != min(s["query_started_at_ns"] for s in relevant)
    ):
        raise ValueError(
            "trade report query-start provenance disagrees with instruments"
        )
    if type(query_ns) is not int and (relevant or query_ns is not None):
        raise ValueError("trade report lacks actual query-start evidence")
    if query_ns is not None and query_ns - cutoff < int(
        config.trade_indexing_lag_seconds * 1e9
    ):
        raise ValueError("trade query falls inside measured indexing lag window")


def _metadata(config, day, deadline):
    path = config.data_dir / "state" / "discovery" / f"{day}.json"
    discovery = json.loads(path.read_text()) if path.exists() else {"instruments": []}
    journals = []
    for path in (
        sorted((config.data_dir / "state" / "outages").glob("*.json"))
        if config.depth_enabled
        else []
    ):
        remaining_seconds(deadline)
        payload = json.loads(path.read_text())
        if Path(payload.get("segment", "")).parent == Path("raw/depth") / str(day):
            journals.append(payload)
    return {"discovery": discovery, "recovery_journals": journals}


def seal_revision(
    config: Config,
    day: date,
    trade_report: dict,
    *,
    end_ns=None,
    allow_today=False,
    deadline=None,
    ohlcv_report=None,
) -> Path:
    receipt = _validate_request(config, day, trade_report, end_ns, allow_today)
    if not 1024 <= config.archive_part_bytes < 1024**3:
        raise ValueError("archive_part_bytes must be >=1024 and <1 GiB")
    db = publication_db(config)
    try:
        with db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute(
                "SELECT 1 FROM revisions WHERE day=? AND status!='committed'",
                (str(day),),
            ).fetchone():
                raise ValueError("day has unfinished revision; use resume_revision")
            revision = db.execute(
                "SELECT coalesce(max(revision),0)+1 FROM revisions WHERE day=?",
                (str(day),),
            ).fetchone()[0]
            request = {
                "datasets": list(config.datasets),
                "trade_report": trade_report,
                "receipt": receipt,
                "end_ns": end_ns,
                "allow_today": allow_today,
                "part_bytes": config.archive_part_bytes,
                **_metadata(config, day, deadline),
            }
            if ohlcv_report is not None:
                bundle = bundle_path(config, day, revision)
                frozen = []
                for entry in ohlcv_report["files"]:
                    remaining_seconds(deadline)
                    source = config.data_dir / entry["path"]
                    source.resolve().relative_to(config.data_dir.resolve())
                    if not entry["path"].startswith(("ohlcv/", "state/ohlcv/")):
                        raise ValueError("invalid OHLCV snapshot path")
                    target = bundle / "entries" / "ohlcv" / entry["path"]
                    _atomic(
                        target,
                        lambda temp, source=source: shutil.copyfile(source, temp),
                    )
                    record = _record(target, bundle, deadline)
                    if (record["sha256"], record["size"]) != (
                        entry["sha256"],
                        entry["size"],
                    ):
                        raise ValueError("OHLCV snapshot changed before freeze")
                    frozen.append(record)
                request["ohlcv_report"] = ohlcv_report
                request["ohlcv_entries"] = frozen
            db.execute(
                "INSERT INTO revisions(day,revision,status,request_json) VALUES (?,?,?,?)",
                (str(day), revision, "sealing", json.dumps(request, sort_keys=True)),
            )
            reserved = {r[0] for r in db.execute("SELECT identity FROM segments")}
            explicit = (
                {
                    s
                    for s in receipt["closed_segments"]
                    if Path(s).parent == Path("raw/depth") / str(day)
                }
                if receipt
                else set()
            )
            for path in (
                sorted((config.data_dir / "raw" / "depth" / str(day)).glob("*.frames"))
                if config.depth_enabled
                else []
            ):
                remaining_seconds(deadline)
                identity = str(path.relative_to(config.data_dir))
                if identity in reserved:
                    continue
                if receipt:
                    # Check EVERY frame, including raw/control, so rollback cannot
                    # cause post-cutoff frames to be silently assigned to activation.
                    maximum = None
                    for frame in read_segment(path):
                        remaining_seconds(deadline)
                        index = frame.get("receipt_index")
                        if type(index) is not int:
                            raise ValueError("activation segment lacks receipt indices")
                        maximum = index if maximum is None else max(maximum, index)
                    if maximum is None:
                        raise ValueError("empty activation segment")
                    if maximum > receipt["cutoff_receipt_index"]:
                        if identity in explicit:
                            raise ValueError(
                                "rotate receipt has inconsistent segment indices"
                            )
                        continue
                db.execute(
                    "INSERT INTO segments(identity,day,revision) VALUES (?,?,?)",
                    (identity, str(day), revision),
                )
            if explicit and not explicit.issubset(
                reserved
                | {
                    r[0]
                    for r in db.execute(
                        "SELECT identity FROM segments WHERE day=? AND revision=?",
                        (str(day), revision),
                    )
                }
            ):
                raise ValueError("receipt segment missing or outside target date")
        return resume_revision(config, day, revision, deadline=deadline)
    finally:
        db.close()


def _record(path, root, deadline=None):
    return {
        "path": str(path.relative_to(root)),
        "size": path.stat().st_size,
        "sha256": sha256(path, deadline),
    }


def _valid(record, root, deadline):
    path = root / record["path"]
    return (
        path.is_file()
        and path.stat().st_size == record["size"]
        and sha256(path, deadline) == record["sha256"]
    )


def _split_file(source, dest, root, chunk_size, deadline):
    entries = []
    with source.open("rb") as stream:
        index = 0
        while chunk := stream.read(chunk_size):
            remaining_seconds(deadline)
            path = dest / f"chunk{index:06d}"
            _atomic(path, lambda temp, chunk=chunk: temp.write_bytes(chunk))
            entries.append(_record(path, root, deadline))
            index += 1
    return entries


def _normalize_segment(
    config, identity, bundle, chunk_size, deadline, recovery_journals=None
):
    source = config.data_dir / identity
    target = bundle / "entries" / "depth" / source.stem
    raw = _split_file(source, target / "raw.frames", bundle, chunk_size, deadline)
    entries, batch, number = list(raw), [], 0
    checkpoints, outages, coverage = [], [], {}
    batch_bytes = 0
    minimum_ns = maximum_ns = previous_ns = None
    rollback = False
    last_index, uncertain = None, False
    for row in read_segment(source):
        remaining_seconds(deadline)
        index = row.get("receipt_index")
        sample = row.get("received_ns")
        if type(sample) is int:
            rollback |= previous_ns is not None and sample < previous_ns
            minimum_ns = sample if minimum_ns is None else min(sample, minimum_ns)
            maximum_ns = sample if maximum_ns is None else max(sample, maximum_ns)
            previous_ns = sample
        uncertain |= type(index) is not int or (
            last_index is not None and index < last_index
        )
        if type(index) is int:
            last_index = index
        if row["type"] == "checkpoint":
            checkpoints.append(row)
            for instrument, book in row.get("checkpoints", {}).items():
                coverage[instrument] = {
                    "status": "partial" if book.get("ready") else "unavailable",
                    "start_received_ns": book.get("last_update_received_ns"),
                    "start_receipt_index": book.get("last_update_receipt_index"),
                    "end_received_ns": book.get("last_update_received_ns"),
                    "end_receipt_index": book.get("last_update_receipt_index"),
                    "session_ids": [book["session_id"]]
                    if book.get("session_id")
                    else [],
                }
                if book.get("pending_outage"):
                    outages.append(book["pending_outage"])
        elif row["type"] == "outage":
            outages.append(row)
        if row["type"] in ("snapshot", "delta"):
            instrument = row["instrument"]
            state = coverage.setdefault(
                instrument,
                {
                    "status": "partial",
                    "start_received_ns": sample,
                    "start_receipt_index": index,
                    "session_ids": [],
                },
            )
            state["end_received_ns"], state["end_receipt_index"] = sample, index
            if row.get("session_id") and row["session_id"] not in state["session_ids"]:
                state["session_ids"].append(row["session_id"])
        if row["type"] != "raw":
            batch.append(
                {
                    k: row.get(k)
                    for k in ("type", "instrument", "received_ns", "receipt_index")
                }
                | {
                    "record_json": json.dumps(
                        row, sort_keys=True, separators=(",", ":")
                    )
                }
            )
            batch_bytes += len(batch[-1]["record_json"])
        if len(batch) >= 1000 or batch_bytes >= 16 * 1024**2:
            path = target / f"normalized{number:06d}.parquet"
            write_parquet_atomic(path, batch, DEPTH_SCHEMA)
            entries.append(_record(path, bundle, deadline))
            batch, number, batch_bytes = [], number + 1, 0
    if batch:
        path = target / f"normalized{number:06d}.parquet"
        write_parquet_atomic(path, batch, DEPTH_SCHEMA)
        entries.append(_record(path, bundle, deadline))
    # Journals are small operational state: keep them locally and include them
    # even if recovery died before appending an outage into another segment.
    payload = next(
        (
            j
            for j in (recovery_journals or [])
            if Path(j.get("segment", "")).stem == source.stem
        ),
        None,
    )
    if payload is not None:
        outages.extend(payload.get("instrument_outages", {}).values() or [payload])
        path = target / "recovery.json"
        write_json_atomic(path, payload)
        entries.append(_record(path, bundle, deadline))
    for instrument, state in coverage.items():
        instrument_outages = [
            o for o in outages if o.get("instrument", instrument) == instrument
        ]
        state["outages"] = instrument_outages
        state["clock_rollback"] = rollback or any(
            o.get("clock_rollback") for o in instrument_outages
        )
        state["receipt_order_uncertain"] = uncertain or any(
            o.get("receipt_order_uncertain") for o in instrument_outages
        )
        if (
            instrument_outages
            or state["clock_rollback"]
            or state["receipt_order_uncertain"]
        ):
            state["status"] = "uncertain"
    return {
        "identity": identity,
        "raw_chunks": raw,
        "entries": entries,
        "checkpoints": checkpoints,
        "outages": outages,
        "receipt_order_uncertain": uncertain,
        "clock_rollback": rollback,
        "instruments": coverage,
        "observed_received_ns": [minimum_ns, maximum_ns]
        if minimum_ns is not None
        else None,
    }


def _tar(path, entries, root):
    def write(temp):
        with (
            temp.open("wb") as out,
            gzip.GzipFile(
                filename="", mode="wb", fileobj=out, mtime=0, compresslevel=6
            ) as gz,
            tarfile.open(fileobj=gz, mode="w|", format=tarfile.GNU_FORMAT) as tar,
        ):
            for entry in sorted(entries, key=lambda x: x["path"]):
                source = root / entry["path"]
                info = tarfile.TarInfo(entry["path"])
                info.size = source.stat().st_size
                info.mtime = info.uid = info.gid = 0
                info.mode = 0o644
                with source.open("rb") as stream:
                    tar.addfile(info, stream)

    _atomic(path, write)


def _parts(config, db, day, revision, dataset, entries, bundle, cap, deadline):
    # Entries may themselves exceed the final cap (e.g. one instrument's
    # Parquet). Byte chunks are lossless; manifest entries specify reconstruction.
    bounded = []
    for entry in sorted(entries, key=lambda x: x["path"]):
        if entry["size"] > cap // 4:
            chunks = _split_file(
                bundle / entry["path"],
                bundle / (entry["path"] + ".chunks"),
                bundle,
                max(1, cap // 4),
                deadline,
            )
            bounded.extend(chunks)
            entry["chunks"] = [c["path"] for c in chunks]
        else:
            bounded.append(entry)
    groups, group, size = [], [], 0
    for entry in bounded:
        if group and size + entry["size"] + 1024 > cap // 2:
            groups.append(group)
            group, size = [], 0
        group.append(entry)
        size += entry["size"] + 1024
    if group:
        groups.append(group)
    result = []
    for number, group in enumerate(groups, 1):
        remaining_seconds(deadline)
        name = f"grvt-{dataset}-{day}-rev{revision:02d}-part{number:02d}.tar.gz"
        saved = db.execute(
            "SELECT record_json FROM parts WHERE day=? AND revision=? AND name=?",
            (str(day), revision, name),
        ).fetchone()
        if saved:
            asset = json.loads(saved[0])
            if asset["entries"] != group:
                raise ValueError("durable part inputs changed; refusing to mix builds")
            if not _valid(asset, bundle, deadline):
                raise ValueError("durable part hash mismatch; refusing rebuild")
        else:
            path = bundle / name
            _tar(path, group, bundle)
            if path.stat().st_size > cap:
                raise ValueError("compressed part exceeds configured cap; fail closed")
            asset = _record(path, bundle, deadline) | {
                "name": name,
                "dataset": dataset,
                "revision": revision,
                "entries": group,
            }
            with db:
                db.execute(
                    "INSERT INTO parts(day,revision,name,record_json) VALUES (?,?,?,?)",
                    (str(day), revision, name, json.dumps(asset, sort_keys=True)),
                )
        result.append(asset)
    return result


def verify_journal(bundle, journal, deadline=None):
    for asset in journal["assets"]:
        if not _valid(asset, bundle, deadline):
            raise ValueError(f"sealed byte mismatch: {asset['path']}")


def resume_revision(config: Config, day: date, revision: int, *, deadline=None) -> Path:
    db = publication_db(config)
    try:
        row = db.execute(
            "SELECT * FROM revisions WHERE day=? AND revision=?", (str(day), revision)
        ).fetchone()
        if row is None:
            raise ValueError("unknown revision")
        bundle = bundle_path(config, day, revision)
        if row["status"] != "sealing":
            verify_journal(bundle, json.loads(row["journal_json"]), deadline)
            return bundle
        request = json.loads(row["request_json"])
        report, cap = request["trade_report"], request["part_bytes"]
        # Historical requests predate datasets and always captured both.
        depth_enabled = "depth" in request.get("datasets", ["trades", "depth"])
        _validate_query_provenance(config, report, report["start_ns"], report["end_ns"])
        bundle.mkdir(parents=True, exist_ok=True)
        for temp in bundle.rglob(".*.tmp"):
            remaining_seconds(deadline)
            if temp.is_file():
                temp.unlink()
        journal_path = bundle / "journal.json"
        if journal_path.exists():
            # Crash after journaling but before the SQLite state transition.
            journal = json.loads(journal_path.read_text())
            verify_journal(bundle, journal, deadline)
            manifest = json.loads(
                (bundle / f"manifest-rev{revision:02d}.json").read_text()
            )
            _finish_seal(db, day, revision, manifest, journal)
            return bundle
        segments, depth_entries = [], []
        for seg in db.execute(
            "SELECT * FROM segments WHERE day=? AND revision=? ORDER BY identity",
            (str(day), revision),
        ).fetchall():
            remaining_seconds(deadline)
            state = (
                json.loads(seg["normalized_json"]) if seg["normalized_json"] else None
            )
            if state is None:
                state = _normalize_segment(
                    config,
                    seg["identity"],
                    bundle,
                    min(16 * 1024**2, cap // 4),
                    deadline,
                    request["recovery_journals"],
                )
                with db:
                    db.execute(
                        "UPDATE segments SET normalized_json=? WHERE identity=?",
                        (json.dumps(state, sort_keys=True), seg["identity"]),
                    )
            if not all(_valid(e, bundle, deadline) for e in state["entries"]):
                raise ValueError("durable normalized segment mismatch")
            segments.append(state)
            depth_entries.extend(state["entries"])
        discovery = request["discovery"]
        recovery_journals = request["recovery_journals"]
        if depth_enabled:
            for payload in recovery_journals:
                remaining_seconds(deadline)
                name = Path(payload["segment"]).stem + ".json"
                target = bundle / "entries" / "depth" / "outages" / name
                write_json_atomic(target, payload)
                depth_entries.append(_record(target, bundle, deadline))
            metadata_path = bundle / "entries" / "depth" / "discovery.json"
            write_json_atomic(metadata_path, discovery)
            depth_entries.append(_record(metadata_path, bundle, deadline))
        trade_entries, inputs = [], []
        if not depth_enabled:
            metadata_path = bundle / "entries" / "trades" / "discovery.json"
            write_json_atomic(metadata_path, discovery)
            trade_entries.append(_record(metadata_path, bundle, deadline))
        instrument_states = copy.deepcopy(report["instruments"])
        for item in discovery["instruments"]:
            instrument_states.setdefault(
                item["instrument"], {"complete": False, "error": "missing trade report"}
            )
        for instrument, state in sorted(instrument_states.items()):
            remaining_seconds(deadline)
            if not state.get("complete"):
                continue
            path = Path(state["path"])
            # Only task-owned trade storage is eligible for later deletion.
            path.resolve().relative_to(
                (config.data_dir / "trades" / str(day)).resolve()
            )
            target = bundle / "entries" / "trades" / instrument / "trades.parquet"
            _atomic(target, lambda temp, path=path: shutil.copyfile(path, temp))
            trade_entries.append(_record(target, bundle, deadline))
            inputs.append(str(path.relative_to(config.data_dir)))
            inputs.extend(
                str(p.relative_to(config.data_dir))
                for p in sorted((path.parent / "pages").glob("*.json"))
            )
        evidence = bundle / "entries" / "trades" / "coverage.json"
        write_json_atomic(evidence, report)
        trade_entries.append(_record(evidence, bundle, deadline))
        depth_assets = _parts(
            config, db, day, revision, "depth", depth_entries, bundle, cap, deadline
        )
        trade_assets = _parts(
            config, db, day, revision, "trades", trade_entries, bundle, cap, deadline
        )
        ohlcv_entries = copy.deepcopy(request.get("ohlcv_entries", []))
        ohlcv_assets = []
        if "ohlcv_report" in request:
            if not all(_valid(entry, bundle, deadline) for entry in ohlcv_entries):
                raise ValueError("frozen OHLCV snapshot mismatch")
            evidence = bundle / "entries/ohlcv/coverage.json"
            write_json_atomic(evidence, request["ohlcv_report"])
            ohlcv_entries.append(_record(evidence, bundle, deadline))
            ohlcv_assets = _parts(
                config, db, day, revision, "ohlcv", ohlcv_entries, bundle, cap, deadline
            )
        prior = db.execute(
            "SELECT manifest_json FROM revisions WHERE day=? AND revision<? ORDER BY revision DESC LIMIT 1",
            (str(day), revision),
        ).fetchone()
        manifest = (
            json.loads(prior[0])
            if prior
            else {
                "schema_version": 1,
                "day": str(day),
                "depth": {"assets": [], "segments": [], "segment_records": []},
                "trades": {"assets": [], "instruments": {}},
            }
        )
        if "ohlcv_report" in request:
            section = manifest.setdefault("ohlcv", {"assets": [], "files": []})
            section["assets"].extend(ohlcv_assets)
            section["files"].append({"revision": revision, "entries": ohlcv_entries})
            section["latest_snapshot"] = request["ohlcv_report"]
            section["snapshot_revision"] = revision
            section["status"] = (
                "api-exhausted" if request["ohlcv_report"]["complete"] else "partial"
            )
            section["coverage_clock"] = (
                "snapshot_at_ns across historical opens; independent of tick day"
            )
        manifest["revision"] = revision
        manifest["requested_interval_ns"] = list(window_bounds(day))
        manifest["partition_clocks"] = {
            "depth": "received_ns",
            "trades": "exchange event_time",
        }
        manifest["datasets"] = request.get("datasets", ["trades", "depth"])
        if depth_enabled:
            manifest["depth"]["assets"].extend(depth_assets)
            manifest["depth"]["segments"].extend(s["identity"] for s in segments)
            manifest["depth"]["segment_records"].extend(segments)
            manifest["depth"]["status"] = (
                "partial" if manifest["depth"]["segments"] else "unavailable"
            )
            manifest["depth"]["coverage_note"] = (
                "Only captured segments; uncaptured and uncertain intervals are unavailable. No historical depth backfill."
            )
            manifest["depth"]["rate_ms"] = config.depth_rate_ms
            coverage = manifest["depth"].setdefault("instruments", {})
            for item in discovery["instruments"]:
                coverage.setdefault(
                    item["instrument"], {"status": "unavailable", "segments": []}
                )
            for segment in segments:
                for instrument, state in segment["instruments"].items():
                    current = coverage.setdefault(
                        instrument, {"status": "partial", "segments": []}
                    )
                    current["segments"].append(
                        {"identity": segment["identity"], **state}
                    )
                    # No union of partial/unavailable/uncertain intervals proves full
                    # day coverage; uncertainties remain visible in each segment.
                    current["status"] = (
                        "uncertain"
                        if any(s["status"] == "uncertain" for s in current["segments"])
                        else "partial"
                    )
            manifest["depth"].setdefault("recovery_journals", []).extend(
                recovery_journals
            )
            manifest["depth"].setdefault("files", []).append(
                {"revision": revision, "entries": depth_entries}
            )
        elif not prior:
            manifest["depth"]["status"] = "disabled"
        manifest["trades"].setdefault("files", []).append(
            {"revision": revision, "entries": trade_entries}
        )
        for instrument in manifest["trades"]["instruments"]:
            instrument_states.setdefault(
                instrument,
                {"complete": False, "error": "missing prior-day instrument report"},
            )
        # Old tick bytes remain immutable. Supersession is per instrument so a
        # failed new download cannot erase a previously successful instrument.
        for instrument, state in instrument_states.items():
            if state.get("complete"):
                for old in manifest["trades"]["assets"]:
                    for ownership in old.get("instrument_coverage", []):
                        if (
                            ownership["instrument"] == instrument
                            and "superseded_by" not in ownership
                        ):
                            ownership["superseded_by"] = revision
            manifest["trades"]["instruments"][instrument] = state | {
                "revision": revision
            }
        for asset in trade_assets:
            asset["instrument_coverage"] = [
                {"instrument": name, "revision": revision}
                for name, state in sorted(instrument_states.items())
                if state.get("complete")
            ]
        manifest["trades"]["assets"].extend(trade_assets)
        full = (
            not request["allow_today"]
            and report.get("complete") is True
            and not report.get("failures")
            and not report.get("provisional")
            and all(
                s.get("complete")
                and s.get("pagination_exhausted")
                and (s.get("start_ns"), s.get("end_ns")) == window_bounds(day)
                for s in instrument_states.values()
            )
        )
        manifest["trades"].update(
            full_day_complete=bool(full),
            provisional=not bool(full),
            latest_report=report,
            instruments=instrument_states,
            cutoff_note=(
                "receive-clock approximation of exchange bound; full-day re-download required"
                if depth_enabled
                else "explicit exchange-time bound; full-day re-download required"
            )
            if request["allow_today"]
            else "exchange-time UTC day",
        )
        manifest["activation_receipt"] = request["receipt"]
        manifest["writer"] = {
            "pyarrow": pa.__version__,
            "compression": "zstd",
            "gzip_mtime": 0,
            "gzip_os": 255,
        }
        manifest["inputs"] = {
            "segments": [s["identity"] for s in segments],
            "trades": inputs,
        }
        # Must be durable BEFORE the revision could become public.
        _retry(config, day, not full)
        path = bundle / f"manifest-rev{revision:02d}.json"
        write_json_atomic(path, manifest)
        manifest_asset = _record(path, bundle, deadline) | {
            "name": path.name,
            "dataset": "manifest",
            "revision": revision,
        }
        if manifest_asset["size"] >= 1024**3:
            raise ValueError("cumulative manifest exceeds 1 GiB; fail closed")
        journal = {
            "day": str(day),
            "revision": revision,
            "assets": depth_assets + trade_assets + ohlcv_assets + [manifest_asset],
        }
        write_json_atomic(journal_path, journal)
        _finish_seal(db, day, revision, manifest, journal)
        return bundle
    finally:
        db.close()


def _finish_seal(db, day, revision, manifest, journal):
    with db:
        for asset in journal["assets"]:
            db.execute(
                "INSERT OR IGNORE INTO parts(day,revision,name,record_json) VALUES (?,?,?,?)",
                (str(day), revision, asset["name"], json.dumps(asset, sort_keys=True)),
            )
        db.execute(
            "UPDATE revisions SET status='sealed',manifest_json=?,journal_json=? WHERE day=? AND revision=?",
            (
                json.dumps(manifest, sort_keys=True),
                json.dumps(journal, sort_keys=True),
                str(day),
                revision,
            ),
        )
