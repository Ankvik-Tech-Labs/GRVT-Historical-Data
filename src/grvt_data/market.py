"""All public non-depth data: observed snapshots and page-durable histories.

Canonical files retain string numerics and complete source records. Funding and
additional candle queries resume independently; an API terminal cursor certifies
only the returned pagination, never gap-free market history.
"""

import hashlib
import json
import re
import shutil
import time
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from .api import CursorRejected
from .config import DeadlineExceeded, remaining_seconds
from .ohlcv import DAY, INTERVALS, NS
from .storage import write_json_atomic, write_parquet_atomic
from .trades import window_bounds

AGGREGATIONS = ("FUNDING_INTERVAL", "ONE_HOURLY", "FOUR_HOURLY", "EIGHT_HOURLY")
ALL_INTERVALS = {
    **INTERVALS,
    "3m": ("CI_3_M", 180 * NS),
    "8h": ("CI_8_H", 28800 * NS),
    "3d": ("CI_3_D", 3 * DAY),
    "5d": ("CI_5_D", 5 * DAY),
    "2w": ("CI_2_W", 14 * DAY),
    "3w": ("CI_3_W", 21 * DAY),
    "4w": ("CI_4_W", 28 * DAY),
}
GROUPS = ("funding", "ticker", "metadata", "ohlcv_extra")
GLOBALS = (
    "all_instruments",
    "currency",
    "supported_assets",
    "get_all_collateral_asset_info",
)
SCHEMA = pa.schema(
    [
        pa.field("instrument", pa.string(), nullable=False),
        pa.field("series", pa.string(), nullable=False),
        pa.field("event_time_ns", pa.int64(), nullable=False),
        pa.field("observed_at_ns", pa.int64(), nullable=False),
        pa.field("record_json", pa.string(), nullable=False),
        *[
            pa.field(k, pa.string())
            for k in (
                "aggregation",
                "funding_rate",
                "mark_price",
                "funding_rate_8_h_avg",
                "price_type",
                "interval",
                "open",
                "high",
                "low",
                "close",
                "volume_b",
                "volume_q",
            )
        ],
        pa.field("funding_interval_hours", pa.int64()),
        pa.field("close_time_ns", pa.int64()),
        pa.field("trades", pa.int64()),
    ]
)


def _load(path, default):
    return json.loads(path.read_text()) if path.exists() else default


def _hash_object(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _file_hash(path, deadline=None):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024**2):
            remaining_seconds(deadline)
            h.update(block)
    return h.hexdigest()


def _safe(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("unsafe market data path")
    return value


def _disk_guard(root, config):
    if shutil.disk_usage(root).free < config.hard_stop_free_bytes:
        raise OSError("public market collection paused at hard disk threshold")


def _ns(value):
    if (
        not isinstance(value, str)
        or not value.isdecimal()
        or not 0 <= int(value) < 2**63
    ):
        raise ValueError("invalid source nanosecond timestamp")
    return int(value)


def _number(value):
    if not isinstance(value, str) or not Decimal(value).is_finite():
        raise ValueError("market numeric must remain a finite source string")
    return value


def _specs(names, funding_names=None):
    # Every native funding market is visited before the alternate aggregation views.
    funding_names = names if funding_names is None else funding_names
    funding = [
        {"dataset": "funding", "instrument": n, "aggregation": AGGREGATIONS[0]}
        for n in funding_names
    ]
    funding += [
        {"dataset": "funding", "instrument": n, "aggregation": a}
        for n in funding_names
        for a in AGGREGATIONS[1:]
    ]
    candles = [
        {"dataset": "ohlcv_extra", "instrument": n, "price_type": t, "interval": i}
        for n in names
        for t in ("TRADE", "MARK", "INDEX", "MID")
        for i in sorted(ALL_INTERVALS, key=lambda k: ALL_INTERVALS[k][1])
        if t != "TRADE" or i not in INTERVALS
    ]
    return {"funding": funding, "ohlcv_extra": candles}


def _key(spec):
    if spec["dataset"] == "funding":
        return f"{spec['aggregation']}/{spec['instrument']}"
    return f"{spec['price_type']}/{spec['interval']}/{spec['instrument']}"


def _query(start, end):
    return {
        "start_ns": start,
        "end_ns": end,
        "cursor": "",
        "pages": 0,
        "generation": 0,
        "exhausted": False,
        "seen": [],
        "observed_min_ns": None,
        "observed_max_ns": None,
    }


def _overlap(spec):
    return (
        2 * DAY
        if spec["dataset"] == "funding"
        else max(2 * DAY, 2 * ALL_INTERVALS[spec["interval"]][1])
    )


def _state(root, spec, cutoff):
    path = root / "state/market/series" / spec["dataset"] / (_key(spec) + ".json")
    state = _load(path, None)
    if state is None:
        state = {
            "spec": spec,
            "history": _query(0, cutoff),
            "recent": None,
            "covered_until_ns": 0,
            "overlap_policy_version": 1,
        }
        write_json_atomic(path, state)
    if state["spec"] != spec:
        raise ValueError("market series identity mismatch")
    if spec["dataset"] == "ohlcv_extra" and state.get("overlap_policy_version") != 1:
        # Repair from the initial history boundary, not the last recent cutoff:
        # intervening short windows may have permanently missed long bars.
        state["repair_start_ns"] = max(0, state["history"]["end_ns"] - _overlap(spec))
        state["recent"] = None
        state["covered_until_ns"] = 0
        state["overlap_policy_version"] = 1
        write_json_atomic(path, state)
    return path, state


def _request(spec, query):
    payload = {
        "instrument": spec["instrument"],
        "start_time": str(query["start_ns"]),
        "end_time": str(query["end_ns"]),
        "limit": 1000,
        "cursor": query["cursor"],
    }
    if spec["dataset"] == "funding":
        return "funding", payload | {"agg_type": spec["aggregation"]}
    return "kline", payload | {
        "type": spec["price_type"],
        "interval": ALL_INTERVALS[spec["interval"]][0],
    }


def _normalise(record, spec, observed):
    if not isinstance(record, dict) or record.get("instrument") != spec["instrument"]:
        raise ValueError("market history instrument mismatch")
    funding = spec["dataset"] == "funding"
    timestamp = _ns(record.get("funding_time" if funding else "open_time"))
    row = {
        "instrument": spec["instrument"],
        "series": _key(spec),
        "event_time_ns": timestamp,
        "observed_at_ns": observed,
        "record_json": json.dumps(record, sort_keys=True, separators=(",", ":")),
    }
    if funding:
        row["aggregation"] = spec["aggregation"]
        row["funding_rate"] = _number(record.get("funding_rate"))
        for name in ("mark_price", "funding_rate_8_h_avg"):
            if record.get(name) is not None:
                row[name] = _number(record[name])
        hours = record.get("funding_interval_hours")
        if hours is not None and (type(hours) is not int or not 0 <= hours < 2**63):
            raise ValueError("invalid funding interval")
        row["funding_interval_hours"] = hours
    else:
        row.update(
            price_type=spec["price_type"],
            interval=spec["interval"],
            close_time_ns=_ns(record.get("close_time")),
        )
        if row["close_time_ns"] < timestamp:
            raise ValueError("candle closes before open")
        for name in ("open", "high", "low", "close", "volume_b", "volume_q"):
            row[name] = _number(record.get(name))
        if type(record.get("trades")) is not int or not 0 <= record["trades"] < 2**63:
            raise ValueError("invalid candle trade count")
        row["trades"] = record["trades"]
    return row


def _merge(root, spec, rows):
    months = {}
    for row in rows:
        month = datetime.fromtimestamp(row["event_time_ns"] // NS, UTC).strftime(
            "%Y-%m"
        )
        months.setdefault(month, []).append(row)
    for month, batch in months.items():
        path = root / spec["dataset"] / _key(spec) / f"{month}.parquet"
        old = pq.read_table(path).to_pylist() if path.exists() else []
        keyed = {r["event_time_ns"]: r for r in old}
        for row in batch:
            before = keyed.get(row["event_time_ns"])
            if before is None or row["observed_at_ns"] >= before["observed_at_ns"]:
                keyed[row["event_time_ns"]] = row
        write_parquet_atomic(path, [keyed[t] for t in sorted(keyed)], SCHEMA)


def _advance(api, root, spec, query):
    route, payload = _request(spec, query)
    directory = (
        root
        / "state/market/pages"
        / spec["dataset"]
        / _key(spec)
        / f"{query['start_ns']}-{query['end_ns']}"
        / f"generation-{query['generation']:04d}"
    )
    path = directory / f"page-{query['pages']:08d}.json"
    envelope = _load(path, None)
    fetched = envelope is None
    if envelope is None:
        observed = time.time_ns()
        if route == "funding":
            response = api.funding_page(
                spec["instrument"],
                spec["aggregation"],
                query["start_ns"],
                query["end_ns"],
                query["cursor"],
            )
        else:
            response = api.candle_page(
                spec["instrument"],
                payload["interval"],
                query["start_ns"],
                query["end_ns"],
                query["cursor"],
                price_type=spec["price_type"],
            )
        envelope = {
            "request": {"route": route, "payload": payload},
            "observed_at_ns": observed,
            "response": response,
            "response_sha256": _hash_object(response),
        }
    if (
        envelope["request"] != {"route": route, "payload": payload}
        or _hash_object(envelope["response"]) != envelope["response_sha256"]
    ):
        raise ValueError("durable market page identity/hash mismatch")
    if type(envelope["observed_at_ns"]) is not int or not isinstance(
        envelope["response"].get("result"), list
    ):
        raise ValueError("invalid durable market page")
    following = envelope["response"].get("next", "")
    if not isinstance(following, str) or (
        following and (following == query["cursor"] or following in query["seen"])
    ):
        raise ValueError("repeated or invalid market history cursor")
    rows = [
        _normalise(r, spec, envelope["observed_at_ns"])
        for r in envelope["response"]["result"]
    ]
    rows = [
        r
        for r in rows
        if query["start_ns"] <= r["event_time_ns"] < query["end_ns"]
        and (spec["dataset"] == "funding" or r["close_time_ns"] <= query["end_ns"])
    ]
    # Source bytes become durable before canonical replacement and cursor advance.
    if fetched:
        write_json_atomic(path, envelope)
    _merge(root, spec, rows)
    updated = dict(
        query,
        cursor=following,
        pages=query["pages"] + 1,
        exhausted=not following,
        seen=query["seen"] + ([following] if following else []),
    )
    if rows:
        low, high = (
            min(r["event_time_ns"] for r in rows),
            max(r["event_time_ns"] for r in rows),
        )
        updated["observed_min_ns"] = (
            min(low, query["observed_min_ns"])
            if query["observed_min_ns"] is not None
            else low
        )
        updated["observed_max_ns"] = (
            max(high, query["observed_max_ns"])
            if query["observed_max_ns"] is not None
            else high
        )
    return updated, fetched


def _snapshot(api, root, group, day, route, instrument=None):
    path = root / group / "snapshots" / day / route / ((instrument or "all") + ".json")
    previous = _load(path, None)
    request = {"route": route, "instrument": instrument}
    if previous is not None:
        if (
            previous["request"] != request
            or _hash_object(previous["response"]) != previous["response_sha256"]
        ):
            raise ValueError("snapshot identity/hash mismatch")
        path.chmod(0o660)
        return path, False
    observed = time.time_ns()
    response = api.market_snapshot(route, instrument)
    result = response.get("result", response)
    if instrument and (
        not isinstance(result, dict) or result.get("instrument") != instrument
    ):
        raise ValueError("snapshot instrument mismatch")
    if route in ("all_instruments", "currency") and not isinstance(result, list):
        raise ValueError("invalid registry list")
    write_json_atomic(
        path,
        {
            "request": request,
            "observed_at_ns": observed,
            "coverage_clock": "current observation, not historical UTC-day data",
            "response": response,
            "response_sha256": _hash_object(response),
        },
    )
    path.chmod(0o660)
    return path, True


def _stage_ended(stop, deadline):
    now = time.monotonic()
    return now >= stop and (deadline is None or now < deadline)


def market_status(root):
    report = _load(
        root / "state/market/status.json", {"enabled": True, "status": "not started"}
    )
    files = report.pop("files", [])
    return report | {
        "retained_files": len(files),
        "retained_bytes": sum(f["size"] for f in files),
    }


def collect_market(api, root, day, names, *, end_ns=None, deadline=None):
    config = api.config
    root.mkdir(parents=True, exist_ok=True)
    _disk_guard(root, config)
    names = sorted({_safe(n) for n in names})
    cutoff = end_ns if end_ns is not None else window_bounds(day)[1]
    remaining = remaining_seconds(deadline, config.market_budget_seconds + 600)
    budget = min(
        config.market_budget_seconds, max(0, remaining - min(600, remaining / 3))
    )
    stop = time.monotonic() + budget
    snapshot_day = datetime.now(UTC).date().isoformat()
    failures, fetched, processed = {}, Counter(), Counter()
    plan = [("metadata", route, None) for route in GLOBALS]
    plan += [
        (group, route, n)
        for n in names
        for group, route in (
            ("metadata", "margin_rules"),
            ("ticker", "ticker"),
            ("ticker", "mini"),
        )
    ]
    snapshots = {"metadata": [], "ticker": []}
    original_deadline = getattr(api, "deadline", None)
    api.deadline = min(stop, deadline) if deadline is not None else stop
    try:
        for group, route, instrument in plan:
            if (
                time.monotonic() >= stop
                or sum(fetched.values()) >= config.market_page_budget
            ):
                break
            _disk_guard(root, config)
            try:
                path, requested = _snapshot(
                    api, root, group, snapshot_day, route, instrument
                )
                snapshots[group].append(path)
                fetched[group] += int(requested)
            except DeadlineExceeded:
                if not _stage_ended(stop, deadline):
                    raise
                break
            except Exception as exc:  # noqa: BLE001 -- preserve independent partial data
                failures[f"{route}/{instrument or 'all'}"] = (
                    f"{type(exc).__name__}: {exc}"
                )
        registry = (
            _load(
                root / "metadata/snapshots" / snapshot_day / "all_instruments/all.json",
                {},
            )
            .get("response", {})
            .get("result", [])
        )
        kinds = {item["instrument"]: item.get("kind", "PERPETUAL") for item in registry}
        funding_names = [
            n
            for n in names
            if kinds.get(n, "PERPETUAL") in ("PERPETUAL", "STABLE_PERP")
        ]
        series = _specs(names, funding_names)
        schedule_path = root / "state/market/schedule.json"
        schedule = _load(schedule_path, {})
        left = max(0, config.market_page_budget - sum(fetched.values()))
        funding_quota = min(left, max(len(names), config.market_page_budget // 3))
        for group, quota in (("funding", funding_quota), ("ohlcv_extra", None)):
            quota = (
                max(0, config.market_page_budget - sum(fetched.values()))
                if quota is None
                else quota
            )
            specs = series[group]
            offset = schedule.get(group, 0) % max(1, len(specs))
            order = list(range(offset, len(specs))) + list(range(offset))
            failed = set()
            work = 0
            while order and work < quota and time.monotonic() < stop:
                advanced = False
                for index in order:
                    if work >= quota or time.monotonic() >= stop:
                        break
                    _disk_guard(root, config)
                    spec = specs[index]
                    key = group + "/" + _key(spec)
                    if key in failed:
                        continue
                    path, state = _state(root, spec, cutoff)
                    overlap = _overlap(spec)
                    fresh_start = max(0, cutoff - overlap)
                    if state["covered_until_ns"] < cutoff:
                        if (
                            state["history"]["end_ns"] == cutoff
                            and "repair_start_ns" not in state
                        ):
                            mode = "history"
                        else:
                            mode = "recent"
                            current = state.get(mode)
                            if current is None or current["end_ns"] != cutoff:
                                start = state.get(
                                    "repair_start_ns",
                                    max(
                                        0,
                                        (
                                            state["covered_until_ns"]
                                            or state["history"]["end_ns"]
                                        )
                                        - overlap,
                                    ),
                                )
                                if current and not current["exhausted"]:
                                    start = min(start, current["start_ns"])
                                state[mode] = _query(start, cutoff)
                                write_json_atomic(path, state)
                    else:
                        mode = "history"
                    query = state[mode]
                    if query["exhausted"]:
                        continue
                    try:
                        updated, requested = _advance(api, root, spec, query)
                        state[mode] = updated
                        if (
                            mode == "recent"
                            and updated["exhausted"]
                            or (
                                mode == "history"
                                and updated["end_ns"] == cutoff
                                and (
                                    updated["exhausted"]
                                    or (
                                        updated["observed_min_ns"] is not None
                                        and updated["observed_min_ns"] <= fresh_start
                                    )
                                )
                            )
                        ):
                            state["covered_until_ns"] = cutoff
                            if mode == "recent":
                                state.pop("repair_start_ns", None)
                        write_json_atomic(path, state)
                        fetched[group] += int(requested)
                        processed[group] += 1
                    except DeadlineExceeded:
                        if not _stage_ended(stop, deadline):
                            raise
                        break
                    except CursorRejected as exc:
                        saved = _load(path, state)
                        q = saved[mode]
                        saved[mode] = _query(q["start_ns"], q["end_ns"]) | {
                            "generation": q["generation"] + 1
                        }
                        write_json_atomic(path, saved)
                        failures[key] = (
                            f"cursor rejected; new durable generation: {exc}"
                        )
                        failed.add(key)
                    except Exception as exc:  # noqa: BLE001 -- per-series retry boundary
                        failures[key] = f"{type(exc).__name__}: {exc}"
                        failed.add(key)
                    work += 1
                    advanced = True
                    schedule[group] = (index + 1) % len(specs)
                    write_json_atomic(schedule_path, schedule)
                if not advanced:
                    break
    finally:
        api.deadline = original_deadline
    report = {
        "schema_version": 1,
        "candle_overlap_policy_version": 1,
        "snapshot_at_ns": time.time_ns(),
        "snapshot_day": snapshot_day,
        "cutoff_ns": cutoff,
        "historical_query_start_ns": 0,
        "datasets": {},
        "files": [],
        "failures": failures,
        "requests": dict(fetched),
        "processed_pages": dict(processed),
        "budgets": {"seconds": budget, "pages": config.market_page_budget},
        "stage_time_limit_reached": time.monotonic() >= stop,
        "coverage_note": "Historical cursor exhaustion is not gap-free history; tickers/reference data are observed snapshots; no depth collection.",
    }
    for group in ("metadata", "ticker"):
        planned = sum(1 for g, _, _ in plan if g == group)
        done = 0
        for g, route, instrument in plan:
            if g != group:
                continue
            path = (
                root
                / g
                / "snapshots"
                / snapshot_day
                / route
                / ((instrument or "all") + ".json")
            )
            saved = _load(path, None)
            if saved is not None:
                if saved.get("request") == {
                    "route": route,
                    "instrument": instrument,
                } and _hash_object(saved.get("response")) == saved.get(
                    "response_sha256"
                ):
                    done += 1
                else:
                    failures[f"{route}/{instrument or 'all'}"] = (
                        "invalid durable snapshot"
                    )
        report["datasets"][group] = {
            "planned": planned,
            "completed": done,
            "complete": done == planned,
            "clock": "observed_at_ns",
            "snapshot_day": snapshot_day,
        }
    for group, specs in series.items():
        recent = historical = 0
        views = {}
        observed_min, observed_max = None, None
        for spec in specs:
            view = spec.get("aggregation", spec.get("price_type"))
            summary = views.setdefault(
                view, {"planned": 0, "recent_complete": 0, "historical_exhausted": 0}
            )
            summary["planned"] += 1
            path = root / "state/market/series" / group / (_key(spec) + ".json")
            state = _load(path, None)
            if state:
                fresh = (
                    state["covered_until_ns"] >= cutoff
                    and "repair_start_ns" not in state
                    and (group == "funding" or state.get("overlap_policy_version") == 1)
                )
                exhausted = state["history"]["exhausted"]
                recent += fresh
                historical += exhausted
                summary["recent_complete"] += fresh
                summary["historical_exhausted"] += exhausted
                for query in (state["history"], state.get("recent")):
                    if query and query["observed_min_ns"] is not None:
                        low, high = query["observed_min_ns"], query["observed_max_ns"]
                        observed_min = (
                            low if observed_min is None else min(low, observed_min)
                        )
                        observed_max = (
                            high if observed_max is None else max(high, observed_max)
                        )
        report["datasets"][group] = {
            "planned": len(specs),
            "recent_complete": recent,
            "historical_exhausted": historical,
            "complete": recent == historical == len(specs),
            "views": views,
            "observed_min_ns": observed_min,
            "observed_max_ns": observed_max,
            "clock": "funding_time"
            if group == "funding"
            else "closed candle open_time",
        }
    for group in GROUPS:
        roots = [root / group]
        if group in ("metadata", "ticker"):
            roots = [root / group / "snapshots" / snapshot_day]
        if group in ("funding", "ohlcv_extra"):
            roots += [
                root / "state/market/series" / group,
                root / "state/market/pages" / group,
            ]
        count = 0
        for directory in roots:
            for path in sorted(directory.rglob("*")):
                if not path.is_file() or path.name.startswith("."):
                    continue
                remaining_seconds(deadline)
                if path.is_relative_to(root / "state/market/pages"):
                    envelope = _load(path, {})
                    if _hash_object(envelope.get("response")) != envelope.get(
                        "response_sha256"
                    ):
                        raise ValueError("retained market page checksum mismatch")
                report["files"].append(
                    {
                        "dataset": group,
                        "path": str(path.relative_to(root)),
                        "size": path.stat().st_size,
                        "sha256": _file_hash(path, deadline),
                    }
                )
                count += path.suffix == ".parquet"
        report["datasets"][group]["canonical_parquet_files"] = count
    report["complete"] = not failures and all(
        d["complete"] for d in report["datasets"].values()
    )
    report["snapshot_at_ns"] = time.time_ns()
    write_json_atomic(root / "state/market/status.json", report)
    return report
