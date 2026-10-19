"""Bounded, page-durable TRADE candle collection; gaps are never synthesized."""

import calendar
import hashlib
import json
import re
import time
from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .api import CursorRejected
from .config import remaining_seconds
from .storage import write_json_atomic, write_parquet_atomic
from .trades import window_bounds

NS = 10**9
DAY = 86400 * NS
# These are the native intervals shared with the Apex twelve-interval output.
INTERVALS = {
    "1m": ("CI_1_M", 60 * NS),
    "5m": ("CI_5_M", 300 * NS),
    "15m": ("CI_15_M", 900 * NS),
    "30m": ("CI_30_M", 1800 * NS),
    "1h": ("CI_1_H", 3600 * NS),
    "2h": ("CI_2_H", 7200 * NS),
    "4h": ("CI_4_H", 14400 * NS),
    "6h": ("CI_6_H", 21600 * NS),
    "12h": ("CI_12_H", 43200 * NS),
    "1d": ("CI_1_D", DAY),
    "1w": ("CI_1_W", 7 * DAY),
}
NUMBERS = ("open", "high", "low", "close", "volume_b", "volume_q")
SCHEMA = pa.schema(
    [
        *[
            pa.field(k, pa.string(), nullable=False)
            for k in ("instrument", "type", "interval")
        ],
        *[
            pa.field(k, pa.int64(), nullable=False)
            for k in ("open_time", "close_time", "query_started_at_ns")
        ],
        *[pa.field(k, pa.string(), nullable=False) for k in NUMBERS],
        pa.field("trades", pa.int64(), nullable=False),
        pa.field("provenance", pa.string(), nullable=False),
    ]
)


def _load(path, default):
    return json.loads(path.read_text()) if path.exists() else default


def _hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe(instrument):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", instrument):
        raise ValueError("unsafe instrument path")
    return instrument


def _month(ns):
    return datetime.fromtimestamp(ns // NS, UTC).strftime("%Y-%m")


def _candle(row, instrument, interval):
    if row.get("instrument") != instrument:
        raise ValueError("candle instrument mismatch")
    result = {
        "instrument": instrument,
        "type": "TRADE",
        "interval": interval,
        "provenance": "grvt:/full/v1/kline;schema=1",
        "query_started_at_ns": 0,
    }
    for key in ("open_time", "close_time"):
        value = row.get(key)
        if (
            not isinstance(value, str)
            or not value.isdecimal()
            or not 0 <= int(value) < 2**63
        ):
            raise ValueError(f"invalid candle {key}")
        result[key] = int(value)
    if result["close_time"] < result["open_time"]:
        raise ValueError("candle closes before open")
    for key in NUMBERS:
        value = row.get(key)
        if not isinstance(value, str) or not value or not Decimal(value).is_finite():
            raise ValueError(f"invalid candle {key}")
        result[key] = value
    trades = row.get("trades")
    if type(trades) is not int or not 0 <= trades < 2**63:
        raise ValueError("invalid candle trades")
    result["trades"] = trades
    return result


def _merge(root, rows):
    """One calendar partition in memory; newer durable observations win."""
    months = {}
    for row in rows:
        months.setdefault(_month(row["open_time"]), []).append(row)
    for month, batch in sorted(months.items()):
        path = root / f"{month}.parquet"
        old = pq.read_table(path).to_pylist() if path.exists() else []
        keyed = {r["open_time"]: r for r in old}
        for row in batch:
            previous = keyed.get(row["open_time"])
            if (
                previous is None
                or row["query_started_at_ns"] >= previous["query_started_at_ns"]
            ):
                keyed[row["open_time"]] = row
        write_parquet_atomic(path, [keyed[k] for k in sorted(keyed)], SCHEMA)


def _query(root, instrument, interval, start, end):
    # Bounds are immutable and identify each independently resumable query.
    directory = root / "state/ohlcv/queries" / interval / instrument / f"{start}-{end}"
    generation = _load(directory / "active.json", {"generation": 0})["generation"]
    directory = directory / f"generation-{generation:04d}"
    path = directory / "progress.json"
    initial = {
        "schema_version": 1,
        "key": ["instrument", "type", "interval", "open_time"],
        "instrument": instrument,
        "interval": interval,
        "type": "TRADE",
        "start_ns": start,
        "end_ns": end,
        "cursor": "",
        "pages": 0,
        "pagination_exhausted": False,
        "seen_cursors": [],
        "observed_min_ns": None,
        "observed_max_ns": None,
    }
    if not path.exists():
        write_json_atomic(path, initial)
    return directory, _load(path, initial)


def _page(api, root, directory, query):
    """Envelope -> canonical merge -> cursor. Replay the unacknowledged envelope."""
    path = directory / f"page-{query['pages']:08d}.json"
    if path.exists():
        envelope = _load(path, {})
    else:
        started = time.time_ns()
        response = api.candle_page(
            query["instrument"],
            INTERVALS[query["interval"]][0],
            query["start_ns"],
            query["end_ns"],
            query["cursor"],
        )
        envelope = {
            "schema_version": 1,
            "request": {
                k: query[k]
                for k in (
                    "instrument",
                    "interval",
                    "type",
                    "start_ns",
                    "end_ns",
                    "cursor",
                )
            },
            "query_started_at_ns": started,
            "response": response,
            "response_sha256": hashlib.sha256(
                json.dumps(response, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }
        # Validate everything before committing a malformed source page.
        _accepted(envelope, query)
        write_json_atomic(path, envelope)
    rows, following = _accepted(envelope, query)
    _merge(root / "ohlcv" / query["interval"] / query["instrument"], rows)
    updated = dict(query)
    if rows:
        low, high = min(r["open_time"] for r in rows), max(r["open_time"] for r in rows)
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
    updated.update(
        cursor=following,
        pages=query["pages"] + 1,
        pagination_exhausted=not following,
        seen_cursors=query["seen_cursors"] + ([following] if following else []),
    )
    write_json_atomic(directory / "progress.json", updated)
    return updated


def _accepted(envelope, query):
    expected = {
        k: query[k]
        for k in ("instrument", "interval", "type", "start_ns", "end_ns", "cursor")
    }
    if envelope["request"] != expected:
        raise ValueError("durable candle page request mismatch")
    response = envelope["response"]
    actual = hashlib.sha256(
        json.dumps(response, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if (
        actual != envelope["response_sha256"]
        or type(envelope["query_started_at_ns"]) is not int
    ):
        raise ValueError("durable candle page hash/provenance mismatch")
    following = response.get("next", "")
    if not isinstance(following, str) or (
        following
        and (following == query["cursor"] or following in query["seen_cursors"])
    ):
        raise ValueError("repeated or invalid candle cursor")
    if not isinstance(response.get("result"), list):
        raise TypeError("invalid candle result")
    rows = [
        _candle(r, query["instrument"], query["interval"]) for r in response["result"]
    ]
    for row in rows:
        row["query_started_at_ns"] = envelope["query_started_at_ns"]
    duration = INTERVALS[query["interval"]][1]
    # API close_time can be end+1ns; use interval's actual exclusive close.
    rows = [
        r
        for r in rows
        if query["start_ns"] <= r["open_time"] < query["end_ns"]
        and r["open_time"] + duration <= query["end_ns"]
    ]
    return rows, following


def _months(root, instruments, cutoff, deadline):
    coverage = {}
    for instrument in instruments:
        months = {}
        for path in sorted((root / "ohlcv/1d" / instrument).glob("*.parquet")):
            remaining_seconds(deadline)
            year, month = map(int, path.stem.split("-"))
            first = window_bounds(date(year, month, 1))[0]
            count = calendar.monthrange(year, month)[1]
            end = first + count * DAY
            rows = pq.read_table(path).to_pylist()
            keyed = {r["open_time"]: r for r in rows if r["open_time"] + DAY <= cutoff}
            required = [first + n * DAY for n in range(count)]
            full = end <= cutoff and all(n in keyed for n in required)
            months[path.stem] = {
                "status": "derived" if full else "unavailable",
                "expected_daily_bars": count,
                "observed_daily_bars": len(keyed),
                "source": str(path.relative_to(root)),
                "source_sha256": _hash(path),
            }
            if not full:
                continue
            daily = [keyed[n] for n in required]
            with localcontext() as ctx:
                ctx.prec = max(100, max(len(r[k]) for r in daily for k in NUMBERS) + 20)
                aggregate = dict(
                    daily[0],
                    interval="1mo",
                    close=daily[-1]["close"],
                    close_time=end,
                    query_started_at_ns=max(r["query_started_at_ns"] for r in daily),
                    high=max(daily, key=lambda r: Decimal(r["high"]))["high"],
                    low=min(daily, key=lambda r: Decimal(r["low"]))["low"],
                    trades=sum(r["trades"] for r in daily),
                    provenance="derived:UTC-calendar-month;closed-native-1d;all-days;schema=1",
                )
                for key in ("volume_b", "volume_q"):
                    aggregate[key] = format(sum(Decimal(r[key]) for r in daily), "f")
            _merge(root / "ohlcv/1mo" / instrument, [aggregate])
        coverage[instrument] = {
            "status": "derived-only-with-all-days",
            "months": months,
        }
    return coverage


def collect_ohlcv(
    api, root: Path, day: date, instruments: list[str], *, end_ns=None, deadline=None
):
    """Finite fair rounds. Return publishable partial data even on API errors."""
    config = api.config
    cutoff = min(window_bounds(day)[1], end_ns or window_bounds(day)[1], time.time_ns())
    budget = config.ohlcv_budget_seconds
    if deadline is not None:
        remaining = remaining_seconds(deadline, float("inf"))
        budget = min(budget, remaining - min(600, remaining / 3))
    until = time.monotonic() + budget
    recent_until = time.monotonic() + budget * 0.8
    names = sorted({_safe(i) for i in instruments})
    schedule_path = root / "state/ohlcv/schedule.json"
    schedule = _load(schedule_path, {"recent_offset": 0, "history_offset": 0})
    series = [(interval, instrument) for interval in INTERVALS for instrument in names]
    states = {}
    for interval, instrument in series:
        path = root / "state/ohlcv/series" / interval / f"{instrument}.json"
        state = _load(path, {})
        history_end = state.setdefault("history_end_ns", cutoff)
        # A new day queries newest candles first, retaining any unfinished lower
        # bound. Within the same frozen window the exact cursor resumes.
        recent = state.get("recent")
        if not recent or recent["end_ns"] < cutoff:
            previous_end = recent["end_ns"] if recent else cutoff
            start = max(
                0, min(previous_end, cutoff) - max(2 * DAY, 2 * INTERVALS[interval][1])
            )
            if recent and not recent["pagination_exhausted"]:
                start = min(start, recent["start_ns"])
            _, state["recent"] = _query(root, instrument, interval, start, cutoff)
        _, state["history"] = _query(root, instrument, interval, 0, history_end)
        # Query progress is authoritative after a crash between query and series writes.
        for mode in ("recent", "history"):
            q = state[mode]
            _, state[mode] = _query(
                root, instrument, interval, q["start_ns"], q["end_ns"]
            )
        state.pop("error", None)
        states[(interval, instrument)] = (path, state)
        write_json_atomic(path, state)
    fetched = {"recent": 0, "history": 0}
    for mode, page_budget, stop in (
        ("recent", config.ohlcv_recent_pages, recent_until),
        ("history", config.ohlcv_backfill_pages, until),
    ):
        offset_key = f"{mode}_offset"
        offset = schedule[offset_key] % max(1, len(series))
        order = series[offset:] + series[:offset]
        while order and fetched[mode] < page_budget and time.monotonic() < stop:
            next_round = []
            for key in order:
                if fetched[mode] >= page_budget or time.monotonic() >= stop:
                    break
                remaining_seconds(deadline)
                path, state = states[key]
                query = state[mode]
                if query["pagination_exhausted"] or state.get("error"):
                    continue
                directory, query = _query(
                    root, key[1], key[0], query["start_ns"], query["end_ns"]
                )
                original_deadline = getattr(api, "deadline", None)
                api.deadline = (
                    min(stop, original_deadline)
                    if original_deadline is not None
                    else stop
                )
                try:
                    state[mode] = _page(api, root, directory, query)
                except CursorRejected as exc:
                    # Never label rejected cursors exhausted or discard their evidence.
                    # Restart from the same bound with a distinct generation directory.
                    active = directory.parent / "active.json"
                    generation = _load(active, {"generation": 0})["generation"] + 1
                    write_json_atomic(active, {"generation": generation})
                    state["error"] = (
                        f"cursor rejected; next run restarts frozen query: {exc}"
                    )
                except Exception as exc:  # noqa: BLE001 — publish durable partial progress
                    state["error"] = f"{type(exc).__name__}: {exc}"
                finally:
                    api.deadline = original_deadline
                fetched[mode] += 1
                write_json_atomic(path, state)
                schedule[offset_key] = (series.index(key) + 1) % max(1, len(series))
                write_json_atomic(schedule_path, schedule)
                if not state[mode]["pagination_exhausted"] and not state.get("error"):
                    next_round.append(key)
            order = next_round
    report = {
        "schema_version": 1,
        "snapshot_at_ns": time.time_ns(),
        "cutoff_ns": cutoff,
        "source": "grvt:/full/v1/kline;type=TRADE",
        "historical_query_start_ns": 0,
        "coverage_note": "API pagination exhaustion is not proof of gap-free bars; missing bars unknown",
        "fetched_pages": fetched,
        "budgets": {
            "recent_pages": config.ohlcv_recent_pages,
            "history_pages": config.ohlcv_backfill_pages,
            "requested_seconds": config.ohlcv_budget_seconds,
            "allotted_seconds": budget,
        },
        "schedule": schedule,
        "failures": {},
        "intervals": {},
        "complete": True,
        "historical_pagination_exhausted": True,
        "recent_complete": True,
    }
    for interval in INTERVALS:
        report["intervals"][interval] = {}
        for instrument in names:
            _, state = states[(interval, instrument)]
            report["intervals"][interval][instrument] = state
            if state.get("error"):
                report["failures"][f"{interval}/{instrument}"] = state["error"]
            report["historical_pagination_exhausted"] &= state["history"][
                "pagination_exhausted"
            ]
            report["recent_complete"] &= (
                state["recent"]["pagination_exhausted"]
                and state["recent"]["end_ns"] >= cutoff
            )
            report["complete"] &= (
                state["history"]["pagination_exhausted"]
                and state["recent"]["pagination_exhausted"]
                and state["recent"]["end_ns"] >= cutoff
                and not state.get("error")
            )
    report["intervals"]["1mo"] = _months(root, names, cutoff, deadline)
    # Explicitly list every exact canonical/evidence byte to freeze into the revision.
    report["files"] = []
    for directory, pattern in (
        (root / "ohlcv", "*/*/*.parquet"),
        (root / "state/ohlcv/queries", "*/*/*/**/*.json"),
        (root / "state/ohlcv/series", "*/*.json"),
    ):
        for path in sorted(directory.glob(pattern)):
            remaining_seconds(deadline)
            report["files"].append(
                {
                    "path": str(path.relative_to(root)),
                    "sha256": _hash(path),
                    "size": path.stat().st_size,
                }
            )
    report["snapshot_at_ns"] = time.time_ns()
    write_json_atomic(root / "state/ohlcv/status.json", report)
    return report
