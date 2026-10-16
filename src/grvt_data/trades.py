"""Daily trades: atomic page ledger first, recoverable cursor second."""

import hashlib
import json
import re
import time
from datetime import UTC, date, datetime
from pathlib import Path

from .api import CursorRejected, GrvtAPI
from .config import Instrument
from .storage import TRADE_SCHEMA, write_json_atomic, write_parquet_atomic

NS = 10**9


def window_bounds(day: date) -> tuple[int, int]:
    start = (day - date(1970, 1, 1)).days * 86400 * NS
    return start, start + 86400 * NS


def day_universe(data_dir: Path, day: date, discovered: list[Instrument]) -> list[str]:
    """Read Task 2's state/discovery/YYYY-MM-DD.json; never write discovery here.

    Record contract: {"instruments": [<full instrument metadata dict>, ...]}.
    Invalid existing records raise instead of silently dropping delisted markets.
    """
    path = data_dir / "state" / "discovery" / f"{day.isoformat()}.json"
    retained = json.loads(path.read_text())["instruments"] if path.exists() else []
    names = [item["instrument"] for item in [*retained, *discovered]]
    if any(not isinstance(name, str) or not name for name in names):
        raise ValueError("invalid discovery instrument")
    return sorted(set(names))


def _trade(row: dict, instrument: str) -> dict:
    if row.get("instrument") != instrument:
        raise ValueError("trade instrument differs from requested instrument")
    result = {}
    for field in TRADE_SCHEMA:
        value = row.get(field.name)
        if value is None and field.nullable:
            result[field.name] = None
        elif field.name == "event_time":
            if not isinstance(value, str) or not value.isdecimal():
                raise ValueError("event_time must be an integer nanosecond string")
            result[field.name] = int(value)
            if not 0 <= int(value) <= 2**63 - 1:
                raise ValueError("event_time outside int64")
        else:
            expected = bool if field.name in ("is_taker_buyer", "is_rpi") else str
            if not isinstance(value, expected) or (expected is str and not value):
                raise ValueError(f"{field.name} must be {expected.__name__}")
            result[field.name] = value
    return result


def _check(api):
    if hasattr(api, "check_deadline"):
        api.check_deadline()


def _instrument(api, root, instrument, start, end):
    directory = root / instrument / f"{start}-{end}"
    pages = directory / "pages"
    progress_path = directory / "progress.json"
    state = {
        "instrument": instrument,
        "start_ns": start,
        "end_ns": end,
        "cursor": "",
        "pages": 0,
        "rows": 0,
        "seen_cursors": [],
        "page_fingerprints": [],
        "oldest_ns": None,
        "generation": 0,
        "deduplication": "instrument/trade_id from durable page rows",
        "pagination_exhausted": False,
        "complete": False,
        "api_bound_semantics": "start inclusive; end inclusive; local [start,end)",
        "path": str(directory / "trades.parquet"),
    }
    rows_by_id = {}
    envelopes = sorted(pages.glob("*.json"))
    # The envelope is the commit record. If a process died before progress.json
    # was replaced, its last complete page still advances the recovered cursor.
    for index, envelope_path in enumerate(envelopes):
        _check(api)
        if envelope_path.name != f"{index:010d}.json":
            raise ValueError("durable page ledger has a missing segment")
        envelope = json.loads(envelope_path.read_text())
        state = envelope["state"]
        if (state["instrument"], state["start_ns"], state["end_ns"]) != (
            instrument,
            start,
            end,
        ):
            raise ValueError("durable page query identity mismatch")
        for row in envelope["rows"]:
            rows_by_id.setdefault(row["trade_id"], row)
    sequence = len(envelopes)
    if state["pagination_exhausted"] and progress_path.exists():
        final = json.loads(progress_path.read_text())
        if final.get("complete") and Path(state["path"]).is_file():
            return final

    def persist(page_rows, request_cursor, kind="page"):
        nonlocal sequence
        envelope = {
            "kind": kind,
            "request_cursor": request_cursor,
            "state": state,
            "rows": page_rows,
        }
        write_json_atomic(pages / f"{sequence:010d}.json", envelope)
        sequence += 1
        write_json_atomic(progress_path, state)

    restarted = False
    try:
        while not state["pagination_exhausted"]:
            _check(api)
            request_cursor = state["cursor"]
            try:
                yielded = False
                for response in api.trade_pages(instrument, start, end, request_cursor):
                    yielded = True
                    _check(api)
                    following = response["next"]
                    if not isinstance(following, str):
                        raise TypeError("invalid pagination cursor")
                    if following and following in [
                        request_cursor,
                        *state["seen_cursors"],
                    ]:
                        raise ValueError("repeated pagination cursor")
                    validated = [_trade(row, instrument) for row in response["result"]]
                    fingerprint = hashlib.sha256(
                        json.dumps([row["trade_id"] for row in validated]).encode()
                    ).hexdigest()
                    if validated and (
                        fingerprint in state["page_fingerprints"]
                        or (
                            state["oldest_ns"] is not None
                            and max(row["event_time"] for row in validated)
                            > state["oldest_ns"]
                        )
                    ):
                        raise CursorRejected(
                            "cursor ignored or pagination moved backwards"
                        )
                    new_rows = []
                    for row in validated:
                        if (
                            start <= row["event_time"] < end
                            and row["trade_id"] not in rows_by_id
                        ):
                            rows_by_id[row["trade_id"]] = row
                            new_rows.append(row)
                    state = {
                        **state,
                        "cursor": following,
                        "pages": state["pages"] + 1,
                        "rows": len(rows_by_id),
                        "page_fingerprints": [*state["page_fingerprints"], fingerprint],
                        "oldest_ns": min(
                            (row["event_time"] for row in validated),
                            default=state["oldest_ns"],
                        ),
                        "seen_cursors": [*state["seen_cursors"], following]
                        if following
                        else state["seen_cursors"],
                        "pagination_exhausted": not following,
                        "error": None,
                    }
                    persist(new_rows, request_cursor)
                    request_cursor = following
                    if not following:
                        break
                if not yielded or not state["pagination_exhausted"]:
                    raise ValueError("pagination iterator ended before terminal cursor")
            except CursorRejected:
                if restarted or not state["cursor"]:
                    raise
                # Preserve durable rows and dedup identity across the fresh query.
                state = {
                    **state,
                    "cursor": "",
                    "seen_cursors": [],
                    "page_fingerprints": [],
                    "oldest_ns": None,
                    "generation": state["generation"] + 1,
                }
                persist([], request_cursor, "cursor_restart")
                restarted = True
        _check(api)
        rows = sorted(
            rows_by_id.values(), key=lambda row: (row["event_time"], row["trade_id"])
        )
        write_parquet_atomic(Path(state["path"]), rows, TRADE_SCHEMA)
        state = {**state, "complete": True, "error": None}
        write_json_atomic(progress_path, state)
    except Exception as exc:  # noqa: BLE001 — per-instrument failures must remain explicit
        # Re-read the ledger on the next attempt, including a page whose progress
        # replace failed. BaseException intentionally escapes simulated hard kills.
        durable = (
            json.loads(max(pages.glob("*.json")).read_text())["state"]
            if sequence
            else {
                **state,
                "pages": 0,
                "rows": 0,
                "cursor": "",
                "seen_cursors": [],
                "pagination_exhausted": False,
            }
        )
        state = {**durable, "complete": False, "error": f"{type(exc).__name__}: {exc}"}
        write_json_atomic(progress_path, state)
    return state


def download_trades(
    api: GrvtAPI,
    data_dir: Path,
    day: date,
    instruments: list[str],
    *,
    end_ns: int | None = None,
) -> dict:
    data_dir = Path(data_dir).resolve()
    start, full_end = window_bounds(day)
    today = datetime.now(UTC).date()
    if day > today or (day == today and end_ns is None):
        raise ValueError("open day requires explicit activation end_ns")
    if end_ns is not None:
        if day != today or type(end_ns) is not int or not start < end_ns <= full_end:
            raise ValueError(
                "end_ns is valid only within the current UTC activation day"
            )
        if end_ns > time.time_ns():
            raise ValueError("activation end_ns cannot be in the future")
    end = full_end if end_ns is None else end_ns
    if not -(2**63) <= start < end <= 2**63 - 1:
        raise ValueError("window outside int64")
    if any(not re.fullmatch(r"[A-Za-z0-9_]+", name) for name in instruments):
        raise ValueError("invalid instrument name")
    result = {
        "day": day.isoformat(),
        "start_ns": start,
        "end_ns": end,
        "provisional": end_ns is not None,
        "instruments": {},
        "failures": {},
        "rows": 0,
    }
    for instrument in sorted(set(instruments)):
        _check(api)
        try:
            state = _instrument(
                api, data_dir / "trades" / day.isoformat(), instrument, start, end
            )
        except Exception as exc:  # noqa: BLE001 — retain explicit failures even for corrupt local state
            state = {
                "instrument": instrument,
                "start_ns": start,
                "end_ns": end,
                "pages": 0,
                "rows": 0,
                "complete": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        result["instruments"][instrument] = state
        result["rows"] += state["rows"]
        if not state["complete"]:
            result["failures"][instrument] = state["error"]
    result["complete"] = not result["failures"]
    return result
