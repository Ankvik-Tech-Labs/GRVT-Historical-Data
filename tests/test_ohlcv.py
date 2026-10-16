"""Closed-bar bounds, durable resumability and immutable release integration."""

import hashlib
import io
import json
import tarfile
import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
from test_archive import DAY, committed, report
from test_publish import FakeGitHub

from grvt_data import archive, cli, ohlcv
from grvt_data.api import CursorRejected, GrvtAPI
from grvt_data.config import Config, DeadlineExceeded
from grvt_data.publish import prune_verified, publish_revision
from grvt_data.storage import write_json_atomic
from grvt_data.trades import window_bounds

START, END = window_bounds(DAY)
MINUTE = 60 * 10**9
INSTRUMENT = "BTC_USDT_Perp"


def settings(root, **kw):
    return Config(data_dir=root, datasets=("trades", "ohlcv"), **kw)


def candle(open_ns=START, **kw):
    return {
        "instrument": INSTRUMENT,
        "open_time": str(open_ns),
        "close_time": str(open_ns + MINUTE),
        "open": "10.00000000000000001",
        "high": "12.0",
        "low": "9.0",
        "close": "11.0",
        "volume_b": "0.0",
        "volume_q": "0.0",
        "trades": 0,
    } | kw


class API:
    def __init__(self, config, handler=None):
        self.config = config
        self.deadline = None
        self.calls = []
        self.handler = handler or (lambda *args: {"result": [], "next": ""})

    def candle_page(self, *args):
        self.calls.append(args)
        return self.handler(*args)


@pytest.fixture
def minute_only(monkeypatch):
    monkeypatch.setattr(ohlcv, "INTERVALS", {"1m": ("CI_1_M", MINUTE)})


def read(root, interval="1m", month="2026-10"):
    return pq.read_table(
        root / "ohlcv" / interval / INSTRUMENT / f"{month}.parquet"
    ).to_pylist()


def collect(api, root, **kw):
    return ohlcv.collect_ohlcv(api, root, DAY, [INSTRUMENT], **kw)


def test_live_fixture_filters_inclusive_end_preserves_zero_and_strings(
    tmp_path, minute_only
):
    fixture = json.loads(
        (
            Path(__file__).parent / "fixtures/ohlcv/grvt-kline-live-2026-10-16.json"
        ).read_text()
    )
    start, end = (
        int(fixture["request"]["start_time"]),
        int(fixture["request"]["end_time"]),
    )
    directory, query = ohlcv._query(tmp_path, INSTRUMENT, "1m", start, end)
    api = API(settings(tmp_path), lambda *a: fixture["response"])
    state = ohlcv._page(api, tmp_path, directory, query)
    rows = read(tmp_path)
    assert state["pagination_exhausted"]
    assert len(rows) == 5 and max(r["open_time"] for r in rows) < end
    assert any(r["volume_b"] == "0.0" and r["trades"] == 0 for r in rows)
    assert all(isinstance(r["open_time"], int) for r in rows)
    assert (
        tmp_path / "ohlcv/1m" / INSTRUMENT / "2026-10.parquet"
    ).stat().st_mode & 0o777 == 0o660


def test_reverse_pages_zero_gaps_no_open_bar_and_real_provenance(tmp_path, minute_only):
    def handler(instrument, interval, start, end, cursor):
        if not cursor:
            return {
                "result": [
                    candle(END),
                    candle(END - MINUTE, close_time=str(END + 1)),
                    candle(END - 3 * MINUTE),
                ],
                "next": "older",
            }
        return {"result": [candle(START)], "next": ""}

    api = API(settings(tmp_path), handler)
    result = collect(api, tmp_path)
    rows = read(tmp_path)
    assert [r["open_time"] for r in rows] == [START, END - 3 * MINUTE, END - MINUTE]
    assert rows[-1]["close_time"] == END + 1
    assert result["complete"] is True
    assert any(
        call[2] == 0 for call in api.calls
    )  # metadata create_time never bounds history
    pages = list((tmp_path / "state/ohlcv/queries").rglob("page-*.json"))
    assert len(pages) == 4
    assert all(json.loads(p.read_text())["query_started_at_ns"] > 0 for p in pages)
    assert result["intervals"]["1mo"][INSTRUMENT]["months"] == {}


def test_budget_partial_resumes_actual_cursor_and_rotates_series(tmp_path, minute_only):
    config = settings(tmp_path, ohlcv_recent_pages=1, ohlcv_backfill_pages=1)
    api = API(
        config, lambda *a: {"result": [candle()], "next": "a" if not a[-1] else ""}
    )
    first = ohlcv.collect_ohlcv(api, tmp_path, DAY, [INSTRUMENT, "ZZ_RETAINED"])
    # Different instruments must return their own rows.
    assert not first["complete"]
    assert len(api.calls) == 2
    api.handler = lambda *a: {"result": [], "next": ""}
    second = ohlcv.collect_ohlcv(api, tmp_path, DAY, [INSTRUMENT, "ZZ_RETAINED"])
    assert api.calls[2][0] == "ZZ_RETAINED" and api.calls[3][0] == "ZZ_RETAINED"
    third = ohlcv.collect_ohlcv(api, tmp_path, DAY, [INSTRUMENT, "ZZ_RETAINED"])
    assert any(call[-1] == "a" for call in api.calls[4:])
    assert third["complete"] and second["complete"] is False


def test_page_commit_before_canonical_failure_replays_without_refetch(
    tmp_path, minute_only, monkeypatch
):
    api = API(settings(tmp_path), lambda *a: {"result": [candle()], "next": ""})
    original = ohlcv._merge
    monkeypatch.setattr(
        ohlcv, "_merge", lambda *a: (_ for _ in ()).throw(OSError("disk write"))
    )
    first = collect(api, tmp_path)
    assert not first["complete"] and len(api.calls) == 1
    monkeypatch.setattr(ohlcv, "_merge", original)
    second = collect(api, tmp_path)
    assert (
        second["complete"] and len(api.calls) == 2
    )  # only history fetch; recent durable replay
    assert len(read(tmp_path)) == 1


def test_repeated_cursor_does_not_certify_or_advance(tmp_path, minute_only):
    config = settings(tmp_path, ohlcv_recent_pages=2, ohlcv_backfill_pages=1)
    api = API(config, lambda *a: {"result": [candle()], "next": "same"})
    result = collect(api, tmp_path)
    state = result["intervals"]["1m"][INSTRUMENT]
    assert not result["complete"] and "repeated" in state["error"]
    assert state["recent"]["pages"] == 1
    assert len(list((tmp_path / "state/ohlcv/queries").rglob("page-*.json"))) == 1


def test_cursor_rejection_restarts_generation_keeps_prior_evidence(
    tmp_path, minute_only
):
    config = settings(tmp_path, ohlcv_recent_pages=1, ohlcv_backfill_pages=1)

    def handler(*args):
        if args[-1]:
            raise CursorRejected("expired")
        return {"result": [candle()], "next": "old"}

    api = API(config, handler)
    collect(api, tmp_path)
    failed = collect(api, tmp_path)
    assert not failed["complete"]
    api.handler = lambda *a: {"result": [candle(close="12.5")], "next": ""}
    done = collect(api, tmp_path)
    assert done["complete"] and all(r["close"] == "12.5" for r in read(tmp_path))
    assert list((tmp_path / "state/ohlcv/queries").rglob("generation-0000/page-*.json"))
    assert list((tmp_path / "state/ohlcv/queries").rglob("generation-0001/page-*.json"))


def test_next_day_forward_overlap_replaces_key_and_preserves_history(
    tmp_path, minute_only
):
    api = API(settings(tmp_path), lambda *a: {"result": [candle()], "next": ""})
    collect(api, tmp_path)
    api.handler = lambda *a: {
        "result": [candle(close="99.000000000000000001")],
        "next": "",
    }
    later = ohlcv.collect_ohlcv(api, tmp_path, DAY + timedelta(days=1), [INSTRUMENT])
    assert (
        len(read(tmp_path)) == 1
        and read(tmp_path)[0]["close"] == "99.000000000000000001"
    )
    assert later["complete"]
    assert api.calls[-1][2] <= START and api.calls[-1][3] == END + ohlcv.DAY
    assert len(api.calls) == 3  # exhausted historical query retained, new recent only


@pytest.mark.parametrize(
    "month,days", [("2024-02", 29), ("2025-02", 28), ("2026-09", 30), ("2026-10", 31)]
)
def test_true_calendar_month_requires_every_closed_daily_bar(tmp_path, month, days):
    first = window_bounds(date.fromisoformat(month + "-01"))[0]
    rows = [
        ohlcv._candle(
            candle(
                first + n * ohlcv.DAY,
                close_time=str(first + (n + 1) * ohlcv.DAY),
                volume_b="0.1",
                trades=1,
            ),
            INSTRUMENT,
            "1d",
        )
        for n in range(days)
    ]
    ohlcv._merge(tmp_path / "ohlcv/1d" / INSTRUMENT, rows[:-1])
    missing = ohlcv._months(tmp_path, [INSTRUMENT], first + days * ohlcv.DAY, None)
    assert missing[INSTRUMENT]["months"][month]["status"] == "unavailable"
    assert not list((tmp_path / "ohlcv/1mo").rglob("*.parquet"))
    ohlcv._merge(tmp_path / "ohlcv/1d" / INSTRUMENT, rows[-1:])
    open_month = ohlcv._months(
        tmp_path, [INSTRUMENT], first + (days - 1) * ohlcv.DAY, None
    )
    assert open_month[INSTRUMENT]["months"][month]["status"] == "unavailable"
    full = ohlcv._months(tmp_path, [INSTRUMENT], first + days * ohlcv.DAY, None)
    monthly = read(tmp_path, "1mo", month)[0]
    assert full[INSTRUMENT]["months"][month]["status"] == "derived"
    assert monthly["close_time"] - monthly["open_time"] == days * ohlcv.DAY
    assert Decimal(monthly["volume_b"]) == Decimal(days) / 10
    assert monthly["trades"] == days and "all-days" in monthly["provenance"]


# Decimal imported here to keep calendar assertions exact.
from decimal import Decimal


def test_empty_actual_exhaustion_differs_from_error(tmp_path, minute_only):
    api = API(settings(tmp_path))
    done = collect(api, tmp_path)
    assert done["complete"]
    assert done["intervals"]["1m"][INSTRUMENT]["history"]["observed_min_ns"] is None
    other = tmp_path / "failed"

    def fail(*args):
        raise RuntimeError("upstream unavailable")

    api.handler = fail
    failed = collect(api, other)
    assert (
        not failed["complete"]
        and "unavailable" in failed["intervals"]["1m"][INSTRUMENT]["error"]
    )


def test_every_interval_visible_even_before_first_page(tmp_path, monkeypatch):
    api = API(settings(tmp_path, ohlcv_recent_pages=1, ohlcv_backfill_pages=1))
    result = collect(api, tmp_path)
    assert set(result["intervals"]) == {*ohlcv.INTERVALS, "1mo"}
    assert len(result["intervals"]) == 12 and not result["complete"]


def test_expired_worker_deadline_never_claims_completion(tmp_path, minute_only):
    with pytest.raises(DeadlineExceeded):
        collect(API(settings(tmp_path)), tmp_path, deadline=time.monotonic() - 1)


def test_stage_deadline_is_passed_to_network_then_restored(tmp_path, minute_only):
    api = API(settings(tmp_path, ohlcv_budget_seconds=10))
    original = time.monotonic() + 100
    api.deadline = original
    seen = []
    api.handler = lambda *a: seen.append(api.deadline) or {"result": [], "next": ""}
    collect(api, tmp_path, deadline=original)
    assert len(seen) == 2 and all(time.monotonic() < d < original for d in seen)
    assert api.deadline == original


def test_api_payload_retries_and_optional_empty_next(tmp_path):
    calls = []

    def transport(request):
        calls.append(json.loads(request.content))
        if len(calls) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"result": []})

    api = GrvtAPI(
        settings(tmp_path),
        client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    api._sleep = lambda *a: None
    try:
        assert api.candle_page(INSTRUMENT, "CI_1_M", 0, END, "cursor")["next"] == ""
    finally:
        api.close()
    assert len(calls) == 2
    assert calls[0] == {
        "instrument": INSTRUMENT,
        "interval": "CI_1_M",
        "type": "TRADE",
        "start_time": "0",
        "end_time": str(END),
        "limit": 1000,
        "cursor": "cursor",
    }


def test_frozen_snapshot_survives_interrupted_seal_and_later_correction(
    tmp_path, minute_only, monkeypatch
):
    api = API(settings(tmp_path), lambda *a: {"result": [candle()], "next": ""})
    snapshot = collect(api, tmp_path)
    original = archive.resume_revision
    monkeypatch.setattr(
        archive,
        "resume_revision",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("crash before pack")),
    )
    with pytest.raises(RuntimeError):
        archive.seal_revision(api.config, DAY, report(), ohlcv_report=snapshot)
    frozen = (
        archive.bundle_path(api.config, DAY, 1)
        / "entries/ohlcv/ohlcv/1m"
        / INSTRUMENT
        / "2026-10.parquet"
    )
    old_hash = archive.sha256(frozen)
    api.handler = lambda *a: {"result": [candle(close="99")], "next": ""}
    ohlcv.collect_ohlcv(api, tmp_path, DAY + timedelta(days=1), [INSTRUMENT])
    assert (
        archive.sha256(tmp_path / "ohlcv/1m" / INSTRUMENT / "2026-10.parquet")
        != old_hash
    )
    monkeypatch.setattr(archive, "resume_revision", original)
    bundle = original(replace(api.config, datasets=("trades",)), DAY, 1)
    assert archive.sha256(frozen) == old_hash
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    assert (
        manifest["ohlcv"]["latest_snapshot"]["snapshot_at_ns"]
        == snapshot["snapshot_at_ns"]
    )
    assert any(
        e["sha256"] == old_hash for e in manifest["ohlcv"]["files"][0]["entries"]
    )


def test_snapshot_shards_remote_hashes_cumulative_refs_and_retention(
    tmp_path, minute_only
):
    config = settings(tmp_path, archive_part_bytes=4096)
    api = API(config, lambda *a: {"result": [candle()], "next": ""})
    snapshot = collect(api, tmp_path)
    bundle = archive.seal_revision(config, DAY, report(), ohlcv_report=snapshot)
    first = json.loads((bundle / "manifest-rev01.json").read_text())
    assets = first["ohlcv"]["assets"]
    assert len(assets) > 1 and all(a["size"] <= 4096 for a in assets)
    gh = FakeGitHub(config)
    publish_revision(config, DAY, 1, github=gh)
    assert "OHLCV:" in gh.state["body"]
    for asset in assets:
        remote = next(a for a in gh.state["assets"] if a["name"] == asset["name"])
        assert hashlib.sha256(gh.bytes[remote["id"]]).hexdigest() == asset["sha256"]
    canonical = tmp_path / "ohlcv/1m" / INSTRUMENT / "2026-10.parquet"
    restored = {}
    for asset in assets:
        remote = next(a for a in gh.state["assets"] if a["name"] == asset["name"])
        with tarfile.open(
            fileobj=io.BytesIO(gh.bytes[remote["id"]]), mode="r:gz"
        ) as tar:
            for member in tar.getmembers():
                restored[member.name] = tar.extractfile(member).read()
    entry = next(
        e
        for e in first["ohlcv"]["files"][0]["entries"]
        if e["path"].endswith(".parquet")
    )
    pieces = entry.get("chunks", [entry["path"]])
    canonical_bytes = b"".join(restored[p] for p in pieces)
    assert canonical_bytes == canonical.read_bytes()
    assert hashlib.sha256(canonical_bytes).hexdigest() == entry["sha256"]
    raw = list((tmp_path / "state/ohlcv/queries").rglob("page-*.json"))
    prune_verified(config, datetime.now(UTC), github=gh)
    assert canonical.exists() and all(p.exists() for p in raw)
    second = archive.seal_revision(config, DAY, report(), ohlcv_report=snapshot)
    latest = json.loads((second / "manifest-rev02.json").read_text())
    assert {a["name"] for a in assets} <= {a["name"] for a in latest["ohlcv"]["assets"]}
    publish_revision(config, DAY, 2, github=gh)
    assert "Active revision: 2" in gh.state["body"]


def test_partial_ohlcv_requeues_yesterday_after_terminal_ticks(tmp_path, minute_only):
    config = settings(tmp_path, ohlcv_recent_pages=1, ohlcv_backfill_pages=1)
    api = API(config, lambda *a: {"result": [], "next": "more"})
    snapshot = collect(api, tmp_path)
    assert not snapshot["complete"]
    archive.seal_revision(config, DAY, report(), ohlcv_report=snapshot)
    committed(config)
    assert DAY in archive.pending_dates(config, DAY + timedelta(days=1))
    assert DAY not in archive.pending_dates(
        replace(config, datasets=("trades",)), DAY + timedelta(days=1)
    )


def test_default_cli_combined_explicit_legacy_and_budgets(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(cli, "daily", lambda c, **kw: seen.append(c) or 0)
    cli.main(["daily", "--ohlcv-backfill-pages", "3", "--ohlcv-budget-seconds", "9"])
    assert seen[-1].datasets == ("trades", "ohlcv")
    assert seen[-1].ohlcv_backfill_pages == 3 and seen[-1].ohlcv_budget_seconds == 9
    cli.main(["daily", "--datasets", "trades"])
    assert seen[-1].datasets == ("trades",)
    assert Config().datasets == ("trades", "depth")


def test_large_universe_rotates_recent_and_history_without_starvation(tmp_path):
    names = [f"MARKET_{n:03d}" for n in range(184)]
    config = settings(tmp_path, ohlcv_recent_pages=13, ohlcv_backfill_pages=7)
    api = API(config, lambda *a: {"result": [], "next": "older"})
    first = ohlcv.collect_ohlcv(api, tmp_path, DAY, names)
    before = list(api.calls)
    assert len(before) == 20 and not first["complete"]
    second = ohlcv.collect_ohlcv(api, tmp_path, DAY, names)
    recent_first = {(a[0], a[1]) for a in before if a[2] != 0}
    recent_next = {(a[0], a[1]) for a in api.calls[20:] if a[2] != 0}
    history_first = {(a[0], a[1]) for a in before if a[2] == 0}
    history_next = {(a[0], a[1]) for a in api.calls[20:] if a[2] == 0}
    assert not recent_first & recent_next and not history_first & history_next
    assert len(first["intervals"]["1m"]) == 184
    assert sum(len(first["intervals"][i]) for i in ohlcv.INTERVALS) == 2024
    assert not second["complete"]


def test_ohlcv_worker_reuses_pruned_terminal_ticks_under_publication_lock(
    tmp_path, minute_only, monkeypatch
):
    from test_archive import trade_report

    config = settings(tmp_path, ohlcv_recent_pages=1, ohlcv_backfill_pages=1)
    api = API(config, lambda *a: {"result": [], "next": "more"})
    snapshot = collect(api, tmp_path)
    ticks = trade_report(config)
    archive.seal_revision(config, DAY, ticks, ohlcv_report=snapshot)
    gh = FakeGitHub(config)
    publish_revision(config, DAY, 1, github=gh)
    prune_verified(config, datetime.now(UTC), github=gh)
    assert not list((tmp_path / "trades").rglob("pages/*.json"))
    monkeypatch.setattr(cli, "utc_today", lambda: DAY + timedelta(days=1))
    api.instruments = lambda **kw: [
        {"instrument": INSTRUMENT, "create_time": str(END + 100)}
    ]
    api.close = lambda: None
    api.handler = lambda *a: {"result": [], "next": ""}
    monkeypatch.setattr(cli, "GrvtAPI", lambda *a, **kw: api)
    monkeypatch.setattr(cli, "ObservedGitHub", lambda *a, **kw: gh)
    monkeypatch.setattr(
        cli,
        "download_trades",
        lambda *a, **kw: pytest.fail("completed ticks re-downloaded"),
    )
    observed = []
    real_collect = cli.collect_ohlcv

    def locked(*args, **kw):
        import os

        fd = int(os.environ["GRVT_PUBLICATION_LOCK_FD"])
        os.fstat(fd)
        observed.append(fd)
        return real_collect(*args, **kw)

    monkeypatch.setattr(cli, "collect_ohlcv", locked)
    with cli.publication_lock(config, 0):
        assert cli.run_date(config, DAY, deadline=time.monotonic() + 60) == 0
    assert observed and "Active revision: 2" in gh.state["body"]
    assert (
        json.loads(cli.revisions(config, DAY)[-1]["manifest_json"])["ohlcv"]["status"]
        == "api-exhausted"
    )
    assert DAY not in archive.pending_dates(config, DAY + timedelta(days=1))
    assert DAY + timedelta(days=1) in archive.pending_dates(
        config, DAY + timedelta(days=2)
    )


def test_ohlcv_status_remains_local_and_depth_disabled(
    tmp_path, minute_only, monkeypatch
):
    config = settings(tmp_path, hard_stop_free_bytes=0)
    partial = collect(
        API(
            replace(config, ohlcv_recent_pages=1),
            lambda *a: {"result": [], "next": "more"},
        ),
        tmp_path,
    )
    write_json_atomic(tmp_path / "state/depth-status.json", {"invalid": "ignored"})
    result = cli.status(config)
    assert result["collector"]["enabled"] is False
    assert result["ohlcv"]["snapshot_at_ns"] == partial["snapshot_at_ns"]
    assert not result["ohlcv"]["complete"]


def test_replaying_old_durable_page_cannot_undo_newer_observed_correction(
    tmp_path, minute_only, monkeypatch
):
    old_dir, old_query = ohlcv._query(tmp_path, INSTRUMENT, "1m", START, END)
    api = API(
        settings(tmp_path), lambda *a: {"result": [candle(close="10")], "next": ""}
    )
    original = ohlcv._merge
    monkeypatch.setattr(
        ohlcv,
        "_merge",
        lambda *a: (_ for _ in ()).throw(OSError("crash after durable page")),
    )
    with pytest.raises(OSError):
        ohlcv._page(api, tmp_path, old_dir, old_query)
    monkeypatch.setattr(ohlcv, "_merge", original)
    new_dir, new_query = ohlcv._query(tmp_path, INSTRUMENT, "1m", START - MINUTE, END)
    api.handler = lambda *a: {"result": [candle(close="99.00000000001")], "next": ""}
    ohlcv._page(api, tmp_path, new_dir, new_query)
    ohlcv._page(api, tmp_path, old_dir, old_query)
    assert read(tmp_path)[0]["close"] == "99.00000000001"
    assert len(api.calls) == 2


def test_rollover_prioritizes_current_closed_day_over_unfinished_recent_window(
    tmp_path, minute_only
):
    config = settings(tmp_path, ohlcv_recent_pages=1, ohlcv_backfill_pages=1)
    api = API(config, lambda *a: {"result": [], "next": "older"})
    first = collect(api, tmp_path)
    original = first["intervals"]["1m"][INSTRUMENT]["recent"]
    later = ohlcv.collect_ohlcv(api, tmp_path, DAY + timedelta(days=1), [INSTRUMENT])
    new = later["intervals"]["1m"][INSTRUMENT]["recent"]
    assert api.calls[2][-1] == ""  # newest page of the new frozen range
    assert new["end_ns"] == END + ohlcv.DAY and new["start_ns"] == original["start_ns"]
    assert later["intervals"]["1m"][INSTRUMENT]["history"]["end_ns"] == END
    assert not later["complete"]
