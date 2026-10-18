"""Lossless public datasets, fair progress and immutable release integration."""

import hashlib
import io
import json
import tarfile
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pyarrow.parquet as pq
import pytest
from test_archive import DAY, committed, report
from test_publish import FakeGitHub

from grvt_data import archive, cli, market
from grvt_data.api import CursorRejected, GrvtAPI
from grvt_data.config import PUBLIC_DATASETS, Config
from grvt_data.publish import prune_verified, publish_revision
from grvt_data.storage import write_json_atomic
from grvt_data.trades import window_bounds

NAME = "BTC_USDT_Perp"
START, END = window_bounds(DAY)


def settings(root, **kwargs):
    return Config(
        data_dir=root,
        datasets=PUBLIC_DATASETS,
        market_page_budget=kwargs.pop("market_page_budget", 1000),
        market_budget_seconds=100,
        **kwargs,
    )


def funding(instrument, end):
    return {
        "instrument": instrument,
        "funding_time": str(end - 8 * 3600 * market.NS),
        "funding_rate": "-0.001234567890123456789",
        "mark_price": "100.123456789",
        "funding_interval_hours": 8,
        "new_source_field": "retain me",
    }


def candle(instrument, end):
    return {
        "instrument": instrument,
        "open_time": str(end - 120 * market.NS),
        "close_time": str(end - 60 * market.NS),
        "open": "1.000000000000001",
        "high": "2.0",
        "low": "0.5",
        "close": "1.5",
        "volume_b": "0.0",
        "volume_q": "0.0",
        "trades": 0,
    }


class API:
    def __init__(self, config):
        self.config = config
        self.deadline = None
        self.calls = []

    def market_snapshot(self, route, instrument=None):
        self.calls.append((route, instrument))
        assert "book" not in route
        if route in ("all_instruments", "currency"):
            return {"result": []}
        if route == "supported_assets":
            return {"funding": [{"asset_code": "USDT"}], "spot": []}
        if route == "get_all_collateral_asset_info":
            return {"assets": [{"currency": "USDT", "collateral_value_ratio": "99.8"}]}
        if route == "margin_rules":
            return {
                "instrument": instrument,
                "risk_brackets": [{"initial_margin_rate": "0.02"}],
            }
        return {
            "result": {
                "instrument": instrument,
                "mark_price": "100.123456789",
                "open_interest": "200.000000001",
                "unknown": ["preserve", "all", "fields"],
            }
        }

    def funding_page(self, instrument, aggregation, start, end, cursor):
        self.calls.append(("funding", instrument, aggregation, start, end, cursor))
        return {"result": [funding(instrument, end)], "next": ""}

    def candle_page(self, instrument, interval, start, end, cursor, *, price_type):
        self.calls.append(
            ("kline", instrument, interval, price_type, start, end, cursor)
        )
        return {"result": [candle(instrument, end)], "next": ""}


def collect(api, root, day=DAY):
    return market.collect_market(api, root, day, [NAME])


def test_all_aggregations_prices_intervals_and_lossless_snapshots(tmp_path):
    api = API(settings(tmp_path))
    result = collect(api, tmp_path)
    assert result["complete"] and not result["failures"]
    assert result["datasets"]["funding"]["planned"] == 4
    assert result["datasets"]["ohlcv_extra"]["planned"] == 61
    assert {c[2] for c in api.calls if c[0] == "funding"} == set(market.AGGREGATIONS)
    assert {c[3] for c in api.calls if c[0] == "kline"} == {
        "TRADE",
        "MARK",
        "INDEX",
        "MID",
    }
    assert len(market.ALL_INTERVALS) == 18
    path = next((tmp_path / "funding/FUNDING_INTERVAL").rglob("*.parquet"))
    row = pq.read_table(path).to_pylist()[0]
    assert row["funding_rate"] == "-0.001234567890123456789"
    assert json.loads(row["record_json"])["new_source_field"] == "retain me"
    assert path.stat().st_mode & 0o777 == 0o660
    ticker = next((tmp_path / "ticker").rglob("ticker/*.json"))
    assert json.loads(ticker.read_text())["response"]["result"]["unknown"] == [
        "preserve",
        "all",
        "fields",
    ]
    assert ticker.stat().st_mode & 0o777 == 0o660
    ticker.chmod(0o600)  # An earlier deployment's cached snapshots remain reusable.
    assert "files" not in market.market_status(tmp_path)
    before = list(api.calls)
    assert collect(api, tmp_path)["complete"]
    assert api.calls == before, (
        "terminal same-cutoff history and snapshots must be a no-op"
    )
    assert ticker.stat().st_mode & 0o777 == 0o660


def test_snapshot_client_preserves_documented_unwrapped_shapes_and_blocks_depth(
    tmp_path, monkeypatch
):
    payloads = {
        "supported_assets": {"funding": [], "spot": []},
        "get_all_collateral_asset_info": {"assets": []},
        "margin_rules": {"instrument": NAME, "risk_brackets": []},
    }
    requests = []

    def handler(request):
        route = request.url.path.rsplit("/", 1)[-1]
        requests.append(route)
        return httpx.Response(200, json=payloads[route])

    api = GrvtAPI(
        settings(tmp_path), client=httpx.Client(transport=httpx.MockTransport(handler))
    )
    monkeypatch.setattr(api, "_sleep", lambda seconds: None)
    for route, response in payloads.items():
        assert (
            api.market_snapshot(route, NAME if route == "margin_rules" else None)
            == response
        )
    with pytest.raises(ValueError, match="non-depth"):
        api.market_snapshot("book", NAME)
    assert "book" not in requests


def test_page_survives_merge_failure_and_replays_without_refetch(tmp_path, monkeypatch):
    api = API(settings(tmp_path))
    original = market._merge
    failed = []

    def merge(root, spec, rows):
        if spec.get("aggregation") == "FUNDING_INTERVAL" and not failed:
            failed.append(True)
            raise OSError("injected canonical write interruption")
        return original(root, spec, rows)

    monkeypatch.setattr(market, "_merge", merge)
    assert not collect(api, tmp_path)["complete"]
    saved = next(
        (tmp_path / "state/market/pages/funding/FUNDING_INTERVAL").rglob("page-*.json")
    )
    before = saved.read_bytes()
    calls = len(
        [c for c in api.calls if c[:3] == ("funding", NAME, "FUNDING_INTERVAL")]
    )
    assert collect(api, tmp_path)["complete"]
    assert (
        len([c for c in api.calls if c[:3] == ("funding", NAME, "FUNDING_INTERVAL")])
        == calls
    )
    assert saved.read_bytes() == before


def test_old_page_replay_cannot_undo_a_newer_correction(tmp_path):
    spec = {"dataset": "funding", "instrument": NAME, "aggregation": "FUNDING_INTERVAL"}
    api = API(settings(tmp_path))
    query = market._query(0, END)
    market._advance(api, tmp_path, spec, query)
    path = next((tmp_path / "funding").rglob("*.parquet"))
    before = pq.read_table(path).to_pylist()[0]
    new = before | {
        "observed_at_ns": before["observed_at_ns"] + 1,
        "funding_rate": "0.999",
    }
    market._merge(tmp_path, spec, [new])
    market._advance(api, tmp_path, spec, query)
    assert pq.read_table(path).to_pylist()[0]["funding_rate"] == "0.999"


def test_repeated_cursor_fails_before_source_commit(tmp_path):
    api = API(settings(tmp_path))
    api.funding_page = lambda *a: {"result": [funding(NAME, END)], "next": "same"}
    query = market._query(0, END) | {"cursor": "same", "seen": ["same"]}
    with pytest.raises(ValueError, match="cursor"):
        market._advance(
            api,
            tmp_path,
            {
                "dataset": "funding",
                "instrument": NAME,
                "aggregation": "FUNDING_INTERVAL",
            },
            query,
        )
    assert not list(tmp_path.rglob("page-*.json"))
    assert not list(tmp_path.rglob("*.parquet"))


def test_budget_rotation_finishes_all_series_and_new_day_refreshes(tmp_path):
    api = API(settings(tmp_path, market_page_budget=12))
    partial = collect(api, tmp_path)
    assert not partial["complete"] and not partial["failures"]
    for _ in range(12):
        partial = collect(api, tmp_path)
        if partial["complete"]:
            break
    assert partial["complete"], "budget continuation starved unvisited series"
    before = len(api.calls)
    next_day = collect(api, tmp_path, DAY + timedelta(days=1))
    for _ in range(12):
        if next_day["complete"]:
            break
        next_day = collect(api, tmp_path, DAY + timedelta(days=1))
    assert next_day["complete"]
    new = [c for c in api.calls[before:] if c[0] == "funding"]
    assert new and all(c[3] != 0 and c[4] == END + market.DAY for c in new)


def test_bounds_exclude_inclusive_funding_end_and_unclosed_candles(tmp_path):
    api = API(settings(tmp_path))
    api.funding_page = lambda *a: {
        "result": [funding(NAME, END), funding(NAME, END) | {"funding_time": str(END)}],
        "next": "",
    }
    spec = {"dataset": "funding", "instrument": NAME, "aggregation": "FUNDING_INTERVAL"}
    market._advance(api, tmp_path, spec, market._query(0, END))
    assert pq.read_table(next((tmp_path / "funding").rglob("*.parquet"))).num_rows == 1
    api.candle_page = lambda *a, **kw: {
        "result": [candle(NAME, END) | {"close_time": str(END + 1)}],
        "next": "",
    }
    market._advance(
        api,
        tmp_path,
        {
            "dataset": "ohlcv_extra",
            "instrument": NAME,
            "price_type": "MARK",
            "interval": "1m",
        },
        market._query(0, END),
    )
    assert not list((tmp_path / "ohlcv_extra").rglob("*.parquet"))


def test_freeze_publish_cumulative_assets_and_verified_cleanup_retain_canonical(
    tmp_path,
):
    config = settings(tmp_path)
    snapshot = collect(API(config), tmp_path)
    ohlcv = {"files": [], "complete": True, "snapshot_at_ns": time.time_ns()}
    bundle = archive.seal_revision(
        config, DAY, report(), ohlcv_report=ohlcv, market_report=snapshot
    )
    frozen = json.loads((bundle / "manifest-rev01.json").read_text())
    assert set(frozen["market"]["latest_snapshot"]["datasets"]) == set(market.GROUPS)
    assets = frozen["market"]["assets"]
    assert {a["dataset"] for a in assets} == {
        "funding",
        "ticker",
        "metadata",
        "ohlcv-extra",
    }
    canonical = sorted((tmp_path / "funding").rglob("*.parquet"))
    originals = {p: p.read_bytes() for p in canonical}
    gh = FakeGitHub(config)
    publish_revision(config, DAY, 1, github=gh)
    assert "funding: complete" in gh.state["body"]
    restored = {}
    for a in assets:
        remote = next(r for r in gh.state["assets"] if r["name"] == a["name"])
        with tarfile.open(
            fileobj=io.BytesIO(gh.bytes[remote["id"]]), mode="r:gz"
        ) as tar:
            restored.update(
                {m.name: tar.extractfile(m).read() for m in tar.getmembers()}
            )
    for e in frozen["market"]["files"][0]["entries"]:
        data = b"".join(restored[p] for p in e.get("chunks", [e["path"]]))
        assert hashlib.sha256(data).hexdigest() == e["sha256"]
    prune_verified(config, datetime.now(UTC), github=gh)
    assert all(p.read_bytes() == b for p, b in originals.items())
    second = archive.seal_revision(
        config, DAY, report(), ohlcv_report=ohlcv, market_report=snapshot
    )
    manifest2 = json.loads((second / "manifest-rev02.json").read_text())
    assert {a["name"] for a in assets} <= {
        a["name"] for a in manifest2["market"]["assets"]
    }
    publish_revision(config, DAY, 2, github=gh)
    assert "Active revision: 2" in gh.state["body"]


def test_all_mode_requires_reports_and_partial_market_requeues_terminal_ticks(tmp_path):
    config = settings(tmp_path, market_page_budget=8)
    with pytest.raises(ValueError, match="coverage reports"):
        archive.seal_revision(config, DAY, report())
    snapshot = collect(API(config), tmp_path)
    assert not snapshot["complete"]
    archive.seal_revision(
        config,
        DAY,
        report(),
        ohlcv_report={"files": [], "complete": True, "snapshot_at_ns": 1},
        market_report=snapshot,
    )
    committed(config)
    assert DAY in archive.pending_dates(config, DAY + timedelta(days=1))
    assert DAY not in archive.pending_dates(
        replace(config, datasets=("trades", "ohlcv")), DAY + timedelta(days=1)
    )


def test_cli_all_mode_budget_and_legacy_selection(monkeypatch):
    seen = []
    monkeypatch.setattr(cli, "daily", lambda config, **kw: seen.append(config) or 0)
    assert (
        cli.main(
            [
                "daily",
                "--datasets",
                "all",
                "--market-page-budget",
                "55",
                "--market-budget-seconds",
                "9",
            ]
        )
        == 0
    )
    assert seen[-1].datasets == PUBLIC_DATASETS and not seen[-1].depth_enabled
    assert seen[-1].market_page_budget == 55 and seen[-1].market_budget_seconds == 9
    cli.main(["daily", "--datasets", "trades,ohlcv"])
    assert not seen[-1].market_enabled


def test_cursor_rejection_keeps_old_generation_and_retries_frozen_bounds(tmp_path):
    api = API(settings(tmp_path, market_page_budget=100))
    original = api.funding_page
    rejected = []

    def pages(instrument, aggregation, start, end, cursor):
        if aggregation != "FUNDING_INTERVAL" or rejected:
            return original(instrument, aggregation, start, end, cursor)
        if cursor:
            rejected.append(True)
            raise CursorRejected("test cursor expired")
        return {"result": [funding(instrument, end)], "next": "older"}

    api.funding_page = pages
    first = collect(api, tmp_path)
    assert not first["complete"] and first["failures"]
    state_path = next(
        (tmp_path / "state/market/series/funding/FUNDING_INTERVAL").glob("*.json")
    )
    state = json.loads(state_path.read_text())
    assert state["history"]["generation"] == 1 and not state["history"]["exhausted"]
    old = list(
        (tmp_path / "state/market/pages/funding/FUNDING_INTERVAL").rglob(
            "generation-0000/page-*.json"
        )
    )
    assert old
    assert collect(api, tmp_path)["complete"]
    assert all(p.exists() for p in old)


def test_seal_resume_uses_frozen_market_bytes_after_source_changes(
    tmp_path, monkeypatch
):
    config = settings(tmp_path)
    snapshot = collect(API(config), tmp_path)
    original_resume = archive.resume_revision
    monkeypatch.setattr(
        archive,
        "resume_revision",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("crash after freeze")),
    )
    with pytest.raises(RuntimeError, match="after freeze"):
        archive.seal_revision(
            config,
            DAY,
            report(),
            ohlcv_report={"files": [], "complete": True, "snapshot_at_ns": 1},
            market_report=snapshot,
        )
    canonical = next((tmp_path / "funding").rglob("*.parquet"))
    old = canonical.read_bytes()
    canonical.write_bytes(b"subsequent external correction")
    monkeypatch.setattr(archive, "resume_revision", original_resume)
    bundle = archive.resume_revision(config, DAY, 1)
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    frozen = next(
        e
        for e in manifest["market"]["files"][0]["entries"]
        if e["path"].endswith(str(canonical.relative_to(tmp_path)))
    )
    assert (bundle / frozen["path"]).read_bytes() == old
    assert frozen["sha256"] == hashlib.sha256(old).hexdigest()


def test_corrupt_cached_snapshot_never_becomes_complete_coverage(tmp_path):
    api = API(settings(tmp_path))
    assert collect(api, tmp_path)["complete"]
    path = next((tmp_path / "ticker").rglob("ticker/*.json"))
    data = json.loads(path.read_text())
    data["response"]["result"]["mark_price"] = "999.0"
    write_json_atomic(path, data)
    state = collect(api, tmp_path)
    assert not state["complete"] and state["failures"]
    assert not state["datasets"]["ticker"]["complete"]


def test_hard_disk_stop_prevents_api_and_archive_copies(tmp_path, monkeypatch):
    from collections import namedtuple

    usage = namedtuple("usage", "total used free")
    config = settings(tmp_path)
    api = API(config)
    monkeypatch.setattr(market.shutil, "disk_usage", lambda root: usage(100, 99, 1))
    with pytest.raises(OSError, match="disk threshold"):
        collect(api, tmp_path)
    assert not api.calls


def test_existing_complete_ticks_are_reused_when_adding_public_datasets(
    tmp_path, monkeypatch
):
    from test_archive import trade_report

    legacy = replace(settings(tmp_path), datasets=("trades", "ohlcv"))
    trades = trade_report(legacy, DAY)
    ohlcv = {"files": [], "complete": True, "snapshot_at_ns": 1}
    archive.seal_revision(legacy, DAY, trades, ohlcv_report=ohlcv)
    gh = FakeGitHub(legacy)
    publish_revision(legacy, DAY, 1, github=gh)
    prune_verified(legacy, datetime.now(UTC), github=gh)
    old_assets = {a["name"]: (a["id"], gh.hash(a)) for a in gh.state["assets"]}

    class WorkerAPI(API):
        def __init__(self, config, **kwargs):
            super().__init__(config)

        def instruments(self, **kwargs):
            return [{"instrument": NAME}]

        def close(self):
            pass

    monkeypatch.setattr(cli, "utc_today", lambda: DAY + timedelta(days=1))
    monkeypatch.setattr(cli, "GrvtAPI", WorkerAPI)
    monkeypatch.setattr(cli, "ObservedGitHub", lambda *a, **kw: gh)
    monkeypatch.setattr(
        cli,
        "download_trades",
        lambda *a, **kw: pytest.fail("complete retained ticks were downloaded again"),
    )
    monkeypatch.setattr(cli, "collect_ohlcv", lambda *a, **kw: ohlcv)
    assert cli.run_date(settings(tmp_path), DAY, deadline=time.monotonic() + 100) == 0
    assert (
        old_assets.items()
        <= {a["name"]: (a["id"], gh.hash(a)) for a in gh.state["assets"]}.items()
    )
    assert "Active revision: 2" in gh.state["body"] and "funding:" in gh.state["body"]


def test_live_legacy_funding_zero_interval_is_retained_without_inventing_cadence():
    from pathlib import Path

    fixture = json.loads(
        (
            Path(__file__).parent / "fixtures/market/public-market-live-2026-10-18.json"
        ).read_text()
    )
    raw = fixture["legacy_funding_zero_interval"]
    row = market._normalise(
        raw,
        {"dataset": "funding", "instrument": NAME, "aggregation": "FUNDING_INTERVAL"},
        123,
    )
    assert row["funding_interval_hours"] == 0
    assert row["funding_rate"] == raw["funding_rate"]
    assert json.loads(row["record_json"]) == raw
