"""Tick-only mode exercises real download, archives and publication offline."""

import hashlib
import io
import json
import sys
import tarfile
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_archive import DAY, committed, report, segment, trade_report
from test_publish import FakeGitHub

from grvt_data import archive, cli
from grvt_data.archive import pending_dates, resume_revision, seal_revision, sha256
from grvt_data.config import Config
from grvt_data.publish import prune_verified, publish_revision
from grvt_data.storage import (
    TRADE_SCHEMA,
    publication_db,
    start_date_run,
    write_json_atomic,
)
from grvt_data.trades import window_bounds


def ticks(tmp_path):
    return Config(data_dir=tmp_path, datasets=("trades",))


def manifest(bundle, revision=1):
    return json.loads((bundle / f"manifest-rev{revision:02d}.json").read_text())


def assert_remote_canonical(gh, paths):
    archived = {}
    for asset in gh.state["assets"]:
        if not asset["name"].endswith(".tar.gz"):
            continue
        with tarfile.open(
            fileobj=io.BytesIO(gh.bytes[asset["id"]]), mode="r:gz"
        ) as tar:
            for entry in tar.getmembers():
                if entry.name.endswith("trades.parquet"):
                    instrument = Path(entry.name).parent.name
                    archived[instrument] = hashlib.sha256(
                        tar.extractfile(entry).read()
                    ).hexdigest()
    assert archived == {p.parent.parent.name: sha256(p) for p in paths}


def test_daily_download_publishes_and_retains_185_canonical_files(
    tmp_path, monkeypatch
):
    config = ticks(tmp_path)
    day = cli.utc_today() - timedelta(days=1)
    start, end = window_bounds(day)
    active = [{"instrument": f"ACTIVE_{i}", "base": str(i)} for i in range(184)]
    retained = {"instrument": "RETAINED", "base": "delisted"}
    write_json_atomic(
        tmp_path / "state/discovery" / f"{day}.json", {"instruments": [retained]}
    )
    queried = []
    row = {
        "instrument": "ACTIVE_0",
        "trade_id": "one",
        "event_time": str(start + 1),
        "price": "123.4567890123456789",
        "size": "0.00000001",
        "venue": "GRVT",
        "is_taker_buyer": True,
    }

    class API:
        def __init__(self, settings, **kwargs):
            self.config = settings

        def instruments(self, *, active):
            assert active is True
            return globals_active

        def trade_pages(self, instrument, first, last, cursor):
            assert (first, last, cursor) == (start, end, "")
            queried.append(instrument)
            yield {"result": [row] if instrument == "ACTIVE_0" else [], "next": ""}

        def close(self):
            pass

    globals_active = active
    gh = FakeGitHub(config)
    monkeypatch.setattr(cli, "GrvtAPI", API)
    monkeypatch.setattr(cli, "ObservedGitHub", lambda *a, **kw: gh)
    monkeypatch.setattr(cli, "collect_depth", lambda *a: pytest.fail("depth started"))
    monkeypatch.setattr(
        cli, "depth_control", lambda *a: pytest.fail("depth socket used")
    )
    assert cli.run_date(config, day, deadline=time.monotonic() + 30) == 0
    assert set(queried) == {r["instrument"] for r in [*active, retained]}
    paths = sorted((tmp_path / "trades" / str(day)).glob("*/*/trades.parquet"))
    assert len(paths) == 185
    assert_remote_canonical(gh, paths)
    for path in paths:
        assert pq.read_schema(path) == TRADE_SCHEMA
        assert path.stat().st_mode & 0o777 == 0o660
    populated = next(p for p in paths if p.parent.parent.name == "ACTIVE_0")
    assert pq.read_table(populated).to_pylist()[0]["price"] == row["price"]
    assert pq.read_table(populated).to_pylist()[0]["event_time"] == start + 1
    assert not list((tmp_path / "trades").glob("*/*/*/pages/*.json"))
    assert not (tmp_path / "staging" / str(day) / "rev01").exists()
    assert not any("depth" in a["name"] for a in gh.state["assets"])
    with publication_db(config) as db:
        assert db.execute("SELECT count(*) FROM segments").fetchone()[0] == 0
        saved = json.loads(
            db.execute("SELECT manifest_json FROM revisions").fetchone()[0]
        )
    assert saved["depth"]["status"] == "disabled"
    assert saved["trades"]["full_day_complete"]
    assert (
        len(
            json.loads((tmp_path / "state/discovery" / f"{day}.json").read_text())[
                "instruments"
            ]
        )
        == 185
    )
    before = len(queried)
    assert cli.run_date(config, day, deadline=time.monotonic() + 30) == 0
    assert len(queried) == before


@pytest.mark.parametrize("legacy", [False, True])
def test_verified_cleanup_preserves_canonical_even_for_old_mixed_rows(tmp_path, legacy):
    config = Config(data_dir=tmp_path) if legacy else ticks(tmp_path)
    trades = trade_report(config)
    path = Path(trades["instruments"]["BTC_USDT_Perp"]["path"])
    page = path.parent / "pages/0000000000.json"
    write_json_atomic(page, {"transient": True})
    progress = path.parent / "progress.json"
    write_json_atomic(progress, {"small": "state"})
    bundle = seal_revision(config, DAY, trades)
    gh = FakeGitHub(config)
    assert prune_verified(config, datetime.now(UTC), github=gh) == []
    assert page.exists() and path.exists() and bundle.exists()
    publish_revision(config, DAY, 1, github=gh)
    assert_remote_canonical(gh, [path])
    prune_verified(ticks(tmp_path), datetime.now(UTC), github=gh)
    assert path.exists() and progress.exists()
    assert not page.exists() and not bundle.exists()
    assert_remote_canonical(gh, [path])


@pytest.mark.parametrize("legacy_request", [False, True])
def test_crashed_seal_resumes_with_frozen_datasets(
    tmp_path, monkeypatch, legacy_request
):
    config = Config(data_dir=tmp_path) if legacy_request else ticks(tmp_path)
    raw = segment(config)
    original = archive._parts

    def crash(*args, **kwargs):
        raise RuntimeError("crash before packaging")

    monkeypatch.setattr(archive, "_parts", crash)
    with pytest.raises(RuntimeError, match="crash"):
        seal_revision(config, DAY, trade_report(config))
    monkeypatch.setattr(archive, "_parts", original)
    other = ticks(tmp_path) if legacy_request else Config(data_dir=tmp_path)
    bundle = resume_revision(other, DAY, 1)
    saved = manifest(bundle)
    assert saved["datasets"] == list(config.datasets)
    assert bool(saved["depth"]["assets"]) is legacy_request
    with publication_db(config) as db:
        assert db.execute("SELECT count(*) FROM segments").fetchone()[0] == int(
            legacy_request
        )
        request = json.loads(
            db.execute("SELECT request_json FROM revisions").fetchone()[0]
        )
    assert request["datasets"] == list(config.datasets)
    assert raw.exists()


def test_tick_revision_preserves_old_depth_manifest_and_assets(tmp_path):
    legacy = Config(data_dir=tmp_path)
    segment(legacy)
    first = seal_revision(legacy, DAY, trade_report(legacy))
    old = manifest(first)
    gh = FakeGitHub(legacy)
    publish_revision(legacy, DAY, 1, github=gh)
    old_remote = dict(gh.bytes)
    leftover = segment(legacy, index=2)
    second = seal_revision(ticks(tmp_path), DAY, trade_report(legacy))
    new = manifest(second, 2)
    assert new["depth"] == old["depth"]
    assert new["datasets"] == ["trades"]
    assert new["trades"]["assets"][0]["instrument_coverage"][0]["superseded_by"] == 2
    own = json.loads((second / "journal.json").read_text())["assets"]
    assert {a["dataset"] for a in own} == {"trades", "manifest"}
    publish_revision(ticks(tmp_path), DAY, 2, github=gh)
    assert all(gh.bytes[k] == v for k, v in old_remote.items())
    prune_verified(ticks(tmp_path), datetime.now(UTC), github=gh)
    assert leftover.exists()


def test_pending_ignores_depth_only_but_retains_attempts_and_tick_retry(tmp_path):
    config = ticks(tmp_path)
    today = DAY + timedelta(days=4)
    seal_revision(config, today - timedelta(days=1), report(today - timedelta(days=1)))
    committed(config, today - timedelta(days=1))
    raw = segment(config)
    write_json_atomic(
        tmp_path / "state/outages/depth-only.json",
        {"segment": str(raw.relative_to(tmp_path))},
    )
    assert pending_dates(config, today) == set()
    assert DAY in pending_dates(replace(config, datasets=("trades", "depth")), today)
    attempt_day = DAY + timedelta(days=1)
    start_date_run(config, attempt_day)
    assert pending_dates(config, today) == {attempt_day}
    write_json_atomic(
        tmp_path / "state/retry-queue.json", {"dates": {str(DAY): {"created_at_ns": 1}}}
    )
    assert pending_dates(config, today) == {DAY, attempt_day}


def test_same_day_exact_exchange_cutoff_without_depth_receipt(tmp_path, monkeypatch):
    config = ticks(tmp_path)
    today = cli.utc_today()
    cutoff = time.time_ns() - 40 * 10**9
    assert window_bounds(today)[0] < cutoff
    yesterday = today - timedelta(days=1)
    seal_revision(config, yesterday, report(yesterday))
    committed(config, yesterday)
    seen = []
    monkeypatch.setattr(
        cli,
        "supervise_date",
        lambda c, d, *a, **kw: seen.append((d, kw["end_ns"], c.datasets)) or 0,
    )
    assert cli.daily(config, allow_today=True, end_ns=cutoff) == 0
    assert seen == [(today, cutoff, ("trades",))]
    bundle = seal_revision(
        config,
        today,
        trade_report(config, today, cutoff),
        allow_today=True,
        end_ns=cutoff,
    )
    saved = manifest(bundle)
    assert saved["activation_receipt"] is None
    assert saved["trades"]["latest_report"]["end_ns"] == cutoff
    assert saved["trades"]["provisional"]
    assert "explicit exchange-time" in saved["trades"]["cutoff_note"]
    assert str(today) in archive.retry_queue(config)["dates"]
    assert not (tmp_path / "state/depth-checkpoint.json").exists()


def test_tick_status_ignores_stale_or_corrupt_depth_health(tmp_path, monkeypatch):
    config = ticks(tmp_path)
    path = tmp_path / "state/depth-status.json"
    path.parent.mkdir()
    path.write_text("obsolete corrupt heartbeat")
    monkeypatch.setattr(cli, "disk_status", lambda *a: {"cleanup_requested": False})
    snapshot = cli.status(config, check=True)
    assert (tmp_path / "state/status-health.json").exists()
    assert snapshot["datasets"] == ["trades"]
    assert snapshot["collector"]["status"] == "disabled"
    assert snapshot["collector"]["enabled"] is False
    assert "live" not in snapshot["collector"]


def test_cli_defaults_and_worker_payload_preserve_tick_scope(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(cli, "daily", lambda config, **kw: seen.append(config) or 0)
    assert cli.main(["daily", "--datasets", "trades"]) == 0
    assert seen[0].data_dir == Path("/srv/grvt")
    assert seen[0].datasets == ("trades",)
    output = tmp_path / "payload.json"
    command = [
        sys.executable,
        "-c",
        (
            "import json,os,sys; from pathlib import Path; "
            "assert os.read(int(os.environ['GRVT_WORKER_GATE_FD']),1)==b'1'; "
            f"Path({str(output)!r}).write_text(sys.argv[1])"
        ),
    ]
    config = replace(ticks(tmp_path), per_date_deadline_seconds=2)
    with cli.publication_lock(config, 0) as fd:
        assert cli.supervise_date(config, DAY, fd, command=command) == 0
    payload = json.loads(output.read_text())
    assert payload["datasets"] == ["trades"]
    assert Config(**payload).datasets == ("trades",)
