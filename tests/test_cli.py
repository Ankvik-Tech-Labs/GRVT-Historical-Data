"""Offline command orchestration and real process deadline checks."""

from datetime import UTC, datetime, timedelta

from grvt_data import cli
from grvt_data.config import Config
from grvt_data.storage import publication_db, start_date_run


def test_crash_before_spawn_remains_pending_across_rollover(tmp_path, monkeypatch):
    config = Config(data_dir=tmp_path)
    old = datetime.now(UTC).date() - timedelta(days=3)
    start_date_run(config, old)
    seen = []
    monkeypatch.setattr(
        cli, "supervise_date", lambda c, d, *a, **kw: seen.append(d) or 0
    )
    assert cli.daily(config) == 0
    assert seen == [datetime.now(UTC).date() - timedelta(days=1), old]
    with publication_db(config) as db:
        assert (
            db.execute(
                "SELECT status FROM per_date_runs WHERE day=? ORDER BY attempt_id",
                (str(old),),
            ).fetchone()[0]
            == "interrupted"
        )


def test_end_time_ns_int64():
    assert cli.int64("123") == 123


import argparse
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from test_archive import committed, report
from test_publish import FakeGitHub

from grvt_data.archive import pending_dates, seal_revision
from grvt_data.storage import write_json_atomic
from grvt_data.trades import window_bounds


def yesterday():
    return datetime.now(UTC).date() - timedelta(days=1)


def outcomes(config):
    db = publication_db(config)
    try:
        return [
            dict(r)
            for r in db.execute("SELECT * FROM per_date_runs ORDER BY started_at_ns")
        ]
    finally:
        db.close()


def terminal(config, day):
    seal_revision(config, day, report(day))
    committed(config, day)


def test_yesterday_first_then_oldest_and_failed_date_isolated(tmp_path, monkeypatch):
    config = Config(data_dir=tmp_path)
    old = yesterday() - timedelta(days=3)
    middle = yesterday() - timedelta(days=1)
    for day in (old, middle):
        start_date_run(config, day)
    seen = []

    def worker(c, day, *a, **kw):
        assert outcomes(c)[-1]["status"] == "running"
        seen.append(day)
        return int(day == old)

    monkeypatch.setattr(cli, "supervise_date", worker)
    assert cli.daily(config) == 1
    assert seen == [yesterday(), old, middle]
    assert [r["status"] for r in outcomes(config)[2:]] == [
        "completed",
        "failed",
        "completed",
    ]


def test_terminal_yesterday_is_true_noop(tmp_path, monkeypatch):
    config = Config(data_dir=tmp_path)
    terminal(config, yesterday())
    monkeypatch.setattr(
        cli, "supervise_date", lambda *a, **kw: pytest.fail("worker on terminal date")
    )
    assert cli.daily(config) == cli.daily(config) == 0
    assert outcomes(config) == []


def test_finish_unfinished_before_new_seal_and_remote_commit_reconciles(
    tmp_path, monkeypatch
):
    config = Config(data_dir=tmp_path)
    day = yesterday()
    seal_revision(config, day, report(day))
    gh = FakeGitHub(config)
    monkeypatch.setattr(cli, "ObservedGitHub", lambda *a, **kw: gh)
    monkeypatch.setattr(
        cli, "GrvtAPI", lambda *a, **kw: pytest.fail("download after reconciled commit")
    )
    # Commit remotely and leave local ledger stale, as if killed after PATCH.
    gh.crash = "publish"
    from test_publish import Crash

    with pytest.raises(Crash):
        cli.publish_revision(config, day, 1, github=gh)
    gh.calls.clear()
    assert cli.run_date(config, day, deadline=time.monotonic() + 5) == 0
    assert not any(c[0] in ("upload", "create", "patch") for c in gh.calls)
    assert len(cli.revisions(config, day)) == 1


def test_partial_download_publishes_valid_bytes_but_exits_nonzero(
    tmp_path, monkeypatch
):
    config = Config(data_dir=tmp_path)
    gh = FakeGitHub(config)
    monkeypatch.setattr(cli, "ObservedGitHub", lambda *a, **kw: gh)

    class API:
        def __init__(self, *a, **kw):
            assert kw["deadline"] > time.monotonic()

        def instruments(self, **kw):
            return []

        def close(self):
            pass

    monkeypatch.setattr(cli, "GrvtAPI", API)
    monkeypatch.setattr(
        cli,
        "download_trades",
        lambda *a, **kw: report(yesterday(), complete=False, failures={"X": "failed"}),
    )
    assert cli.run_date(config, yesterday(), deadline=time.monotonic() + 5) == 1
    assert cli.revisions(config, yesterday())[-1]["status"] == "committed"
    assert yesterday() in pending_dates(config, cli.utc_today())


@pytest.mark.parametrize(
    "value", ["2026-10-16T00:00:00Z", "1.0", str(2**63), str(-(2**63) - 1), "+1", ""]
)
def test_end_time_rejects_non_int64(value):
    with pytest.raises(argparse.ArgumentTypeError):
        cli.int64(value)


def test_end_time_requires_allow_today(tmp_path):
    assert cli.main(["daily", "--data-dir", str(tmp_path), "--end-time-ns", "1"]) == 1


def test_allow_today_requires_drained_pending_then_exact_today(tmp_path, monkeypatch):
    config = Config(data_dir=tmp_path)
    cutoff = time.time_ns() - 1000
    write_json_atomic(
        tmp_path / "state/depth-checkpoint.json", {"cutoff_received_ns": cutoff}
    )
    with pytest.raises(ValueError, match="drain pending"):
        cli.daily(config, allow_today=True, end_ns=cutoff)
    terminal(config, yesterday())
    seen = []
    monkeypatch.setattr(
        cli,
        "supervise_date",
        lambda c, d, *a, **kw: seen.append((d, kw["end_ns"])) or 0,
    )
    assert cli.daily(config, allow_today=True, end_ns=cutoff) == 0
    assert seen == [(cli.utc_today(), cutoff)]
    start_date_run(config, yesterday() - timedelta(days=2))
    with pytest.raises(ValueError, match="drain pending"):
        cli.daily(config, allow_today=True, end_ns=cutoff)


@pytest.mark.parametrize("offset", [-1, 86400 * 10**9])
def test_activation_rejects_outside_today(tmp_path, offset):
    config = Config(data_dir=tmp_path)
    terminal(config, yesterday())
    with pytest.raises(ValueError, match="within today"):
        cli.daily(
            config, allow_today=True, end_ns=window_bounds(cli.utc_today())[0] + offset
        )


def test_activation_rejects_unpersisted_cutoff(tmp_path):
    config = Config(data_dir=tmp_path)
    terminal(config, yesterday())
    cutoff = time.time_ns() - 10**9
    write_json_atomic(
        tmp_path / "state/depth-checkpoint.json", {"cutoff_received_ns": cutoff - 1}
    )
    with pytest.raises(ValueError, match="persisted"):
        cli.daily(config, allow_today=True, end_ns=cutoff)


def test_rotate_now_uses_actual_control_receipt(tmp_path, monkeypatch, capsys):
    async def control(root, command):
        assert command == "rotate_now"
        return {
            "cutoff_received_ns": 1770000000123456789,
            "closed_segments": ["exact.frames"],
        }

    monkeypatch.setattr(cli, "depth_control", control)
    assert cli.main(["rotate-now", "--data-dir", str(tmp_path)]) == 0
    output = capsys.readouterr().out.splitlines()
    assert json.loads(output[0])["closed_segments"] == ["exact.frames"]
    assert output[1] == "1770000000123456789"


BOOTSTRAP = """
import sys, os, time, signal, subprocess, json
from pathlib import Path
from grvt_data import cli
from grvt_data.config import Config
class API:
    def __init__(self, *a, **kw): pass
    def instruments(self, **kw): return []
    def close(self): pass
cli.GrvtAPI = API
{setup}
sys.exit(cli.main(['_worker', *sys.argv[1:]]))
"""


def test_parent_deadline_kills_real_in_process_sealing_and_descendant(
    tmp_path, monkeypatch
):
    config = Config(data_dir=tmp_path, per_date_deadline_seconds=0.7)
    pid_path = tmp_path / "descendant"
    code = BOOTSTRAP.replace(
        "{setup}",
        f"""
def seal(*args, **kw):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = subprocess.Popen([sys.executable, '-c', 'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'], pass_fds=(int(os.environ['GRVT_PUBLICATION_LOCK_FD']),))
    Path({str(pid_path)!r}).write_text(str(child.pid))
    while True: pass
cli.seal_revision = seal
""",
    )
    original = cli.supervise_date
    old = yesterday() - timedelta(days=3)
    later = yesterday() - timedelta(days=2)
    for day in (old, later):
        start_date_run(config, day)
    seen = []

    def supervise(c, day, fd, **kw):
        seen.append(day)
        if day == old:
            return original(c, day, fd, command=[sys.executable, "-c", code], grace=0.1)
        return 0

    monkeypatch.setattr(cli, "supervise_date", supervise)
    started = time.monotonic()
    assert cli.daily(config) == 1
    assert time.monotonic() - started < 3
    assert seen == [yesterday(), old, later]
    assert [r["status"] for r in outcomes(config)[2:]] == [
        "completed",
        "deadline_exceeded",
        "completed",
    ]
    assert pid_path.exists(), "actual sealer must have started"
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_path.read_text()), 0)
    with cli.publication_lock(config, 0) as fd:
        assert fd is not None


def test_parent_deadline_kills_trickling_real_http_worker(tmp_path):
    class Trickle(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(200)
            self.end_headers()
            try:
                for _ in range(100):
                    self.wfile.write(b" ")
                    self.wfile.flush()
                    time.sleep(0.03)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Trickle)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = Config(
        data_dir=tmp_path,
        per_date_deadline_seconds=0.7,
        rest_url=f"http://127.0.0.1:{server.server_port}",
    )
    code = BOOTSTRAP.replace(
        "{setup}",
        """
from grvt_data.api import GrvtAPI
# Suppress cooperative checks to prove the hard supervisor itself bounds a
# transport that trickles under every socket timeout.
GrvtAPI.check_deadline = lambda self: None
cli.GrvtAPI = GrvtAPI
""",
    )
    try:
        with cli.publication_lock(config, 0) as fd:
            started = time.monotonic()
            assert (
                cli.supervise_date(
                    config,
                    yesterday(),
                    fd,
                    command=[sys.executable, "-c", code],
                    grace=0.1,
                )
                == 124
            )
            assert time.monotonic() - started < 2
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_worker_and_gh_descendants_inherit_real_lock_fd(tmp_path):
    config = Config(data_dir=tmp_path, per_date_deadline_seconds=3)
    code = BOOTSTRAP.replace(
        "{setup}",
        """
def worker(config, day, **kw):
    fd = int(os.environ['GRVT_PUBLICATION_LOCK_FD'])
    os.fstat(fd)
    # Use the actual GitHub subprocess path with a fake gh executable.
    cli.GitHub(config, deadline=kw['deadline'])._run(['api', 'anything'])
    return 0
cli.run_date = worker
""",
    )
    gh = tmp_path / "gh"
    gh.write_text(
        "#!"
        + sys.executable
        + '\nimport os,json\nos.fstat(int(os.environ["GRVT_PUBLICATION_LOCK_FD"]))\nprint(json.dumps({"ok":True}))\n'
    )
    gh.chmod(0o755)
    prior = os.environ["PATH"]
    try:
        os.environ["PATH"] = str(tmp_path) + ":" + prior
        with cli.publication_lock(config, 0) as fd:
            assert (
                cli.supervise_date(
                    config, yesterday(), fd, command=[sys.executable, "-c", code]
                )
                == 0
            )
    finally:
        os.environ["PATH"] = prior


def test_manual_publish_and_daily_contend_on_same_real_lock(tmp_path):
    config = Config(data_dir=tmp_path)
    with cli.publication_lock(config, 0):
        for command in (["daily"], ["publish", "--date", str(yesterday())]):
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "grvt_data.cli",
                    *command,
                    "--data-dir",
                    str(tmp_path),
                    "--lock-wait-seconds",
                    ".05",
                ],
                capture_output=True,
                text=True,
                timeout=3,
                check=False,
            )
            assert result.returncode == 75
            assert "lock held, skipping run" in result.stdout
    assert outcomes(config) == []


def test_status_local_only_heartbeat_quiet_books_disk_and_release_counts(
    tmp_path, monkeypatch
):
    config = Config(data_dir=tmp_path)
    now = time.time_ns()
    write_json_atomic(
        tmp_path / "state/depth-status.json",
        {
            "received_ns": now,
            "instruments": {
                "X": {"ready": True, "last_update_received_ns": now - 86400 * 10**9}
            },
        },
    )
    write_json_atomic(
        tmp_path / "state/releases/2026-10-15.json",
        {"asset_count": 14, "observed_at_ns": now},
    )
    write_json_atomic(
        tmp_path / "state/retry-queue.json",
        {"dates": {"2026-10-14": {"created_at_ns": now - 100 * 10**9}}},
    )
    monkeypatch.setattr(
        cli, "publication_lock", lambda *a: pytest.fail("ordinary status must not lock")
    )
    monkeypatch.setattr(
        cli, "ObservedGitHub", lambda *a, **kw: pytest.fail("status network")
    )
    monkeypatch.setattr(
        cli,
        "disk_status",
        lambda *a: {
            "tier": "warning",
            "free_fraction": 0.15,
            "total_bytes": 1000,
            "free_bytes": 150,
            "cleanup_requested": False,
        },
    )
    snapshot = cli.status(config)
    assert snapshot["collector"]["live"]
    assert (
        snapshot["collector"]["instruments"]["X"]["last_update_received_ns"]
        < now - 3600 * 10**9
    )
    assert snapshot["disk"]["free_fraction"] == 0.15
    assert snapshot["releases"][0]["asset_count"] == 14
    assert snapshot["retry_queue"]["oldest_age_seconds"] >= 100
    assert not (tmp_path / "state/status-health.json").exists()
    cli.status(config, check=True)
    assert (tmp_path / "state/status-health.json").exists()
    assert len((tmp_path / "logs/status.log").read_text().splitlines()) == 1


def test_status_critical_prune_skips_busy_lock_then_takes_same_lock(
    tmp_path, monkeypatch
):
    config = Config(data_dir=tmp_path)
    monkeypatch.setattr(cli, "disk_status", lambda *a: {"cleanup_requested": True})
    calls = []
    monkeypatch.setattr(
        cli,
        "prune_if_critical",
        lambda *a, **kw: (
            calls.append(os.fstat(int(os.environ["GRVT_PUBLICATION_LOCK_FD"]))) or []
        ),
    )
    with cli.publication_lock(config, 0):
        assert cli.status(config, check=True)["cleanup"].startswith("skipped")
    assert calls == []
    assert cli.status(config, check=True)["cleanup"] == []
    assert len(calls) == 1


def test_parent_death_does_not_release_worker_inherited_lock(tmp_path):
    cli._subreaper()
    config = Config(data_dir=tmp_path)
    ready = tmp_path / "ready"
    code = f"""
import os,sys,time,subprocess
from pathlib import Path
from grvt_data import cli
from grvt_data.config import Config
config=Config(data_dir=Path({str(tmp_path)!r}))
with cli.publication_lock(config,0) as fd:
    child = subprocess.Popen([sys.executable,'-c', 'import time; time.sleep(1.2)'], pass_fds=(fd,))
    Path({str(ready)!r}).write_text(str(child.pid))
    os._exit(0)
"""
    proc = subprocess.Popen([sys.executable, "-c", code])
    proc.wait(timeout=3)
    assert ready.exists()
    with cli.publication_lock(config, 0.01) as fd:
        assert fd is None
    os.waitpid(int(ready.read_text()), 0)
    with cli.publication_lock(config, 0) as fd:
        assert fd is not None


def test_per_date_budget_reaches_http_and_gh_and_shrinks(tmp_path, monkeypatch):
    import httpx

    from grvt_data.api import GrvtAPI
    from grvt_data.config import DeadlineExceeded
    from grvt_data.publish import GitHub

    deadline = time.monotonic() + 0.5
    observed = []

    def request(req):
        observed.append(req.extensions["timeout"]["read"])
        return httpx.Response(200, json={"result": []})

    api = GrvtAPI(
        Config(data_dir=tmp_path),
        deadline=deadline,
        client=httpx.Client(transport=httpx.MockTransport(request)),
    )
    api._post("instruments", {})
    time.sleep(0.02)
    api._next_request = 0
    api._post("instruments", {})
    assert 0 < observed[1] < observed[0] <= 0.5
    bounds = []

    def run(*a, **kw):
        bounds.append(kw["timeout"])
        return subprocess.CompletedProcess(a, 0, b"{}", b"")

    monkeypatch.setattr(cli.subprocess, "run", run)
    gh = GitHub(Config(data_dir=tmp_path), deadline=deadline)
    gh._run(["api", "x"])
    time.sleep(0.02)
    gh._run(["api", "x"])
    assert 0 < bounds[1] < bounds[0] < observed[1]
    api.deadline = time.monotonic() - 1
    with pytest.raises(DeadlineExceeded):
        api._post("instruments", {})
    api.close()


def test_activation_retry_same_receipt_does_not_create_second_revision(
    tmp_path, monkeypatch
):
    config = Config(data_dir=tmp_path)
    today = cli.utc_today()
    cutoff = time.time_ns() - 40 * 10**9
    # Use a fake ledger row to focus orchestration; sealer receipt certification
    # itself is already exercised by the archive activation tests.
    row = {
        "status": "committed",
        "revision": 1,
        "request_json": json.dumps({"end_ns": cutoff}),
    }
    monkeypatch.setattr(cli, "revisions", lambda *a: [row])
    monkeypatch.setattr(
        cli, "GrvtAPI", lambda *a, **kw: pytest.fail("duplicate download")
    )
    monkeypatch.setattr(cli, "prune_verified", lambda *a, **kw: [])
    assert (
        cli.run_date(config, today, deadline=time.monotonic() + 1, end_ns=cutoff) == 0
    )


def test_sigterm_records_interrupted_and_cleans_real_worker(tmp_path):
    ready = tmp_path / "ready"
    # Parent supervises a real worker and is interrupted while blocked in wait.
    code = f"""
import sys,time,os
from pathlib import Path
from grvt_data import cli
from grvt_data.config import Config
original=cli.supervise_date
worker="import os,time; from pathlib import Path; fd=int(os.environ['GRVT_WORKER_GATE_FD']); assert os.read(fd,1)==b'1'; Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
cli.supervise_date=lambda c,d,fd,**kw: original(c,d,fd,command=[sys.executable,'-c',worker],grace=.1)
try: cli.daily(Config(data_dir=Path({str(tmp_path)!r})))
except InterruptedError: sys.exit(143)
"""
    proc = subprocess.Popen([sys.executable, "-c", code])
    try:
        until = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < until:
            time.sleep(0.01)
        assert ready.exists()
        proc.terminate()
        assert proc.wait(timeout=3) == 143
        assert outcomes(Config(data_dir=tmp_path))[-1]["status"] == "interrupted"
        with pytest.raises(ProcessLookupError):
            os.kill(int(ready.read_text()), 0)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_status_critical_cleanup_paths_are_json_safe_and_release_lock(
    tmp_path, monkeypatch
):
    config = Config(data_dir=tmp_path)
    removed = [
        tmp_path / "staging/2026-10-15/rev01",
        tmp_path / "raw/depth/2026-10-15/closed.frames",
    ]
    monkeypatch.setattr(cli, "disk_status", lambda *a: {"cleanup_requested": True})

    def prune(*args, **kwargs):
        os.fstat(int(os.environ["GRVT_PUBLICATION_LOCK_FD"]))
        return removed

    monkeypatch.setattr(cli, "prune_if_critical", prune)
    snapshot = cli.status(config, check=True)
    expected = [str(path) for path in removed]
    assert snapshot["cleanup"] == expected
    assert json.loads((tmp_path / "state/status-health.json").read_text()) == snapshot
    log = tmp_path / "logs/status.log"
    assert [json.loads(line) for line in log.read_text().splitlines()] == [snapshot]
    with cli.publication_lock(config, 0) as fd:
        assert fd is not None
    assert cli.main(["status", "--check", "--data-dir", str(tmp_path)]) == 0
    persisted = json.loads((tmp_path / "state/status-health.json").read_text())
    assert persisted["cleanup"] == expected
    lines = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(lines) == 2
    assert lines[-1] == persisted
    with cli.publication_lock(config, 0) as fd:
        assert fd is not None
