"""Host CLI: one publication lock, durable attempts and killable date workers."""

import argparse
import asyncio
import contextlib
import ctypes
import fcntl
import json
import logging
import math
import os
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from .api import GrvtAPI
from .archive import pending_dates, resume_revision, retry_status, seal_revision
from .config import Config, DeadlineExceeded
from .depth import collect_depth, depth_control, persist_discovery
from .publish import GitHub, prune_if_critical, prune_verified, publish_revision
from .storage import (
    disk_status,
    finish_date_run,
    publication_db,
    recover_date_runs,
    start_date_run,
    write_json_atomic,
)
from .trades import day_universe, download_trades, window_bounds

DEFAULT_ROOT = Path("/srv/grvt")


def utc_today():
    return datetime.now(UTC).date()


def int64(value):
    try:
        if not value or value.lstrip("-").isdigit() is False:
            raise ValueError
        number = int(value)
        if not -(2**63) <= number < 2**63:
            raise ValueError
        return number
    except ValueError:
        raise argparse.ArgumentTypeError("expected numeric int64 nanoseconds") from None


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("expected finite positive seconds")
    return number


@contextlib.contextmanager
def publication_lock(config, seconds=60):
    path = config.data_dir / "state/run-daily.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as stream:
        deadline = time.monotonic() + seconds
        while True:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    print(
                        f"{datetime.now(UTC).isoformat()} lock held, skipping run",
                        flush=True,
                    )
                    yield None
                    return
                time.sleep(min(0.1, max(0, deadline - time.monotonic())))
        # Closing the parent's FD must not unlock a surviving worker's duplicate.
        previous = os.environ.get("GRVT_PUBLICATION_LOCK_FD")
        os.environ["GRVT_PUBLICATION_LOCK_FD"] = str(stream.fileno())
        try:
            yield stream.fileno()
        finally:
            if previous is None:
                os.environ.pop("GRVT_PUBLICATION_LOCK_FD", None)
            else:
                os.environ["GRVT_PUBLICATION_LOCK_FD"] = previous


class ObservedGitHub(GitHub):
    """Cache actual remote observations for local-only status, never estimates."""

    def release(self, day):
        release = super().release(day)
        write_json_atomic(
            self.config.data_dir / "state/releases" / f"{day}.json",
            {
                "day": str(day),
                "observed_at_ns": time.time_ns(),
                "asset_count": len(release["assets"]) if release else 0,
                "asset_cap": self.config.release_asset_cap,
            },
        )
        return release


def revisions(config, day):
    db = publication_db(config)
    try:
        return [
            dict(r)
            for r in db.execute(
                "SELECT * FROM revisions WHERE day=? ORDER BY revision", (str(day),)
            )
        ]
    finally:
        db.close()


def run_date(config, day, *, deadline, end_ns=None, publish_only=False):
    """Internal unlocked worker. Reconcile unfinished remote work before new work."""
    github = ObservedGitHub(config, deadline=deadline)
    rows = revisions(config, day)
    unfinished = [r for r in rows if r["status"] != "committed"]
    incomplete = False
    for row in unfinished:
        resume_revision(config, day, row["revision"], deadline=deadline)
        publish_revision(config, day, row["revision"], deadline=deadline, github=github)
        incomplete |= not json.loads(row["request_json"])["trade_report"]["complete"]
    # Reconcile remote commits first, then reclaim proven inputs before new work
    # can consume critical disk space or fail while retaining the global lock.
    prune_if_critical(config, datetime.now(UTC), deadline=deadline, github=github)
    if publish_only:
        if not rows:
            raise ValueError("no revision exists for date")
        if not unfinished:
            publish_revision(
                config, day, rows[-1]["revision"], deadline=deadline, github=github
            )
    elif (
        end_ns is not None
        and not (rows and json.loads(rows[-1]["request_json"]).get("end_ns") == end_ns)
    ) or (end_ns is None and day in pending_dates(config, utc_today())):
        api = GrvtAPI(config, deadline=deadline)
        try:
            discovered = api.instruments(active=True)
            instruments = day_universe(config.data_dir, day, discovered)
            if not config.depth_enabled:
                # The depth service used to retain discovery. Tick-only runs
                # own this metadata boundary before any instrument downloads.
                persist_discovery(config.data_dir, day, discovered)
            report = download_trades(
                api, config.data_dir, day, instruments, end_ns=end_ns
            )
        finally:
            api.close()
        seal_revision(
            config,
            day,
            report,
            deadline=deadline,
            allow_today=end_ns is not None,
            end_ns=end_ns,
        )
        row = revisions(config, day)[-1]
        publish_revision(config, day, row["revision"], deadline=deadline, github=github)
        incomplete |= not report["complete"]
    prune_verified(config, datetime.now(UTC), deadline=deadline, github=github)
    return int(incomplete)


def _subreaper():
    # Linux deployment: adopt orphaned gh grandchildren so group cleanup can reap
    # them even on hosts whose PID 1 does not promptly reap (including containers).
    if sys.platform == "linux":
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            raise OSError(ctypes.get_errno(), "cannot enable child subreaper")


def _signal_group(pgid, sig):
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return False


def _reap_group(pgid):
    while True:
        try:
            pid, _ = os.waitpid(-pgid, os.WNOHANG)
            if pid == 0:
                return
        except ChildProcessError:
            return


def stop_group(proc, grace=5):
    _signal_group(proc.pid, signal.SIGTERM)
    until = time.monotonic() + grace
    while time.monotonic() < until:
        if proc.poll() is not None:
            _reap_group(proc.pid)
            if not _signal_group(proc.pid, 0):
                return
        time.sleep(0.02)
    _signal_group(proc.pid, signal.SIGKILL)
    proc.wait()
    # All members have received KILL; reap adopted descendants before advancing.
    while _signal_group(proc.pid, 0):
        _reap_group(proc.pid)
        time.sleep(0.01)


def supervise_date(
    config, day, fd, *, end_ns=None, publish_only=False, grace=5, command=None
):
    _subreaper()
    deadline = time.monotonic() + config.per_date_deadline_seconds
    read_fd, write_fd = os.pipe()
    payload = asdict(config)
    payload["data_dir"] = str(config.data_dir)
    env = dict(
        os.environ, GRVT_PUBLICATION_LOCK_FD=str(fd), GRVT_WORKER_GATE_FD=str(read_fd)
    )
    args = (command or [sys.executable, "-m", "grvt_data.cli", "_worker"]) + [
        json.dumps(payload),
        str(day),
        str(deadline),
        str(end_ns) if end_ns is not None else "-",
        str(int(publish_only)),
    ]
    proc = None
    registration = None
    try:
        proc = subprocess.Popen(
            args, env=env, start_new_session=True, pass_fds=(fd, read_fd)
        )
        os.close(read_fd)
        read_fd = None
        registry = os.environ.get("GRVT_WORKER_REGISTRY")
        if registry:
            registration = Path(registry) / str(proc.pid)
            # Worker cannot perform work until registration is durable. EOF before
            # this point makes the worker exit after a parent crash.
            write_json_atomic(registration, {"pgid": proc.pid})
        os.write(write_fd, b"1")
        os.close(write_fd)
        write_fd = None
        try:
            code = proc.wait(timeout=max(0.001, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            return 124
        return code
    finally:
        for pipe in (read_fd, write_fd):
            if pipe is not None:
                os.close(pipe)
        # A signal during cleanup must not bypass descendant KILL/reaping.
        blocked = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT}
        )
        try:
            if proc is not None:
                stop_group(proc, grace)
            if registration is not None:
                registration.unlink(missing_ok=True)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, blocked)


@contextlib.contextmanager
def interrupted_by_signal():
    def interrupt(signum, frame):
        # One interruption initiates bounded cleanup; repeated TERM must not
        # interrupt cleanup before KILL/reaping and durable outcome recording.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise InterruptedError(f"signal {signum}")

    previous = {s: signal.signal(s, interrupt) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def daily(
    config,
    *,
    lock_seconds=60,
    allow_today=False,
    end_ns=None,
    day=None,
    publish_only=False,
):
    with publication_lock(config, lock_seconds) as fd:
        if fd is None:
            return 75
        recover_date_runs(config)
        today = utc_today()
        pending = pending_dates(config, today)
        if allow_today:
            if pending:
                raise ValueError(
                    "drain pending dates with plain daily before --allow-today"
                )
            start, end = window_bounds(today)
            if end_ns is None or not start < end_ns <= min(end, time.time_ns()):
                raise ValueError(
                    "--end-time-ns must be within today and not in the future"
                )
            if config.depth_enabled:
                receipt = json.loads(
                    (config.data_dir / "state/depth-checkpoint.json").read_text()
                )
                if receipt["cutoff_received_ns"] != end_ns:
                    raise ValueError(
                        "--end-time-ns must match persisted rotate-now receipt"
                    )
            targets = [today]
        elif end_ns is not None:
            raise ValueError("--end-time-ns requires --allow-today")
        elif publish_only:
            targets = [day]
        else:
            yesterday = today - timedelta(days=1)
            targets = sorted(pending, key=lambda d: (d != yesterday, d))
        result = 0
        with interrupted_by_signal():
            for target in targets:
                attempt = start_date_run(config, target)
                outcome, error = "failed", None
                try:
                    code = supervise_date(
                        config, target, fd, end_ns=end_ns, publish_only=publish_only
                    )
                    outcome = (
                        "deadline_exceeded"
                        if code == 124
                        else ("failed" if code else "completed")
                    )
                    if code:
                        error = f"worker exit {code}"
                        result = 1
                except InterruptedError as exc:
                    outcome, error = "interrupted", str(exc)
                    raise
                except Exception as exc:  # noqa: BLE001 — isolate date failures
                    error = f"{type(exc).__name__}: {exc}"
                    result = 1
                    print(f"{target}: {error}", file=sys.stderr, flush=True)
                finally:
                    finish_date_run(config, target, attempt, outcome, error)
        return result


def _local_rows(config, *, attempts=False):
    path = config.data_dir / "state/publication.sqlite"
    if not path.exists():
        return []
    db = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        return [
            dict(r)
            for r in db.execute(
                "SELECT * FROM per_date_runs"
                if attempts
                else "SELECT day,revision,status,remote_verified,pruned FROM revisions"
            )
        ]
    finally:
        db.close()


def status(config, *, check=False):
    now = time.time_ns()
    heartbeat_path = config.data_dir / "state/depth-status.json"
    heartbeat = (
        json.loads(heartbeat_path.read_text())
        if config.depth_enabled and heartbeat_path.exists()
        else {}
    )
    age = (now - heartbeat["received_ns"]) / 1e9 if heartbeat else None
    disk = disk_status(config.data_dir, config)
    result = {
        "observed_at_ns": now,
        "collector": {
            "live": age is not None and 0 <= age < 60,
            "heartbeat_age_seconds": age,
            "liveness_source": "apply-loop heartbeat",
            "websocket_ping_interval_seconds": 20,
            "websocket_ping_timeout_seconds": 20,
            "instruments": heartbeat.get("instruments", {}),
        },
        "datasets": list(config.datasets),
        "disk": disk,
        "revisions": _local_rows(config),
        "date_runs": _local_rows(config, attempts=True),
        "retry_queue": retry_status(config),
        "releases": [
            json.loads(p.read_text())
            for p in sorted((config.data_dir / "state/releases").glob("*.json"))
        ],
        "release_asset_cap": config.release_asset_cap,
        "release_counts_source": "last remote observation; absent means unknown",
        "cleanup": "not requested",
    }
    if not config.depth_enabled:
        result["collector"] = {
            "enabled": False,
            "status": "disabled",
            "reason": "tick-only datasets; no depth collector required",
        }
    else:
        result["collector"]["enabled"] = True
    if check and disk["cleanup_requested"]:
        with publication_lock(config, 0) as fd:
            if fd is None:
                result["cleanup"] = "skipped: publication lock held"
            else:
                # Network/mutation is limited to the explicit critical cleanup
                # branch; ordinary status and ordinary checks are local-only.
                recover_date_runs(config)
                try:
                    removed = prune_if_critical(
                        config,
                        datetime.now(UTC),
                        deadline=time.monotonic() + 60,
                        github=ObservedGitHub(config, deadline=time.monotonic() + 60),
                    )
                    result["cleanup"] = [str(path) for path in removed]
                except Exception as exc:  # noqa: BLE001 — persist cleanup failure in health snapshot
                    result["cleanup"] = f"failed: {exc}"
    if check:
        write_json_atomic(config.data_dir / "state/status-health.json", result)
        path = config.data_dir / "logs/status.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as stream:
            stream.write(json.dumps(result, sort_keys=True) + "\n")
    return result


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "_worker":
        gate = int(os.environ["GRVT_WORKER_GATE_FD"])
        try:
            if os.read(gate, 1) != b"1":
                return 1
        finally:
            os.close(gate)
        os.fstat(int(os.environ["GRVT_PUBLICATION_LOCK_FD"]))
        settings = json.loads(argv[1])
        settings["data_dir"] = Path(settings["data_dir"])
        try:
            return run_date(
                Config(**settings),
                date.fromisoformat(argv[2]),
                deadline=float(argv[3]),
                end_ns=None if argv[4] == "-" else int(argv[4]),
                publish_only=bool(int(argv[5])),
            )
        except DeadlineExceeded:
            return 124
        except Exception:
            logging.getLogger(__name__).exception("date worker failed")
            return 1
    parser = argparse.ArgumentParser(prog="grvt-data")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("depth", "daily", "publish", "rotate-now", "status"):
        command = commands.add_parser(name)
        command.add_argument("--data-dir", type=Path, default=DEFAULT_ROOT)
        command.add_argument(
            "--datasets",
            choices=("trades", "trades,depth"),
            default="trades,depth" if name in ("depth", "rotate-now") else "trades",
        )
        if name in ("daily", "publish"):
            command.add_argument("--repo", default=Config.repo)
            command.add_argument("--lock-wait-seconds", type=positive, default=60)
            command.add_argument(
                "--per-date-deadline-seconds", type=positive, default=3600
            )
        if name == "daily":
            command.add_argument("--allow-today", action="store_true")
            command.add_argument("--end-time-ns", type=int64)
        if name == "publish":
            command.add_argument("--date", type=date.fromisoformat, required=True)
        if name == "status":
            command.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    config = Config(
        data_dir=args.data_dir.resolve(),
        datasets=tuple(args.datasets.split(",")),
        repo=getattr(args, "repo", Config.repo),
        per_date_deadline_seconds=getattr(args, "per_date_deadline_seconds", 3600),
    )
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        if args.command == "depth":
            if not config.depth_enabled:
                raise ValueError("depth command requires --datasets trades,depth")
            asyncio.run(collect_depth(config))
        elif args.command == "rotate-now":
            if not config.depth_enabled:
                raise ValueError("rotate-now requires --datasets trades,depth")
            receipt = asyncio.run(depth_control(config.data_dir, "rotate_now"))
            print(json.dumps(receipt, sort_keys=True))
            print(receipt["cutoff_received_ns"])
        elif args.command == "status":
            snapshot = status(config, check=args.check)
            if not args.check:
                print(json.dumps(snapshot, sort_keys=True))
        else:
            return daily(
                config,
                lock_seconds=args.lock_wait_seconds,
                allow_today=getattr(args, "allow_today", False),
                end_ns=getattr(args, "end_time_ns", None),
                day=getattr(args, "date", None),
                publish_only=args.command == "publish",
            )
    except (ValueError, OSError, InterruptedError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
