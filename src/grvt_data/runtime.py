"""Outer cron safety supervisor; timeout alone cannot reach isolated workers."""

import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from .cli import _reap_group, _signal_group, _subreaper


def supervise(command, *, grace=25):
    _subreaper()
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    previous = {s: signal.signal(s, stop) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        with tempfile.TemporaryDirectory(prefix="grvt-workers-") as directory:
            env = dict(os.environ, GRVT_WORKER_REGISTRY=directory)
            proc = subprocess.Popen(command, env=env, start_new_session=True)
            while proc.poll() is None and not stopping:
                time.sleep(0.05)
            # Include workers registered after the initial signal but before the
            # CLI reacts. Its gate prohibits unregistered workers doing work.
            until = time.monotonic() + grace
            signaled = set()
            while True:
                groups = {
                    int(p.name) for p in Path(directory).iterdir() if p.name.isdigit()
                }
                groups.add(proc.pid)
                for pgid in groups - signaled:
                    _signal_group(pgid, signal.SIGTERM)
                    signaled.add(pgid)
                proc.poll()
                for pgid in signaled - {proc.pid}:
                    _reap_group(pgid)
                alive = {pgid for pgid in signaled if _signal_group(pgid, 0)}
                if not alive:
                    break
                if time.monotonic() >= until:
                    for pgid in alive:
                        _signal_group(pgid, signal.SIGKILL)
                    break
                time.sleep(0.02)
            proc.wait()
            # Reap descendants adopted on CLI death, including TERM-resistant
            # gh children in the independently registered worker groups.
            for pgid in signaled - {proc.pid}:
                while _signal_group(pgid, 0):
                    _reap_group(pgid)
                    time.sleep(0.01)
            return 143 if stopping else proc.returncode
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    return supervise(
        sys.argv[1:], grace=float(os.environ.get("GRVT_WRAPPER_GRACE_SECONDS", "25"))
    )


if __name__ == "__main__":
    sys.exit(main())
