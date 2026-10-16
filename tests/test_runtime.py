"""Execute deployment scripts against fake host tools; never install anything."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def executable(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    path.chmod(0o755)
    return path


def test_wrapper_timeout_kills_separate_worker_and_descendant_groups(tmp_path):
    deploy = tmp_path / "deployment"
    (deploy / ".venv/bin").mkdir(parents=True)
    executable(
        deploy / ".venv/bin/python", f'#!/bin/bash\nexec "{sys.executable}" "$@"\n'
    )
    data = tmp_path / "data"
    # Invoke the actual CLI worker supervisor, preserving registration/gating and
    # its actual lock. Simulate an unresponsive parent and TERM-resistant sealer.
    worker = """
import sys,os,signal,subprocess,time
from pathlib import Path
signal.signal(signal.SIGTERM, lambda *a: Path(os.environ['GRVT_DATA_DIR'], 'worker-term').write_text('term'))
child=subprocess.Popen([sys.executable,'-c', 'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'],pass_fds=(int(os.environ['GRVT_PUBLICATION_LOCK_FD']),))
Path(os.environ['GRVT_DATA_DIR'], 'descendant').write_text(str(child.pid))
Path(os.environ['GRVT_DATA_DIR'], 'worker').write_text(str(os.getpid()))
while True: time.sleep(.02)
"""
    # Custom command consumes the normal gate before entering the test sealer.
    bootstrap = (
        "import os; fd=int(os.environ['GRVT_WORKER_GATE_FD']); assert os.read(fd,1)==b'1'; os.close(fd);\n"
        + worker
    )
    executable(
        deploy / ".venv/bin/grvt-data",
        f"""#!{sys.executable}
import os,signal,sys
from pathlib import Path
from datetime import date
from grvt_data import cli
from grvt_data.config import Config
signal.signal(signal.SIGTERM,signal.SIG_IGN)
config=Config(data_dir=Path(os.environ['GRVT_DATA_DIR']))
with cli.publication_lock(config,0) as fd:
    cli.supervise_date(config,date(2026,10,15),fd,command=[sys.executable,'-c',{bootstrap!r}])
""",
    )
    env = dict(
        os.environ,
        PYTHONPATH=str(ROOT / "src"),
        GRVT_DEPLOY_ROOT=str(deploy),
        GRVT_DATA_DIR=str(data),
        GRVT_RUN_TIMEOUT="1s",
        GRVT_KILL_AFTER="2s",
        GRVT_WRAPPER_GRACE_SECONDS=".15",
    )
    started = time.monotonic()
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/run-daily.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
        check=False,
    )
    assert result.returncode == 124, result.stderr
    assert time.monotonic() - started < 3
    assert (data / "worker-term").exists(), "TERM must reach isolated group before KILL"
    for name in ("worker", "descendant"):
        pid = int((data / name).read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    from grvt_data.cli import publication_lock
    from grvt_data.config import Config

    with publication_lock(Config(data_dir=data), 0) as fd:
        assert fd is not None


def installer(tmp_path, *, hostname="vitalik8", zone="UTC", old=""):
    fake = tmp_path / "bin"
    fake.mkdir()
    executable(fake / "hostname", f"#!/bin/bash\necho {hostname}\n")
    executable(
        fake / "timedatectl",
        f'#!/bin/bash\n[[ "$*" == "show -p Timezone --value" ]] || exit 2\necho {zone}\n',
    )
    crontab = tmp_path / "crontab"
    executable(fake / "logrotate", "#!/bin/bash\nexit 0\n")
    crontab.write_text(old)
    executable(
        fake / "crontab",
        f"""#!/bin/bash
if [[ $1 == -l ]]; then cat {str(crontab)!r}; else cp "$1" {str(crontab)!r}; fi
""",
    )
    deploy = tmp_path / "deployment"
    data = tmp_path / "data"
    executable(deploy / ".venv/bin/grvt-data", "#!/bin/bash\nexit 0\n")
    executable(deploy / "scripts/run-daily.sh", "#!/bin/bash\nexit 0\n")
    (deploy / "scripts/grvt-data.logrotate").write_text(
        (ROOT / "scripts/grvt-data.logrotate").read_text()
    )
    # Substitute only the fixed deployment roots. Host checks, cron merge and
    # backup logic are the exact production script, with mock live host tools.
    source = (
        (ROOT / "scripts/install-vitalik8.sh")
        .read_text()
        .replace("/home/avik/GRVT-Historical-Data", str(deploy))
        .replace("/home/avik/.local/share/grvt-historical-data", str(data))
        .replace("/usr/sbin/logrotate", str(fake / "logrotate"))
    )
    script = executable(tmp_path / "install.sh", source)
    env = dict(os.environ, PATH=str(fake) + ":" + os.environ["PATH"])
    return script, env, crontab, data


def test_installer_preserves_unrelated_cron_backs_up_and_replaces_idempotently(
    tmp_path,
):
    old = "MAILTO=operator@example.com\n7 4 * * * echo unrelated\n# BEGIN grvt-data-daily\nstale\n# END grvt-data-daily\n"
    script, env, crontab, data = installer(tmp_path, old=old)
    first = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, check=False
    )
    assert first.returncode == 0, first.stderr
    installed = crontab.read_text()
    assert installed.endswith("MAILTO=operator@example.com\n7 4 * * * echo unrelated\n")
    assert "stale" not in installed
    assert installed.count("# BEGIN grvt-data-daily") == 1
    assert "30 0 * * * " in installed and "*/5 * * * * " in installed
    assert "CRON_TZ" not in installed and "TZ=" not in installed
    assert "flock" not in installed
    assert next((data / "state").glob("crontab-*.backup")).read_text() == old
    second = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, check=False
    )
    assert second.returncode == 0, second.stderr
    assert crontab.read_text() == installed
    assert len(list((data / "state").glob("crontab-*.backup"))) == 2


def test_installer_user_logrotate_is_scoped_and_idempotent(tmp_path):
    old = "7 4 * * * echo unrelated\n"
    script, env, crontab, data = installer(tmp_path, old=old)
    for _ in range(2):
        result = subprocess.run(
            ["bash", str(script), "--user-logrotate"],
            env=env, capture_output=True, text=True, check=False,
        )
        assert result.returncode == 0, result.stderr
    installed = crontab.read_text()
    assert installed.endswith(old)
    assert installed.count("45 0 * * * ") == 1
    assert f"--state {data}/state/logrotate.status {data}/state/grvt-data.logrotate" in installed
    config = data / "state/grvt-data.logrotate"
    assert config.stat().st_mode & 0o777 == 0o600
    assert config.read_text() == (ROOT / "scripts/grvt-data.logrotate").read_text()
    assert "weekly" in config.read_text() and "rotate 8" in config.read_text()
    assert "compress" in config.read_text()


@pytest.mark.parametrize(
    ("host", "zone"), [("wrong-host", "UTC"), ("vitalik8", "Europe/London")]
)
def test_installer_refuses_live_wrong_host_or_timezone(tmp_path, host, zone):
    script, env, crontab, data = installer(
        tmp_path, hostname=host, zone=zone, old="keep me\n"
    )
    result = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert "refusing:" in result.stderr
    assert crontab.read_text() == "keep me\n"
    assert not data.exists()


def test_installer_refuses_malformed_existing_block(tmp_path):
    script, env, crontab, _data = installer(
        tmp_path, old="keep me\n# BEGIN grvt-data-daily\n"
    )
    result = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode != 0
    assert crontab.read_text() == "keep me\n# BEGIN grvt-data-daily\n"


def test_compose_only_mounts_data_and_no_github_credentials():
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "docker/compose.yml"),
            "config",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)["services"]["depth"]
    assert [v["target"] for v in config["volumes"]] == ["/data"]
    assert config["restart"] == "unless-stopped"
    assert config["user"] == "1000:1000"
    assert not config.get("environment")
    assert config["stop_grace_period"] == "1m0s"
    script = (ROOT / "scripts/run-daily.sh").read_text()
    assert " publish " not in script and "flock" not in script


def test_outer_runtime_propagates_normal_exit():
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "grvt_data.runtime",
            sys.executable,
            "-c",
            "import sys; sys.exit(7)",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=3,
    )
    assert result.returncode == 7


def test_installer_keeps_our_utc_block_before_unrelated_cron_timezone(tmp_path):
    old = "CRON_TZ=America/New_York\n10 9 * * * run-other-job\n"
    script, env, crontab, _data = installer(tmp_path, old=old)
    result = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    installed = crontab.read_text()
    assert installed.startswith("# BEGIN grvt-data-daily\n")
    assert installed.endswith(old)
    assert installed.count("CRON_TZ=") == 1
