import copy
import hashlib
import json
from datetime import UTC, datetime

import pytest
from test_archive import DAY, report, segment

from grvt_data.archive import seal_revision
from grvt_data.config import Config
from grvt_data.publish import GitHub, prune_verified, publish_revision
from grvt_data.storage import publication_db


class Crash(BaseException):
    pass


def test_gh_finds_draft_before_tag_exists_and_reads_all_assets(tmp_path, monkeypatch):
    gh = GitHub(Config(data_dir=tmp_path))
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if "/releases/tags/" in args[1]:
            assert kwargs["absent"]
            return None  # Live GitHub returns 404 until a draft's tag exists.
        if args[1].endswith("/releases?per_page=100"):
            assert "--paginate" in args and "--slurp" in args
            return [
                [{"id": 1, "tag_name": "unrelated"}],
                [{"id": 42, "tag_name": f"data-{DAY}", "draft": True}],
            ]
        assert args[1].endswith("/releases/42/assets?per_page=100")
        return [[{"name": "part1"}], [{"name": "part2"}]]

    monkeypatch.setattr(gh, "_run", run)
    release = gh.release(DAY)
    assert release["id"] == 42 and release["draft"]
    assert [a["name"] for a in release["assets"]] == ["part1", "part2"]
    assert len(calls) == 3


class FakeGitHub:
    def __init__(self, config):
        self.config = config
        self.state = None
        self.calls = []
        self.bytes = {}
        self.next_id = 1
        self.crash = None
        self.bad_upload = False
        self.on_patch = lambda **kw: None

    def release(self, day):
        self.calls.append(("read",))
        return copy.deepcopy(self.state)

    def create(self, day):
        self.calls.append(("create",))
        assert self.state is None
        self.state = {
            "id": 100,
            "tag_name": f"data-{day}",
            "draft": True,
            "body": "",
            "assets": [],
        }
        return copy.deepcopy(self.state)

    def upload(self, release, path):
        self.calls.append(("upload", path.name))
        assert (path.parent / "journal.json").exists()
        payload = path.read_bytes()
        assert not any(a["name"] == path.name for a in self.state["assets"])
        asset = {"id": self.next_id, "name": path.name, "size": len(payload)}
        self.next_id += 1
        self.bytes[asset["id"]] = b"broken!" if self.bad_upload else payload
        self.bad_upload = False
        self.state["assets"].append(asset)
        if self.crash == "upload":
            self.crash = None
            raise Crash()

    def hash(self, asset):
        self.calls.append(("verify", asset["name"]))
        return hashlib.sha256(self.bytes[asset["id"]]).hexdigest()

    def patch(self, release, **kw):
        self.on_patch(**kw)
        self.calls.append(("patch", kw))
        self.state.update(kw)
        if self.crash == "patch" or (
            self.crash == "publish" and kw.get("draft") is False
        ):
            self.crash = None
            raise Crash()

    def delete(self, asset):
        self.calls.append(("delete", asset["name"]))
        self.state["assets"] = [
            a for a in self.state["assets"] if a["id"] != asset["id"]
        ]
        del self.bytes[asset["id"]]


def status(config, rev=1):
    db = publication_db(config)
    try:
        return db.execute(
            "SELECT status FROM revisions WHERE revision=?", (rev,)
        ).fetchone()[0]
    finally:
        db.close()


@pytest.fixture
def sealed(tmp_path):
    config = Config(data_dir=tmp_path)
    segment(config)
    seal_revision(config, DAY, report())
    return config, FakeGitHub(config)


def test_revision_1_committed_only_once_public(sealed):
    config, gh = sealed

    def check(**kw):
        assert status(config) == "sealed"
        if "draft" in kw:
            assert gh.state["draft"] and "manifest-rev01.json" in gh.state["body"]
        else:
            assert gh.state["draft"]
        uploads = [c[1] for c in gh.calls if c[0] == "upload"]
        verified = [c[1] for c in gh.calls if c[0] == "verify"]
        assert set(uploads) == set(verified)

    gh.on_patch = check
    assert publish_revision(config, DAY, 1, github=gh)["status"] == "committed"
    assert status(config) == "committed" and not gh.state["draft"]


def test_retry_after_draft_creation_without_tag_reuses_actual_release(
    sealed, monkeypatch
):
    config, gh = sealed
    gh.base = f"repos/{config.repo}"
    original_create = gh.create

    def create_and_crash(day):
        original_create(day)
        raise Crash()

    def run(args, **kwargs):
        if "/releases/tags/" in args[1]:
            return (
                copy.deepcopy(gh.state) if gh.state and not gh.state["draft"] else None
            )
        if args[1].endswith("/releases?per_page=100"):
            return [[], [copy.deepcopy(gh.state)] if gh.state else []]
        assert args[1].endswith("/releases/100/assets?per_page=100")
        return [copy.deepcopy(gh.state["assets"])]

    monkeypatch.setattr(gh, "_run", run, raising=False)
    monkeypatch.setattr(gh, "release", lambda day: GitHub.release(gh, day))
    monkeypatch.setattr(gh, "create", create_and_crash)
    with pytest.raises(Crash):
        publish_revision(config, DAY, 1, github=gh)
    assert gh.state["id"] == 100 and gh.state["draft"]
    monkeypatch.setattr(gh, "create", original_create)
    assert publish_revision(config, DAY, 1, github=gh)["status"] == "committed"
    assert gh.state["id"] == 100
    assert sum(call[0] == "create" for call in gh.calls) == 1


@pytest.mark.parametrize("ambiguous", [False, True])
def test_release_list_error_or_ambiguity_never_creates(sealed, monkeypatch, ambiguous):
    config, gh = sealed
    gh.base = f"repos/{config.repo}"

    def run(args, **kwargs):
        if "/releases/tags/" in args[1]:
            return None
        if not ambiguous:
            raise RuntimeError("release list authentication/rate limit failure")
        return [
            [{"id": 1, "tag_name": f"data-{DAY}", "draft": True}],
            [{"id": 2, "tag_name": f"data-{DAY}", "draft": False}],
        ]

    monkeypatch.setattr(gh, "_run", run, raising=False)
    monkeypatch.setattr(gh, "release", lambda day: GitHub.release(gh, day))
    with pytest.raises((RuntimeError, ValueError), match="failure|multiple releases"):
        publish_revision(config, DAY, 1, github=gh)
    assert gh.state is None and status(config) == "sealed"
    assert not [call for call in gh.calls if call[0] == "create"]


def test_revision_2_public_body_advances_only_after_all_hashes(sealed):
    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    old_body = gh.state["body"]
    seal_revision(config, DAY, report())
    gh.calls.clear()

    def check(**kw):
        assert gh.state["body"] == old_body
        assert status(config, 2) == "sealed"
        names = {a["name"] for a in gh.state["assets"]} - {"manifest-rev01.json"}
        assert names == {c[1] for c in gh.calls if c[0] == "verify"}

    gh.on_patch = check
    publish_revision(config, DAY, 2, github=gh)
    assert "manifest-rev02.json" in gh.state["body"]
    assert not [c for c in gh.calls if c[0] in ("delete", "create")]


@pytest.mark.parametrize("stage", ["upload", "publish"])
def test_revision1_remote_crash_reconciles_without_second_draft(sealed, stage):
    config, gh = sealed
    gh.crash = stage
    with pytest.raises(Crash):
        publish_revision(config, DAY, 1, github=gh)
    assert status(config) == "sealed"
    gh.calls.clear()
    publish_revision(config, DAY, 1, github=gh)
    assert status(config) == "committed"
    assert not [c for c in gh.calls if c[0] == "create"]
    if stage == "publish":
        assert not [c for c in gh.calls if c[0] in ("upload", "patch")]


@pytest.mark.parametrize("stage", ["upload", "patch"])
def test_revision2_crash_keeps_revision1_or_reconciles_remote_commit(sealed, stage):
    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    old_body = gh.state["body"]
    seal_revision(config, DAY, report())
    gh.crash = stage
    with pytest.raises(Crash):
        publish_revision(config, DAY, 2, github=gh)
    if stage == "upload":
        assert gh.state["body"] == old_body
    else:
        assert "manifest-rev02.json" in gh.state["body"]
    uploaded = {a["name"] for a in gh.state["assets"]}
    gh.calls.clear()
    publish_revision(config, DAY, 2, github=gh)
    assert not [c for c in gh.calls if c[0] == "create"]
    assert not [c for c in gh.calls if c[0] == "upload" and c[1] in uploaded]
    if stage == "patch":
        assert not [c for c in gh.calls if c[0] == "patch"]


def test_hash_mismatch_unreferenced_orphan_repairs_only_one(sealed):
    config, gh = sealed
    gh.bad_upload = True
    publish_revision(config, DAY, 1, github=gh)
    deletes = [c[1] for c in gh.calls if c[0] == "delete"]
    assert len(deletes) == 1
    assert sum(c == ("upload", deletes[0]) for c in gh.calls) == 2


def test_hash_mismatch_after_public_commit_never_repairs(sealed):
    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    gh.bytes[gh.state["assets"][0]["id"]] = b"corrupt"
    gh.calls.clear()
    with pytest.raises(ValueError, match="immutable"):
        publish_revision(config, DAY, 1, github=gh)
    assert not [c for c in gh.calls if c[0] in ("upload", "delete", "patch")]


def test_older_referenced_asset_never_deleted_during_correction(sealed):
    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    old_names = {a["name"] for a in gh.state["assets"]}
    gh.bytes[gh.state["assets"][0]["id"]] = b"corrupt"
    seal_revision(config, DAY, report())
    gh.calls.clear()
    with pytest.raises(ValueError, match="immutable"):
        publish_revision(config, DAY, 2, github=gh)
    assert not [c for c in gh.calls if c[0] == "delete" and c[1] in old_names]


def test_asset_cap_fails_without_remote_mutation_or_cleanup(sealed):
    config, gh = sealed
    from dataclasses import replace

    with pytest.raises(ValueError, match="cap"):
        publish_revision(replace(config, release_asset_cap=1), DAY, 1, github=gh)
    assert gh.state is None
    assert (config.data_dir / "staging" / str(DAY) / "rev01" / "journal.json").exists()


def test_pruning_retains_open_unverified_and_small_state(sealed):
    config, gh = sealed
    active = config.data_dir / "raw" / "depth" / str(DAY) / "active.open"
    active.write_bytes(b"live")
    state = config.data_dir / "state" / "checkpoint.json"
    state.write_text("{}")
    assert prune_verified(config, datetime.now(UTC), github=gh) == []
    publish_revision(config, DAY, 1, github=gh)
    unreserved = segment(config, index=10)
    assert prune_verified(config, datetime.now(UTC), github=gh)
    assert active.exists() and state.exists() and unreserved.exists()
    assert (config.data_dir / "state" / "publication.sqlite").exists()
    assert not (config.data_dir / "staging" / str(DAY) / "rev01").exists()
    seal_revision(config, DAY, report())
    assert prune_verified(config, datetime.now(UTC), github=gh) == []
    assert unreserved.exists()


def test_prune_requires_current_public_body(sealed):
    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    gh.state["draft"] = True
    with pytest.raises(ValueError, match="public"):
        prune_verified(config, datetime.now(UTC), github=gh)
    assert (config.data_dir / "staging" / str(DAY) / "rev01").exists()


def test_retry_old_committed_revision_never_rolls_pointer_back(sealed):
    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    seal_revision(config, DAY, report())
    publish_revision(config, DAY, 2, github=gh)
    gh.calls.clear()
    publish_revision(config, DAY, 1, github=gh)
    assert "manifest-rev02.json" in gh.state["body"]
    assert not [c for c in gh.calls if c[0] == "patch"]


def test_unknown_public_pointer_fails_closed(sealed):
    config, gh = sealed
    gh.create(DAY)
    gh.state.update(draft=False, body="foreign release")
    with pytest.raises(ValueError, match="unknown body"):
        publish_revision(config, DAY, 1, github=gh)
    assert not [c for c in gh.calls if c[0] in ("upload", "delete", "patch")]


def test_digest_verification_needs_no_download(tmp_path, monkeypatch):
    gh = GitHub(Config(data_dir=tmp_path))
    monkeypatch.setattr(
        "grvt_data.publish.subprocess.Popen",
        lambda *a, **k: pytest.fail("digest should avoid download"),
    )
    assert gh.hash({"digest": "sha256:" + "a" * 64}) == "a" * 64


def test_stream_hash_without_digest_has_no_second_copy(tmp_path, monkeypatch):
    import subprocess
    import sys

    original = subprocess.Popen
    payload = b"large-binary\x00data" * 100000

    def popen(args, **kwargs):
        assert args[:2] == ["gh", "api"]
        return original(
            [
                sys.executable,
                "-c",
                "import sys;sys.stdout.buffer.write(b'large-binary\\x00data'*100000)",
            ],
            **kwargs,
        )

    monkeypatch.setattr("grvt_data.publish.subprocess.Popen", popen)
    assert (
        GitHub(Config(data_dir=tmp_path)).hash({"id": 2})
        == hashlib.sha256(payload).hexdigest()
    )
    assert not list(tmp_path.iterdir())


def test_publisher_checks_sealed_local_bytes_before_remote_mutation(sealed):
    config, gh = sealed
    path = config.data_dir / "staging" / str(DAY) / "rev01" / "manifest-rev01.json"
    path.write_text("bad")
    with pytest.raises(ValueError, match="sealed byte mismatch"):
        publish_revision(config, DAY, 1, github=gh)
    assert gh.state is None


def test_critical_disk_triggers_early_verified_prune(sealed, monkeypatch):
    from grvt_data.publish import prune_if_critical

    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    monkeypatch.setattr(
        "grvt_data.storage.disk_status", lambda *a: {"cleanup_requested": True}
    )
    assert prune_if_critical(config, datetime.now(UTC), github=gh)
    assert not (config.data_dir / "staging" / str(DAY) / "rev01").exists()


@pytest.mark.parametrize("cap", [8192, 16384])
def test_superset_and_repartition_corrections_leave_old_remote_bytes_untouched(
    tmp_path, cap
):
    import os

    from test_archive import trade_report

    from grvt_data.storage import TRADE_SCHEMA, write_parquet_atomic

    config = Config(data_dir=tmp_path, archive_part_bytes=8192)
    gh = FakeGitHub(config)
    first = trade_report(config)
    seal_revision(config, DAY, first)
    publish_revision(config, DAY, 1, github=gh)
    old_assets = copy.deepcopy(gh.state["assets"])
    old_bytes = copy.deepcopy(gh.bytes)
    from dataclasses import replace

    revised = trade_report(config)
    path = revised["instruments"]["BTC_USDT_Perp"]["path"]
    from pathlib import Path

    rows = [
        {
            "instrument": "BTC_USDT_Perp",
            "trade_id": str(i),
            "event_time": first["start_ns"] + i,
            "price": os.urandom(80).hex(),
            "size": "1",
            "venue": "ORDERBOOK",
            "is_taker_buyer": True,
        }
        for i in range(500)
    ]
    write_parquet_atomic(Path(path), rows, TRADE_SCHEMA)
    revised["instruments"]["BTC_USDT_Perp"]["rows"] = 500
    config = replace(config, archive_part_bytes=cap)
    bundle = seal_revision(config, DAY, revised)
    gh.calls.clear()
    publish_revision(config, DAY, 2, github=gh)
    assert all(a in gh.state["assets"] for a in old_assets)
    assert all(gh.bytes[id] == value for id, value in old_bytes.items())
    assert not [c for c in gh.calls if c[0] == "delete"]
    manifest = json.loads((bundle / "manifest-rev02.json").read_text())
    assert all(
        o["superseded_by"] == 2
        for a in manifest["trades"]["assets"]
        if a["revision"] == 1
        for o in a["instrument_coverage"]
    )
    assert len([a for a in manifest["trades"]["assets"] if a["revision"] == 2]) > 1


def test_unfinished_revision_protects_reused_trade_input_path(sealed):
    from test_archive import trade_report

    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    second = trade_report(config)
    seal_revision(config, DAY, second)
    # Commit revision2, then seal revision3 reusing the same Task1 window path.
    publish_revision(config, DAY, 2, github=gh)
    third = trade_report(config)
    seal_revision(config, DAY, third)
    path = third["instruments"]["BTC_USDT_Perp"]["path"]
    from pathlib import Path

    assert prune_verified(config, datetime.now(UTC), github=gh)
    assert Path(path).exists()
    assert (config.data_dir / "staging" / str(DAY) / "rev03" / "journal.json").exists()


def test_hash_stream_deadline_kills_trickling_process(tmp_path, monkeypatch):
    import subprocess
    import sys
    import time

    original = subprocess.Popen
    processes = []

    def popen(args, **kw):
        process = original(
            [
                sys.executable,
                "-c",
                "import sys,time\nwhile True:\n sys.stdout.buffer.write(b'x');sys.stdout.flush();time.sleep(.01)",
            ],
            **kw,
        )
        processes.append(process)
        return process

    monkeypatch.setattr("grvt_data.publish.subprocess.Popen", popen)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        GitHub(Config(data_dir=tmp_path), deadline=started + 0.1).hash({"id": 1})
    assert time.monotonic() - started < 1
    assert processes[0].poll() is not None


def test_gh_json_payload_preserves_newlines_and_inherited_lock(tmp_path, monkeypatch):
    import os
    import subprocess

    seen = []
    descriptor = os.open(tmp_path / "lock", os.O_CREAT | os.O_RDWR, 0o600)
    monkeypatch.setenv("GRVT_PUBLICATION_LOCK_FD", str(descriptor))

    def run(args, **kw):
        assert kw["pass_fds"] == (descriptor,)
        assert kw["timeout"] <= 0.9
        path = args[args.index("--input") + 1]
        with open(path) as stream:
            seen.append(json.load(stream))
        assert args[:2] == ["gh", "api"]
        return subprocess.CompletedProcess(args, 0, b"{}", b"")

    monkeypatch.setattr("grvt_data.publish.subprocess.run", run)
    import time

    try:
        GitHub(Config(data_dir=tmp_path), deadline=time.monotonic() + 0.9).patch(
            {"id": 42}, body="literal\nnext `line` $(text)"
        )
    finally:
        os.close(descriptor)
    assert seen == [{"body": "literal\nnext `line` $(text)"}]


def test_gh_release_reads_all_asset_pages(tmp_path, monkeypatch):
    gh = GitHub(Config(data_dir=tmp_path))

    def run(args, **kw):
        if "--paginate" in args:
            assert "--slurp" in args
            return [[{"name": "first"}], [{"name": "last"}]]
        return {"id": 1}

    monkeypatch.setattr(gh, "_run", run)
    assert gh.release(DAY)["assets"] == [{"name": "first"}, {"name": "last"}]


def test_ambiguous_public_commit_retains_reference_history_after_body_reverted(sealed):
    config, gh = sealed
    publish_revision(config, DAY, 1, github=gh)
    old_body = gh.state["body"]
    seal_revision(config, DAY, report())
    gh.crash = "patch"
    with pytest.raises(Crash):
        publish_revision(config, DAY, 2, github=gh)
    # Outside actor reverts the pointer after our worker dies. The revision has
    # been public at some point, so the durable pre-transition intent must still
    # prevent any repair/deletion of those bytes on retry.
    gh.state["body"] = old_body
    asset = next(a for a in gh.state["assets"] if "-rev02-" in a["name"])
    gh.bytes[asset["id"]] = b"corrupt"
    gh.calls.clear()
    with pytest.raises(ValueError, match="immutable"):
        publish_revision(config, DAY, 2, github=gh)
    assert not [c for c in gh.calls if c[0] in ("delete", "upload", "patch")]


def test_expired_remote_hash_deadline_never_spawns(tmp_path, monkeypatch):
    import time

    monkeypatch.setattr(
        "grvt_data.publish.subprocess.Popen",
        lambda *a, **k: pytest.fail("expired call spawned"),
    )
    with pytest.raises(TimeoutError):
        GitHub(Config(data_dir=tmp_path), deadline=time.monotonic() - 1).hash({"id": 1})
