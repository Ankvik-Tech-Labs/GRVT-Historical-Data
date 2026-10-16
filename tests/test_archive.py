import json
from datetime import UTC, date, datetime, timedelta

import pytest

from grvt_data.archive import pending_dates, resume_revision, seal_revision
from grvt_data.config import Config
from grvt_data.storage import (
    SegmentWriter,
    publication_db,
    recover_date_runs,
    start_date_run,
)
from grvt_data.trades import window_bounds

DAY = date(2026, 10, 12)


def report(day=DAY, **kwargs):
    start, end = window_bounds(day)
    return {
        "day": day.isoformat(),
        "start_ns": start,
        "end_ns": end,
        "instruments": {},
        "failures": {},
        "complete": True,
        "provisional": False,
        "query_started_at_ns": int(datetime.now(UTC).timestamp() * 10**9),
    } | kwargs


def segment(config, day=DAY, index=1, **kwargs):
    writer = SegmentWriter(config.data_dir, "depth")
    writer.receipt_index = index
    writer.append(
        dict(
            type="raw",
            received_ns=window_bounds(day)[0] + index,
            receipt_index=index,
            raw_text="hello",
            **kwargs,
        )
    )
    return config.data_dir / writer.close()[0]


def committed(config, day=DAY):
    db = publication_db(config)
    with db:
        db.execute(
            "UPDATE revisions SET status='committed',remote_verified=1 WHERE day=?",
            (str(day),),
        )
        db.execute("UPDATE parts SET verified=1 WHERE day=?", (str(day),))
    db.close()


def test_empty_day_records_unavailable(tmp_path):
    config = Config(data_dir=tmp_path)
    bundle = seal_revision(config, DAY, report())
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    assert manifest["depth"]["status"] == "unavailable"
    assert (bundle / "journal.json").exists()


def test_second_revision_refused_while_first_is_unfinished(tmp_path):
    config = Config(data_dir=tmp_path)
    seal_revision(config, DAY, report())
    with pytest.raises(ValueError, match="unfinished"):
        seal_revision(config, DAY, report())


def test_open_days_refused(tmp_path):
    config = Config(data_dir=tmp_path)
    for day in [datetime.now(UTC).date(), datetime.now(UTC).date() + timedelta(days=1)]:
        with pytest.raises(ValueError):
            seal_revision(config, day, report(day))


def test_attempt_without_checkpoint_survives_rollover(tmp_path):
    config = Config(data_dir=tmp_path)
    start_date_run(config, DAY)
    recover_date_runs(config)
    assert DAY in pending_dates(config, DAY + timedelta(days=5))


def test_first_ever_yesterday_selected_then_terminal_drops_out(tmp_path):
    config = Config(data_dir=tmp_path)
    today = DAY + timedelta(days=1)
    assert pending_dates(config, today) == {DAY}
    seal_revision(config, DAY, report())
    committed(config)
    assert pending_dates(config, today) == set()


def test_segment_reservation_and_cumulative_union(tmp_path):
    config = Config(data_dir=tmp_path)
    first = segment(config)
    bundle = seal_revision(config, DAY, report())
    old = json.loads((bundle / "manifest-rev01.json").read_text())
    committed(config)
    first.unlink()
    second = segment(config, index=2)
    bundle = seal_revision(config, DAY, report())
    new = json.loads((bundle / "manifest-rev02.json").read_text())
    assert set(new["depth"]["segments"]) == {
        str(first.relative_to(tmp_path)),
        str(second.relative_to(tmp_path)),
    }
    assert (
        new["depth"]["assets"][: len(old["depth"]["assets"])] == old["depth"]["assets"]
    )


def test_sealed_resume_checks_bytes_and_never_rebuilds(tmp_path):
    config = Config(data_dir=tmp_path)
    bundle = seal_revision(config, DAY, report())
    assert resume_revision(config, DAY, 1) == bundle
    (bundle / "manifest-rev01.json").write_text("corrupt")
    with pytest.raises(ValueError, match="sealed"):
        resume_revision(config, DAY, 1)


def trade_report(config, day=DAY, end_ns=None):
    from grvt_data.storage import TRADE_SCHEMA, write_parquet_atomic

    start, end = window_bounds(day)
    end = end if end_ns is None else end_ns
    path = (
        config.data_dir
        / "trades"
        / str(day)
        / "BTC_USDT_Perp"
        / f"{start}-{end}"
        / "trades.parquet"
    )
    write_parquet_atomic(path, [], TRADE_SCHEMA)
    return report(
        day,
        end_ns=end,
        provisional=end_ns is not None,
        query_started_at_ns=end + 31 * 10**9,
        instruments={
            "BTC_USDT_Perp": {
                "path": str(path),
                "complete": True,
                "pagination_exhausted": True,
                "start_ns": start,
                "end_ns": end,
                "rows": 0,
                "pages": 1,
            }
        },
    )


def test_deterministic_tar_and_fixed_headers(tmp_path):
    from grvt_data.archive import _record, _tar, sha256

    data = tmp_path / "input"
    data.write_bytes(bytes(range(256)) * 100)
    path = tmp_path / "one.tar.gz"
    entries = [_record(data, tmp_path)]
    _tar(path, entries, tmp_path)
    first = sha256(path)
    _tar(path, entries, tmp_path)
    assert sha256(path) == first
    assert path.read_bytes()[3] == 0  # no gzip filename flags
    assert path.read_bytes()[4:8] == b"\0" * 4
    assert path.read_bytes()[9] == 255  # stable OS byte


def test_small_threshold_splits_large_files_and_reconstructs_exact_frames(tmp_path):
    import os
    import tarfile

    from grvt_data.archive import sha256

    config = Config(data_dir=tmp_path, archive_part_bytes=8192)
    source = segment(config, random_payload=os.urandom(40000).hex())
    bundle = seal_revision(config, DAY, report())
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    journal = json.loads((bundle / "journal.json").read_text())
    parts = [a for a in journal["assets"] if a["dataset"] == "depth"]
    assert len(parts) > 1
    found = {}
    for asset in parts:
        assert (
            asset["size"] <= 8192 and sha256(bundle / asset["path"]) == asset["sha256"]
        )
        with tarfile.open(bundle / asset["path"]) as tar:
            found.update({m.name: tar.extractfile(m).read() for m in tar.getmembers()})
    chunks = manifest["depth"]["segment_records"][0]["raw_chunks"]
    assert b"".join(found[c["path"]] for c in chunks) == source.read_bytes()
    assert all(
        a["name"].startswith(f"grvt-{a['dataset']}-{DAY}-rev01-part")
        for a in journal["assets"]
        if a["dataset"] != "manifest"
    )


def test_pending_ignores_today_and_reopens_closed_day_for_new_segments(tmp_path):
    config = Config(data_dir=tmp_path)
    today = DAY + timedelta(days=1)
    seal_revision(config, DAY, report())
    committed(config)
    assert pending_dates(config, today) == set()
    segment(config, today)
    assert pending_dates(config, today) == set()
    segment(config, DAY)
    assert pending_dates(config, today) == {DAY}


def test_missing_instrument_is_incomplete_and_persists_retry(tmp_path):
    from grvt_data.archive import retry_status
    from grvt_data.storage import write_json_atomic

    config = Config(data_dir=tmp_path)
    write_json_atomic(
        tmp_path / "state" / "discovery" / f"{DAY}.json",
        {"instruments": [{"instrument": "MISSING_USDT_Perp", "extra": "retained"}]},
    )
    bundle = seal_revision(config, DAY, report())
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    assert not manifest["trades"]["full_day_complete"]
    assert not manifest["trades"]["instruments"]["MISSING_USDT_Perp"]["complete"]
    assert str(DAY) in retry_status(config)["dates"]
    committed(config)
    assert DAY in pending_dates(config, DAY + timedelta(days=4))


def test_embedded_pending_and_recovery_outages_keep_distinct_true_bounds(tmp_path):
    from grvt_data.storage import write_json_atomic

    config = Config(data_dir=tmp_path)
    start = window_bounds(DAY)[0]
    bounds = {
        "start_received_ns": start + 20,
        "start_receipt_index": 1,
        "coverage_start_received_ns": start + 2,
        "clock_rollback": True,
        "receipt_order_uncertain": True,
        "end_received_ns": None,
    }
    writer = SegmentWriter(tmp_path, "depth")
    writer.receipt_index = 2
    writer.set_checkpoints({"BTC": {"ready": False, "pending_outage": bounds}})
    writer.append(
        {"type": "raw", "received_ns": start + 5, "receipt_index": 2, "raw_text": "raw"}
    )
    identity = writer.close()[0]
    recovery = {
        "segment": identity.replace(".frames", ".open"),
        "instrument_outages": {
            "BTC": bounds,
            "AAOI": dict(bounds, start_received_ns=start + 9, start_receipt_index=3),
        },
    }
    write_json_atomic(
        tmp_path / "state" / "outages" / f"{(tmp_path / identity).stem}.json", recovery
    )
    # A recovery journal with no valid/publishable segment must still survive.
    write_json_atomic(
        tmp_path / "state" / "outages" / "lost.json",
        dict(recovery, segment=f"raw/depth/{DAY}/lost.open"),
    )
    bundle = seal_revision(config, DAY, report())
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    record = manifest["depth"]["segment_records"][0]
    assert record["checkpoints"][0]["checkpoints"]["BTC"]["pending_outage"] == bounds
    assert bounds in record["outages"]
    assert len(manifest["depth"]["recovery_journals"]) == 2
    assert (
        manifest["depth"]["recovery_journals"][0]["instrument_outages"]["AAOI"][
            "start_received_ns"
        ]
        == start + 9
    )
    assert manifest["depth"]["status"] == "partial"


def test_segment_resume_after_kill_reuses_completed_normalization(
    tmp_path, monkeypatch
):
    from grvt_data import archive

    config = Config(data_dir=tmp_path)
    first = segment(config, index=1)
    second = segment(config, index=2)
    original = archive._normalize_segment
    calls = []

    def normalize(config, identity, *args):
        calls.append(identity)
        if len(calls) == 2:
            raise KeyboardInterrupt()
        return original(config, identity, *args)

    monkeypatch.setattr(archive, "_normalize_segment", normalize)
    with pytest.raises(KeyboardInterrupt):
        seal_revision(config, DAY, report())
    completed_identity = calls[0]
    (tmp_path / completed_identity).unlink()  # durable normalization is self-contained
    monkeypatch.setattr(archive, "_normalize_segment", original)
    resume_revision(config, DAY, 1)
    db = publication_db(config)
    assert db.execute("SELECT count(*) FROM segments").fetchone()[0] == 2
    assert {first.name, second.name} == {
        r[0].split("/")[-1] for r in db.execute("SELECT identity FROM segments")
    }
    db.close()


def test_part_resume_after_kill_reuses_sealed_part(tmp_path, monkeypatch):
    import os

    from grvt_data import archive

    config = Config(data_dir=tmp_path, archive_part_bytes=8192)
    segment(config, random=os.urandom(20000).hex())
    original = archive._tar
    calls = []

    def tar(path, *args):
        calls.append(path)
        if len(calls) == 2:
            raise KeyboardInterrupt()
        return original(path, *args)

    monkeypatch.setattr(archive, "_tar", tar)
    with pytest.raises(KeyboardInterrupt):
        seal_revision(config, DAY, report())
    completed = calls[0]
    before = completed.read_bytes()

    def remaining(path, *args):
        assert path != completed
        return original(path, *args)

    monkeypatch.setattr(archive, "_tar", remaining)
    resume_revision(config, DAY, 1)
    assert completed.read_bytes() == before


def test_journal_to_ledger_crash_recovers_without_rebuilding(tmp_path, monkeypatch):
    from grvt_data import archive

    config = Config(data_dir=tmp_path)
    original = archive._finish_seal
    monkeypatch.setattr(
        archive,
        "_finish_seal",
        lambda *args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    with pytest.raises(KeyboardInterrupt):
        seal_revision(config, DAY, report())
    monkeypatch.setattr(archive, "_finish_seal", original)
    monkeypatch.setattr(
        archive,
        "_tar",
        lambda *args: pytest.fail("journaled bytes must never be rebuilt"),
    )
    resume_revision(config, DAY, 1)


def test_activation_owns_exact_indices_and_completes_after_prune(tmp_path, monkeypatch):
    from test_publish import FakeGitHub

    from grvt_data import archive
    from grvt_data.publish import prune_verified, publish_revision
    from grvt_data.storage import write_json_atomic

    day = datetime.now(UTC).date()
    config = Config(data_dir=tmp_path)
    start, _ = window_bounds(day)
    cutoff = start + 3600 * 10**9
    old = segment(config, day, index=1)
    rotated = segment(config, day, index=3)
    # This post-cutoff operation has an EARLIER wall sample under rollback.
    after = segment(config, day, index=5)
    receipt = {
        "cutoff_received_ns": cutoff,
        "cutoff_receipt_index": 3,
        "closed_segments": [str(rotated.relative_to(tmp_path))],
        "checkpoints": {},
        "partition_clock": "received_ns",
    }
    write_json_atomic(tmp_path / "state" / "depth-checkpoint.json", receipt)
    provisional = trade_report(config, day, cutoff)
    bundle = seal_revision(config, day, provisional, end_ns=cutoff, allow_today=True)
    first = json.loads((bundle / "manifest-rev01.json").read_text())
    assert set(first["depth"]["segments"]) == {
        str(old.relative_to(tmp_path)),
        str(rotated.relative_to(tmp_path)),
    }
    assert (
        not first["trades"]["full_day_complete"]
        and first["activation_receipt"] == receipt
    )
    gh = FakeGitHub(config)
    publish_revision(config, day, 1, github=gh)
    prune_verified(config, datetime.now(UTC), github=gh)
    assert not old.exists() and not rotated.exists() and after.exists()

    class Tomorrow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC)

    monkeypatch.setattr(archive, "datetime", Tomorrow)
    assert day in pending_dates(config, day + timedelta(days=1))
    bundle = seal_revision(config, day, trade_report(config, day))
    final = json.loads((bundle / "manifest-rev02.json").read_text())
    assert final["trades"]["full_day_complete"]
    assert len(final["depth"]["segments"]) == 3
    assert all(
        o["superseded_by"] == 2
        for a in final["trades"]["assets"]
        if a["revision"] == 1
        for o in a["instrument_coverage"]
    )
    publish_revision(config, day, 2, github=gh)
    assert pending_dates(config, day + timedelta(days=1)) == set()


@pytest.mark.parametrize(
    "allow,end", [(False, 1), (True, None), (True, -1), (True, 2**63)]
)
def test_activation_flag_window_validation(tmp_path, allow, end):
    config = Config(data_dir=tmp_path)
    with pytest.raises(ValueError):
        seal_revision(
            config, datetime.now(UTC).date(), report(), allow_today=allow, end_ns=end
        )


def test_date_attempts_monotonic_and_terminal_attempt_cannot_reopen_date(tmp_path):
    from grvt_data.storage import finish_date_run

    config = Config(data_dir=tmp_path)
    first = start_date_run(config, DAY)
    finish_date_run(config, DAY, first, "failed", "no worker")
    second = start_date_run(config, DAY)
    assert second == first + 1
    seal_revision(config, DAY, report())
    committed(config)
    recover_date_runs(config)
    assert pending_dates(config, DAY + timedelta(days=1)) == set()
    db = publication_db(config)
    rows = list(db.execute("SELECT * FROM per_date_runs ORDER BY attempt_id"))
    assert [r["status"] for r in rows] == ["failed", "interrupted"]
    assert all(r["ended_at_ns"] >= r["started_at_ns"] for r in rows)
    db.close()


def test_metadata_is_frozen_before_first_part_and_stable_on_resume(
    tmp_path, monkeypatch
):
    from grvt_data import archive
    from grvt_data.storage import write_json_atomic

    config = Config(data_dir=tmp_path)
    original = archive._parts
    calls = []

    def fail(*args):
        calls.append(1)
        raise KeyboardInterrupt()

    monkeypatch.setattr(archive, "_parts", fail)
    with pytest.raises(KeyboardInterrupt):
        seal_revision(config, DAY, report())
    # Collector discovery changes while sealing worker is down. Existing
    # revision's input snapshot must stay frozen; late work belongs to next.
    write_json_atomic(
        tmp_path / "state" / "discovery" / f"{DAY}.json",
        {"instruments": [{"instrument": "NEW"}]},
    )
    monkeypatch.setattr(archive, "_parts", original)
    bundle = resume_revision(config, DAY, 1)
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    assert "NEW" not in manifest["trades"]["instruments"]


def test_recovery_journal_without_frames_reopens_terminal_date(tmp_path):
    from grvt_data.storage import write_json_atomic

    config = Config(data_dir=tmp_path)
    seal_revision(config, DAY, report())
    committed(config)
    assert pending_dates(config, DAY + timedelta(days=1)) == set()
    write_json_atomic(
        tmp_path / "state" / "outages" / "new.json",
        {"segment": f"raw/depth/{DAY}/new.open", "discarded_bytes": 100},
    )
    assert pending_dates(config, DAY + timedelta(days=1)) == {DAY}


def test_indexing_evidence_missing_or_too_early_refuses_seal(tmp_path):
    config = Config(data_dir=tmp_path)
    missing = report()
    del missing["query_started_at_ns"]
    with pytest.raises(ValueError, match="query-start"):
        seal_revision(config, DAY, missing)
    with pytest.raises(ValueError, match="indexing lag"):
        seal_revision(
            config, DAY, report(query_started_at_ns=window_bounds(DAY)[1] + 2 * 10**9)
        )


def test_expired_sealing_deadline_retains_reservations_for_retry(tmp_path):
    import time

    from grvt_data.config import DeadlineExceeded

    config = Config(data_dir=tmp_path)
    segment(config)
    with pytest.raises(DeadlineExceeded):
        seal_revision(config, DAY, report(), deadline=time.monotonic() - 1)
    # Deadline can expire before the transaction commits: either no revision or
    # one reserved sealing revision is recoverable; no uploaded/committed bytes.
    db = publication_db(config)
    assert not db.execute("SELECT 1 FROM revisions WHERE status='committed'").fetchone()
    db.close()
    seal_revision(config, DAY, report())


def test_retry_discards_incomplete_atomic_temps_only(tmp_path, monkeypatch):
    from grvt_data import archive

    config = Config(data_dir=tmp_path)
    original = archive._finish_seal
    monkeypatch.setattr(
        archive, "_finish_seal", lambda *a: (_ for _ in ()).throw(KeyboardInterrupt())
    )
    with pytest.raises(KeyboardInterrupt):
        seal_revision(config, DAY, report())
    bundle = tmp_path / "staging" / str(DAY) / "rev01"
    orphan = bundle / ".incomplete.tmp"
    orphan.write_bytes(b"partial")
    journal = (bundle / "journal.json").read_bytes()
    monkeypatch.setattr(archive, "_finish_seal", original)
    resume_revision(config, DAY, 1)
    assert not orphan.exists() and (bundle / "journal.json").read_bytes() == journal


def _kill_boundary_worker(config, boundary, send):
    import time

    from grvt_data import archive

    normalize, tar, finish = (
        archive._normalize_segment,
        archive._tar,
        archive._finish_seal,
    )
    counts = {"segment": 0, "part": 0}

    def pause():
        send.send("durable")
        time.sleep(60)

    def normalize_hook(*args):
        counts["segment"] += 1
        if boundary == "segment" and counts["segment"] == 2:
            pause()
        return normalize(*args)

    def tar_hook(*args):
        counts["part"] += 1
        if boundary == "part" and counts["part"] == 2:
            pause()
        return tar(*args)

    def finish_hook(*args):
        if boundary == "journal":
            pause()
        return finish(*args)

    archive._normalize_segment = normalize_hook
    archive._tar = tar_hook
    archive._finish_seal = finish_hook
    seal_revision(config, DAY, report())


@pytest.mark.parametrize("boundary", ["segment", "part", "journal"])
def test_actual_worker_sigkill_resumes_durable_boundaries(
    tmp_path, monkeypatch, boundary
):
    import multiprocessing
    import os

    from grvt_data import archive

    config = Config(data_dir=tmp_path, archive_part_bytes=8192)
    segment(config, index=1, random=os.urandom(10000).hex())
    segment(config, index=2)
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)

    process = context.Process(
        target=_kill_boundary_worker, args=(config, boundary, send)
    )
    process.start()
    send.close()
    try:
        assert receive.poll(5), "worker never reached durable boundary"
        assert receive.recv() == "durable"
    finally:
        process.kill()
        process.join(5)
        receive.close()
    assert process.exitcode == -9
    db = publication_db(config)
    completed = {
        r["identity"]
        for r in db.execute("SELECT * FROM segments WHERE normalized_json IS NOT NULL")
    }
    parts = {
        r["name"]: json.loads(r["record_json"])["sha256"]
        for r in db.execute("SELECT * FROM parts")
    }
    db.close()
    original = archive._normalize_segment

    def remaining(config, identity, *args):
        assert identity not in completed
        return original(config, identity, *args)

    monkeypatch.setattr(archive, "_normalize_segment", remaining)
    bundle = resume_revision(config, DAY, 1)
    assert (bundle / "journal.json").exists()
    assert all(
        archive.sha256(bundle / name) == digest for name, digest in parts.items()
    )
