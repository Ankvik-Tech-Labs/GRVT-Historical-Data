from datetime import date

from grvt_data.storage import SegmentWriter, read_segment


def test_recovery_records_truncated_tail(tmp_path):
    writer = SegmentWriter(tmp_path, "depth")
    writer.append({"received_ns": 1, "type": "raw", "message": {"a": 1}})
    path = next(tmp_path.rglob("*.open"))
    writer.flush()
    with path.open("ab") as stream:
        stream.write(b"broken")
    writer.abort()
    recovered = SegmentWriter(tmp_path, "depth")
    outages = recovered.recover()
    assert outages[0]["reason"] == "crash_recovery"
    assert outages[0]["discarded_bytes"] == 6
    assert not list(tmp_path.rglob("*.open"))
    assert len(list(read_segment(next(tmp_path.rglob("*.frames"))))) == 2


def test_every_segment_opens_with_its_own_checkpoint(tmp_path):
    writer = SegmentWriter(tmp_path, "depth")
    writer.set_checkpoints(
        {"BTC": {"session_id": "s", "last_sequence": 1, "bids": [], "asks": []}}
    )
    writer.append({"received_ns": 1, "type": "raw"})
    receipt = writer.rotate_now()
    writer.append({"received_ns": receipt.cutoff_received_ns + 1, "type": "raw"})
    writer.close_day(date.today())
    writer.close()
    paths = list(tmp_path.rglob("*.frames"))
    assert len(paths) == 2
    for path in paths:
        assert next(read_segment(path))["checkpoints"]["BTC"]["last_sequence"] == 1


def test_crc_corruption_stops_at_first_invalid_frame(tmp_path):
    import struct

    writer = SegmentWriter(tmp_path, "depth")
    writer.append({"received_ns": 1, "value": 1})
    writer.append({"received_ns": 2, "value": 2})
    writer.abort()
    path = next(tmp_path.rglob("*.open"))
    with path.open("r+b") as stream:
        size, _ = struct.unpack(">II", stream.read(8))
        stream.seek(8 + size)
        size, crc = struct.unpack(">II", stream.read(8))
        stream.seek(-4, 1)
        stream.write(struct.pack(">I", crc ^ 1))
    recovered = SegmentWriter(tmp_path, "depth")
    outages = recovered.recover()
    assert outages[0]["discarded_bytes"] > 0
    assert len(list(read_segment(next(tmp_path.rglob("*.frames"))))) == 1
    assert list((tmp_path / "state/outages").glob("*.json"))


def test_fsync_precedes_append_return(tmp_path, monkeypatch):
    import os

    calls = []
    original = os.fsync

    def track(fd):
        calls.append(fd)
        original(fd)

    monkeypatch.setattr(os, "fsync", track)
    writer = SegmentWriter(tmp_path, "depth")
    writer.append({"received_ns": 1})
    count = len(calls)
    writer.append({"received_ns": 2})
    assert len(calls) > count
    writer.close()


def test_disk_thresholds_use_total_capacity(tmp_path, monkeypatch):
    from collections import namedtuple

    from grvt_data.storage import disk_status

    Usage = namedtuple("Usage", "total used free")
    gib = 1024**3
    for free, tier in [
        (21, "healthy"),
        (19, "warning"),
        (9, "critical"),
        (1, "hard_stop"),
    ]:
        monkeypatch.setattr(
            "shutil.disk_usage",
            lambda p, free=free: Usage(100 * gib, (100 - free) * gib, free * gib),
        )
        assert disk_status(tmp_path)["tier"] == tier
    monkeypatch.setattr(
        "shutil.disk_usage", lambda p: Usage(40 * gib, 31 * gib, 9 * gib)
    )
    assert disk_status(tmp_path)["tier"] == "critical"  # 10GiB floor


def test_pause_marker_backoff_is_persistent(tmp_path, monkeypatch):
    from grvt_data.storage import write_pause

    monkeypatch.setattr("time.time_ns", lambda: 100)
    assert [
        write_pause(tmp_path, "disk_hard_stop")["retry_after_ns"] for _ in range(3)
    ] == [1_000_000_100, 2_000_000_100, 4_000_000_100]


def test_hard_stop_flushes_and_records_outages(tmp_path, monkeypatch):
    import pytest

    from grvt_data.config import Config
    from grvt_data.depth import Capture, Connection, DiskHardStop

    capture = Capture(Config(data_dir=tmp_path))
    conn = Connection(["BTC@100", "AAOI@100"])
    conn.open()
    capture.connections.append(conn)
    monkeypatch.setattr(
        "grvt_data.depth.disk_status",
        lambda *a: {"tier": "hard_stop", "cleanup_requested": True},
    )
    with pytest.raises(DiskHardStop):
        capture.status()
    assert not list(tmp_path.rglob("*.open"))
    assert (tmp_path / "state/depth-pause.json").exists()
    assert (tmp_path / "state/cleanup-request.json").exists()
    rows = [r for p in tmp_path.rglob("*.frames") for r in read_segment(p)]
    assert {r["instrument"] for r in rows if r["type"] == "outage"} == {"BTC", "AAOI"}


def test_recovery_itself_can_crash_without_losing_tail_evidence(tmp_path, monkeypatch):
    import os

    import pytest

    writer = SegmentWriter(tmp_path, "depth")
    writer.set_checkpoints(
        {
            "BTC": {"last_update_received_ns": 5, "last_update_receipt_index": 1},
            "AAOI": {"last_update_received_ns": 10, "last_update_receipt_index": 2},
        }
    )
    writer.append({"received_ns": 20, "type": "raw", "receipt_index": 3})
    writer.abort()
    path = next(tmp_path.rglob("*.open"))
    with path.open("ab") as stream:
        stream.write(b"broken")
    replace = os.replace

    def fail_seal(source, target):
        if source == path:
            raise OSError("simulated crash after truncate, before rename")
        replace(source, target)

    with monkeypatch.context() as patch:
        patch.setattr(os, "replace", fail_seal)
        with pytest.raises(OSError, match="simulated crash"):
            SegmentWriter(tmp_path, "depth").recover()
    outages = SegmentWriter(tmp_path, "depth").recover()
    assert outages[0]["discarded_bytes"] == 6

    assert {
        i: o["start_received_ns"] for i, o in outages[0]["instrument_outages"].items()
    } == {"BTC": 5, "AAOI": 10}


def test_corrupt_first_frame_is_not_a_publishable_empty_segment(tmp_path):
    writer = SegmentWriter(tmp_path, "depth")
    writer.append({"received_ns": 1})
    writer.abort()
    next(tmp_path.rglob("*.open")).write_bytes(b"broken")
    outages = SegmentWriter(tmp_path, "depth").recover()
    assert outages[0]["discarded_bytes"] == 6
    assert not list(tmp_path.rglob("*.frames"))
    assert not list(tmp_path.rglob("*.open"))


def test_recovery_uses_per_instrument_normalized_bounds_not_peer_raw_or_control(
    tmp_path,
):
    writer = SegmentWriter(tmp_path, "depth")
    writer.set_checkpoints(
        {
            "BTC": {
                "session_id": "s",
                "ready": True,
                "last_update_received_ns": 5,
                "last_update_receipt_index": 1,
                "bids": [],
                "asks": [],
            },
            "AAOI": {
                "session_id": "s",
                "ready": True,
                "last_update_received_ns": 6,
                "last_update_receipt_index": 2,
                "bids": [],
                "asks": [],
            },
        }
    )
    writer.append(
        {
            "type": "delta",
            "instrument": "AAOI",
            "session_id": "s",
            "received_ns": 10,
            "receipt_index": 3,
        }
    )
    writer.append(
        {"type": "raw", "instrument": "BTC", "received_ns": 20, "receipt_index": 4}
    )
    writer.append({"type": "day_boundary", "received_ns": 30, "receipt_index": 5})
    writer.abort()
    with next(tmp_path.rglob("*.open")).open("ab") as stream:
        stream.write(b"broken")
    outage = SegmentWriter(tmp_path, "depth").recover()[0]
    assert outage["instrument_outages"]["BTC"]["start_received_ns"] == 5
    assert outage["instrument_outages"]["BTC"]["start_receipt_index"] == 1
    assert outage["instrument_outages"]["AAOI"]["start_received_ns"] == 10
    assert outage["instrument_outages"]["AAOI"]["start_receipt_index"] == 3
    assert outage["discarded_bytes"] == 6
