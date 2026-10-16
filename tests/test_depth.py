import gzip
import json
from collections import Counter
from pathlib import Path

import pytest

from grvt_data.depth import Connection, ProtocolViolation, SequenceGap

FIXTURE = (
    Path(__file__).parent
    / "fixtures/depth_protocol/book_d_btc_aaoi_reconnect_full.jsonl.gz"
)


def fixture():
    with gzip.open(FIXTURE, "rt") as stream:
        return [json.loads(line) for line in stream]


def test_fixture_sequence_continuity_has_zero_gaps():
    counts = Counter()
    session = 0
    for row in fixture():
        if row["dir"] == "send":
            session += 1
            connection = Connection(row["msg"]["params"]["selectors"])
            connection.open(session)
        elif row["dir"] == "recv":
            connection.apply(row["msg"], row["t_recv_ns"])
            if row["msg"].get("sequence_number") not in (None, "0"):
                counts[session, row["msg"]["feed"]["instrument"]] += 1
    assert list(counts.values()) == [2106, 588, 253, 83]


def test_pre_snapshot_delta_is_protocol_violation():
    rows = fixture()
    connection = Connection(rows[0]["msg"]["params"]["selectors"])
    connection.open()
    connection.apply(rows[1]["msg"], 1)
    with pytest.raises(ProtocolViolation):
        connection.apply(rows[4]["msg"], 2)


def test_sequence_gap_reconnects_whole_connection():
    rows = fixture()
    connection = Connection(rows[0]["msg"]["params"]["selectors"])
    connection.open()
    for row in rows[1:4]:
        connection.apply(row["msg"], row["t_recv_ns"])
    bad = dict(rows[4]["msg"], sequence_number="964935")
    with pytest.raises(SequenceGap):
        connection.apply(bad, 10)
    outages = connection.outages("gap", 10)
    assert {o["instrument"] for o in outages} == {"BTC_USDT_Perp", "AAOI_USDT_Perp"}
    previous = connection.session_id
    connection.open()
    assert connection.session_id != previous
    assert all(not book.ready for book in connection.books.values())


def test_reconnect_sequence_never_compared_cross_session():
    rows = fixture()
    connection = Connection(rows[0]["msg"]["params"]["selectors"])
    connection.open()
    for row in rows[1:]:
        if row["dir"] == "close_forced":
            break
        if row["dir"] == "recv":
            connection.apply(row["msg"], row["t_recv_ns"])
    new_rows = rows[
        next(i for i, r in enumerate(rows) if r["dir"] == "close_forced") + 1 :
    ]
    delta = next(
        r for r in new_rows if r["msg"].get("sequence_number") not in (None, "0")
    )
    with pytest.raises(SequenceGap):
        connection.apply(delta["msg"], delta["t_recv_ns"])
    connection.open(2)
    for row in new_rows:
        if row["dir"] == "recv":
            connection.apply(row["msg"], row["t_recv_ns"])


def setup_capture(tmp_path):
    from grvt_data.config import Config
    from grvt_data.depth import Capture

    rows = fixture()
    capture = Capture(Config(data_dir=tmp_path))
    connection = Connection(rows[0]["msg"]["params"]["selectors"])
    connection.open()
    capture.connections.append(connection)
    for row in rows[1:4]:
        capture.process(connection, row["msg"], row["t_recv_ns"])
    return capture, connection, rows


def test_snapshot_deltas_delete_replay():
    from grvt_data.depth import DepthBook

    book = DepthBook("BTC")
    book.acknowledge("session", 23)

    def message(seq, bids):
        return {
            "sequence_number": str(seq),
            "feed": {"instrument": "BTC", "event_time": "2", "bids": bids, "asks": []},
        }

    book.apply(message(0, [{"price": "100", "size": "2"}]), 1)
    book.apply(
        message(23, [{"price": "100.0", "size": "0"}, {"price": "99", "size": "3"}]), 1
    )
    checkpoint = book.checkpoint(1)
    assert checkpoint["bids"] == [{"price": "99", "size": "3"}]
    assert checkpoint["last_sequence"] == 23
    assert checkpoint["receipt_index"] == 2


def test_empty_snapshot_and_equal_timestamps():
    from grvt_data.depth import DepthBook

    book = DepthBook("BTC")
    book.acknowledge("session", 2)
    message = {
        "sequence_number": "0",
        "feed": {"instrument": "BTC", "event_time": "1", "bids": [], "asks": []},
    }
    events = book.apply(message, 5)
    events += book.apply(dict(message, sequence_number="2"), 5)
    assert [e["type"] for e in events] == ["snapshot", "delta"]
    assert [e["receipt_index"] for e in events] == [1, 2]
    assert book.checkpoint(5)["bids"] == []


def test_rotate_now_returns_actual_cutoff_and_checkpoint(tmp_path, monkeypatch):
    from grvt_data.storage import read_segment

    capture, connection, rows = setup_capture(tmp_path)
    capture.process(connection, rows[4]["msg"], 17)
    index = capture.receipt_index
    monkeypatch.setattr("time.time_ns", lambda: 2_000_000_000)
    receipt = capture.rotate_now()
    assert receipt.cutoff_received_ns == 2_000_000_000
    assert receipt.cutoff_receipt_index == index
    assert receipt.checkpoints["BTC_USDT_Perp"]["last_sequence"] == 964934
    assert receipt.checkpoints["BTC_USDT_Perp"]["session_id"] == connection.session_id
    assert receipt.closed_segments
    # This socket read predates the command, but is applied after its cutoff.
    capture.process(connection, rows[5]["msg"], 1_000_000_000)
    capture.writer.close()
    new = [
        p
        for p in tmp_path.rglob("*.frames")
        if str(p.relative_to(tmp_path)) not in receipt.closed_segments
    ]
    records = list(read_segment(new[0]))
    assert records[0]["checkpoints"]["BTC_USDT_Perp"]["last_sequence"] == 964934
    raw = next(r for r in records if r["type"] == "raw")
    assert raw["wire_received_ns"] < receipt.cutoff_received_ns
    assert raw["receipt_index"] > receipt.cutoff_receipt_index
    assert connection.books["BTC_USDT_Perp@100"].last_sequence == 964935


def test_wall_clock_rollback_keeps_closed_identity_and_receipt_order(
    tmp_path, monkeypatch
):
    from grvt_data.storage import read_segment

    capture, connection, rows = setup_capture(tmp_path)
    receipt = capture.rotate_now()
    monkeypatch.setattr("time.time_ns", lambda: 10)
    capture.process(connection, rows[4]["msg"], 8)
    capture.writer.close()
    fresh = [
        p
        for p in tmp_path.rglob("*.frames")
        if str(p.relative_to(tmp_path)) not in receipt.closed_segments
    ]
    raw = next(r for r in read_segment(fresh[0]) if r["type"] == "raw")
    assert raw["received_ns"] == 10 < receipt.cutoff_received_ns
    assert raw["receipt_index"] > receipt.cutoff_receipt_index
    assert all((tmp_path / p).exists() for p in receipt.closed_segments)


def test_midnight_checkpoint_and_restart(tmp_path, monkeypatch):
    from grvt_data.config import Config
    from grvt_data.depth import Capture, DepthBook
    from grvt_data.storage import read_segment, utc_day

    before = 1792195199 * 10**9  # 2026-10-16 23:59:59 UTC
    monkeypatch.setattr("time.time_ns", lambda: before)
    capture, connection, rows = setup_capture(tmp_path)
    capture.process(connection, rows[4]["msg"], before)
    after = before + 2 * 10**9
    monkeypatch.setattr("time.time_ns", lambda: after)
    capture.process(connection, rows[5]["msg"], after)
    capture.writer.close()
    path = next((tmp_path / "raw/depth" / str(utc_day(after))).glob("*.frames"))
    records = list(read_segment(path))
    cp = records[0]["checkpoints"]["BTC_USDT_Perp"]
    book = DepthBook(cp["instrument"])
    book.acknowledge(cp["session_id"], cp["first_sequence"])
    book.apply(
        {
            "sequence_number": "0",
            "feed": {
                "instrument": cp["instrument"],
                "event_time": "0",
                "bids": cp["bids"],
                "asks": cp["asks"],
            },
        },
        after,
    )
    book.last_sequence = cp["last_sequence"]
    book.apply(next(r["message"] for r in records if r["type"] == "raw"), after)
    assert (
        book.checkpoint(after)["bids"]
        == connection.books["BTC_USDT_Perp@100"].checkpoint(after)["bids"]
    )
    restarted = Capture(Config(data_dir=tmp_path))
    fresh = Connection(connection.selectors)
    fresh.open()
    restarted.connections.append(fresh)
    for row in rows[1:4]:
        restarted.process(fresh, row["msg"], after)
    restarted.writer.close()
    outages = [
        r
        for p in tmp_path.rglob("*.frames")
        for r in read_segment(p)
        if r["type"] == "outage" and r.get("reason") == "no_midnight_checkpoint"
    ]
    assert len(outages) == 4
    assert restarted.receipt_index > capture.receipt_index


@pytest.mark.parametrize(
    "change",
    [
        {"error": {"code": 1}},
        {"result": {"subs": []}},
    ],
)
def test_rejected_or_failed_ack(change):
    connection = Connection(["BTC_USDT_Perp@100"])
    connection.open()
    with pytest.raises(ProtocolViolation):
        connection.apply(change, 1)


def test_raw_is_durable_before_invalid_normalization(tmp_path):
    from grvt_data.storage import read_segment

    capture, connection, _ = setup_capture(tmp_path)
    with pytest.raises(ProtocolViolation):
        capture.process(connection, "{bad json", 1)
    capture.writer.close()
    records = [r for p in tmp_path.rglob("*.frames") for r in read_segment(p)]
    assert records[-1]["raw_text"] == "{bad json"
    assert records[-1]["type"] == "raw"


def test_bounded_apply_queue_overflow():
    import asyncio

    from grvt_data.depth import QueueOverflow, _submit

    async def run():
        queue = asyncio.Queue(maxsize=1)
        queue.put_nowait(None)
        with pytest.raises(QueueOverflow):
            await _submit(queue, "message", None)

    asyncio.run(run())


def test_discovery_retains_full_metadata_and_removed_instruments(tmp_path):
    from datetime import date

    from grvt_data.depth import persist_discovery
    from grvt_data.trades import day_universe

    day = date(2026, 10, 16)
    persist_discovery(tmp_path, day, [{"instrument": "BTC", "kind": "PERPETUAL"}])
    persist_discovery(
        tmp_path, day, [{"instrument": "AAOI", "kind": "PERPETUAL", "base": "AAOI"}]
    )
    data = json.loads((tmp_path / "state/discovery/2026-10-16.json").read_text())
    assert len(data["instruments"]) == 2
    assert data["instruments"][1]["base"] == "AAOI"
    assert set(day_universe(tmp_path, day, [])) == {"BTC", "AAOI"}


async def control_command(path, command):
    import asyncio

    reader, writer = await asyncio.open_unix_connection(path)
    writer.write(json.dumps(command).encode() + b"\n")
    await writer.drain()
    result = json.loads(await reader.readline())
    writer.close()
    await writer.wait_closed()
    return result


async def eventually(check):
    import asyncio

    async def poll():
        while not check():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), 5)


def synthetic_ack(selectors):
    return {
        "id": 1,
        "result": {
            "stream": "v1.book.d",
            "subs": selectors,
            "unsubs": [],
            "num_snapshots": [1] * len(selectors),
            "first_sequence_number": ["100"] * len(selectors),
            "latest_sequence_number": ["99"] * len(selectors),
        },
    }


def synthetic_feed(selector, sequence):
    return {
        "stream": "v1.book.d",
        "selector": selector,
        "sequence_number": str(sequence),
        "feed": {
            "instrument": selector.rsplit("@", 1)[0],
            "event_time": "123",
            "bids": [],
            "asks": [],
        },
    }


def test_unix_control_live_socket_and_added_instrument(tmp_path, monkeypatch):
    import asyncio
    import contextlib

    from websockets.asyncio.server import serve

    from grvt_data import depth
    from grvt_data.config import Config
    from grvt_data.storage import read_segment

    calls = []
    received = []

    def discovery(config):
        calls.append(1)
        count = 1 if len(calls) == 1 else 22
        return [
            {"instrument": f"EQUITY{i}_USDT_Perp", "kind": "PERPETUAL"}
            for i in range(count)
        ]

    monkeypatch.setattr(depth, "_discover", discovery)

    async def run():
        release_delta = asyncio.Event()

        async def handler(socket):
            request = json.loads(await socket.recv())
            selectors = request["params"]["selectors"]
            received.append(selectors)
            await socket.send(json.dumps(synthetic_ack(selectors)))
            for selector in selectors:
                await socket.send(json.dumps(synthetic_feed(selector, 0)))
            await release_delta.wait()
            for selector in selectors:
                await socket.send(json.dumps(synthetic_feed(selector, 100)))
            await socket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            config = Config(
                data_dir=tmp_path,
                ws_url=f"ws://127.0.0.1:{port}",
                depth_discovery_interval_seconds=0.03,
            )
            collector = asyncio.create_task(depth.collect_depth(config))
            try:
                await eventually(lambda: sum(map(len, received)) == 22)
                assert sorted(map(len, received)) == [1, 1, 20]
                await eventually(lambda: (tmp_path / "control.sock").exists())
                # Let ACK/snapshots finish without per-instrument silence checks.
                for _ in range(100):
                    status = await control_command(
                        tmp_path / "control.sock", {"command": "status"}
                    )
                    if len(status["instruments"]) == 22 and all(
                        x["ready"] for x in status["instruments"].values()
                    ):
                        break
                    await asyncio.sleep(0.005)
                assert all(x["ready"] for x in status["instruments"].values())
                # WebSocket pong works during a quiet book interval.
                await asyncio.sleep(0.05)
                receipt = await control_command(
                    tmp_path / "control.sock", {"command": "rotate_now"}
                )
                assert len(receipt["checkpoints"]) == 22
                assert receipt["closed_segments"]
                assert receipt["partition_clock"] == "received_ns"
                bad = await control_command(
                    tmp_path / "control.sock",
                    {"command": "rotate_now", "cutoff_received_ns": 1},
                )
                assert "error" in bad
                release_delta.set()
                for _ in range(100):
                    status = await control_command(
                        tmp_path / "control.sock", {"command": "status"}
                    )
                    if all(
                        x["last_sequence"] == 100
                        for x in status["instruments"].values()
                    ):
                        break
                    await asyncio.sleep(0.005)
                assert all(
                    x["last_sequence"] == 100 for x in status["instruments"].values()
                )
            finally:
                collector.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await collector

    asyncio.run(run())
    assert not (tmp_path / "control.sock").exists()
    assert not list(tmp_path.rglob("*.open"))
    rows = [r for p in tmp_path.rglob("*.frames") for r in read_segment(p)]
    assert sum(r["type"] == "delta" for r in rows) == 22
    assert all(r["wire_received_ns"] for r in rows if r["type"] == "raw")


def test_protocol_violation_reconnect_backs_off(tmp_path, monkeypatch):
    import asyncio
    import contextlib

    from websockets.asyncio.server import serve

    from grvt_data import depth
    from grvt_data.config import Config
    from grvt_data.storage import read_segment

    monkeypatch.setattr(
        depth, "_discover", lambda c: [{"instrument": "BTC"}, {"instrument": "AAOI"}]
    )
    attempts = []
    original = depth.backoff_seconds

    def fast_backoff(attempt):
        attempts.append(original(attempt))
        return 0.005

    monkeypatch.setattr(depth, "backoff_seconds", fast_backoff)

    async def run():
        async def handler(socket):
            request = json.loads(await socket.recv())
            selectors = request["params"]["selectors"]
            await socket.send(json.dumps(synthetic_ack(selectors)))
            # Invalid pre-snapshot delta repeatedly forces full reconnect.
            await socket.send(json.dumps(synthetic_feed(selectors[0], 100)))
            await socket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            config = Config(
                data_dir=tmp_path,
                ws_url=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",
            )
            collector = asyncio.create_task(depth.collect_depth(config))
            try:
                await eventually(lambda: len(attempts) >= 3)
            finally:
                collector.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await collector

    asyncio.run(run())
    assert attempts[:3] == [1, 2, 4]
    rows = [r for p in tmp_path.rglob("*.frames") for r in read_segment(p)]
    outages = [
        r
        for r in rows
        if r["type"] == "outage" and "delta before" in r.get("reason", "")
    ]
    assert {r["instrument"] for r in outages} == {"BTC", "AAOI"}
    assert len({r["session_id"] for r in outages}) >= 3
    assert not [r for r in rows if r["type"] == "delta"]


def test_liveness_uses_ping_pong_not_message_silence():
    # Replay actual ~4.4s quiet-equity gaps on a multiplexed healthy socket;
    # event timestamps alone must never create an instrument-stalled fault.
    rows = fixture()
    connection = None
    last = None
    largest_gap = 0
    for row in rows:
        if row["dir"] == "send":
            connection = Connection(row["msg"]["params"]["selectors"])
            connection.open(row["msg"]["id"])
            last = None
        elif row["dir"] == "recv":
            message = row["msg"]
            connection.apply(message, row["t_recv_ns"])
            if message.get("selector") == "AAOI_USDT_Perp@100":
                if last:
                    largest_gap = max(largest_gap, row["t_recv_ns"] - last)
                last = row["t_recv_ns"]
    assert largest_gap > 4_000_000_000


def test_actual_gap_closes_socket_and_resubscribes_all_peers(tmp_path, monkeypatch):
    import asyncio
    import contextlib

    from websockets.asyncio.server import serve

    from grvt_data import depth
    from grvt_data.config import Config
    from grvt_data.storage import read_segment

    monkeypatch.setattr(
        depth, "_discover", lambda c: [{"instrument": "BTC"}, {"instrument": "AAOI"}]
    )
    monkeypatch.setattr(depth, "backoff_seconds", lambda attempt: 0.005)
    subscriptions = []

    async def run():
        async def handler(socket):
            request = json.loads(await socket.recv())
            selectors = request["params"]["selectors"]
            subscriptions.append(selectors)
            await socket.send(json.dumps(synthetic_ack(selectors)))
            for selector in selectors:
                await socket.send(json.dumps(synthetic_feed(selector, 0)))
            # Only BTC skips first expected delta, invalidating AAOI too.
            sequence = 101 if len(subscriptions) == 1 else 100
            await socket.send(json.dumps(synthetic_feed("BTC@100", sequence)))
            await socket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            collector = asyncio.create_task(
                depth.collect_depth(
                    Config(
                        data_dir=tmp_path,
                        ws_url=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                    )
                )
            )
            try:
                await eventually(lambda: len(subscriptions) == 2)
                for _ in range(100):
                    status = await control_command(
                        tmp_path / "control.sock", {"command": "status"}
                    )
                    if status["instruments"]["BTC"]["last_sequence"] == 100:
                        break
                    await asyncio.sleep(0.005)
                assert status["instruments"]["BTC"]["last_sequence"] == 100
                assert status["instruments"]["AAOI"]["ready"]
            finally:
                collector.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await collector

    asyncio.run(run())
    assert subscriptions == [["AAOI@100", "BTC@100"]] * 2
    rows = [r for p in tmp_path.rglob("*.frames") for r in read_segment(p)]
    gaps = [
        r
        for r in rows
        if r["type"] == "outage" and "expected 100, got 101" in r.get("reason", "")
    ]
    assert {r["instrument"] for r in gaps} == {"BTC", "AAOI"}
    sessions = {r["session_id"] for r in rows if r["type"] == "snapshot"}
    assert len(sessions) == 2


def test_pause_honored_before_subscribing(tmp_path, monkeypatch):
    import asyncio

    from grvt_data import depth
    from grvt_data.config import Config
    from grvt_data.storage import write_json_atomic

    write_json_atomic(
        tmp_path / "state/depth-pause.json",
        {"attempt": 1, "retry_after_ns": 2_000_000_000},
    )
    monkeypatch.setattr("time.time_ns", lambda: 1_000_000_000)
    waited = []

    async def stop_after_wait(delay):
        waited.append(delay)
        raise asyncio.CancelledError

    monkeypatch.setattr(depth.asyncio, "sleep", stop_after_wait)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(depth.collect_depth(Config(data_dir=tmp_path)))
    assert waited == [1.0]
    assert not (tmp_path / "control.sock").exists()


def test_control_receipt_larger_than_default_stream_limit(tmp_path):
    import asyncio

    from grvt_data.depth import depth_control

    async def run():
        response = {
            "cutoff_received_ns": 1,
            "checkpoints": {"BTC": {"bids": ["x"] * 100000}},
        }

        async def handler(reader, writer):
            await reader.readline()
            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(
            handler, path=tmp_path / "control.sock"
        )
        try:
            assert await depth_control(tmp_path) == response
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_collector_shutdown_does_not_wait_for_nonreading_control_client(
    tmp_path, monkeypatch
):
    import asyncio
    import contextlib

    from websockets.asyncio.server import serve

    from grvt_data import depth
    from grvt_data.config import Config

    monkeypatch.setattr(depth, "_discover", lambda c: [{"instrument": "BTC"}])

    async def run():
        async def handler(socket):
            request = json.loads(await socket.recv())
            selectors = request["params"]["selectors"]
            await socket.send(json.dumps(synthetic_ack(selectors)))
            snapshot = synthetic_feed(selectors[0], 0)
            snapshot["feed"]["bids"] = [
                {"price": str(p), "size": "1.123456789", "num_orders": 1}
                for p in range(1, 20001)
            ]
            await socket.send(json.dumps(snapshot))
            await socket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            collector = asyncio.create_task(
                depth.collect_depth(
                    Config(
                        data_dir=tmp_path,
                        ws_url=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                    )
                )
            )
            writer = None
            try:
                await eventually(lambda: (tmp_path / "control.sock").exists())
                for _ in range(100):
                    status = await depth.depth_control(tmp_path, "status")
                    if status["instruments"].get("BTC", {}).get("ready"):
                        break
                    await asyncio.sleep(0.005)
                assert status["instruments"]["BTC"]["ready"]
                _, writer = await asyncio.open_unix_connection(
                    tmp_path / "control.sock", limit=1024
                )
                writer.write(b'{"command":"rotate_now"}\n')
                await writer.drain()
                await asyncio.sleep(0.02)
            finally:
                collector.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(collector, 4)
                if writer:
                    writer.transport.abort()

    asyncio.run(run())


def test_critical_disk_pauses_growth_until_warning_recovers(tmp_path, monkeypatch):
    import asyncio
    import contextlib

    from grvt_data import depth
    from grvt_data.config import Config

    tier = ["critical"]
    metadata = [{"instrument": "BTC"}]
    starts = []
    discovers = []

    def discover(config):
        discovers.append(1)
        return list(metadata)

    def disk(*args):
        return {"tier": tier[0], "cleanup_requested": tier[0] == "critical"}

    async def fake_connection(connection, queue, config):
        await depth._submit(queue, "open", connection)
        starts.append(connection.selectors)
        await asyncio.Future()

    monkeypatch.setattr(depth, "disk_status", disk)
    monkeypatch.setattr(depth, "_discover", discover)
    monkeypatch.setattr(depth, "_connection_loop", fake_connection)

    async def run():
        collector = asyncio.create_task(
            depth.collect_depth(
                Config(data_dir=tmp_path, depth_discovery_interval_seconds=0.01)
            )
        )
        try:
            await eventually(lambda: len(discovers) >= 2)
            assert not starts
            tier[0] = "warning"
            checkpoint = len(discovers)
            await eventually(lambda: len(discovers) >= checkpoint + 2)
            assert not starts
            tier[0] = "healthy"
            await eventually(lambda: len(starts) == 1)
            tier[0] = "critical"
            metadata.append({"instrument": "AAOI"})
            checkpoint = len(discovers)
            await eventually(lambda: len(discovers) >= checkpoint + 2)
            assert starts == [["BTC@100"]]
            tier[0] = "healthy"
            await eventually(lambda: len(starts) == 2)
        finally:
            collector.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await collector

    asyncio.run(run())
    assert starts == [["BTC@100"], ["AAOI@100"]]
    assert (tmp_path / "state/cleanup-request.json").exists()


def test_outage_starts_at_each_instruments_last_valid_receipt():
    connection = Connection(["BTC@100", "AAOI@100"])
    connection.open()
    connection.apply(synthetic_ack(connection.selectors), 1)
    connection.apply(synthetic_feed("BTC@100", 0), 2)
    connection.apply(synthetic_feed("AAOI@100", 0), 3)
    connection.apply(synthetic_feed("AAOI@100", 100), 10)
    with pytest.raises(SequenceGap):
        connection.apply(synthetic_feed("BTC@100", 101), 20)
    outages = {r["instrument"]: r for r in connection.outages("gap", 20)}
    assert outages["BTC"]["start_received_ns"] == 2
    assert outages["AAOI"]["start_received_ns"] == 10
