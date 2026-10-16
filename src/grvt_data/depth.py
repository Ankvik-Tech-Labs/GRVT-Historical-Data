"""Public full-L2 capture. All book/storage/control mutations have one owner.

received_ns is the apply loop's local receipt timestamp; wire_received_ns is
sampled by the socket reader. Partitioning uses the apply-loop receipt clock. Wall clocks may regress;
receipt_index and closed segment identities define cutoff ownership. Neither
clock is exchange time.
"""

import asyncio
import contextlib
import json
import logging
import signal
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from .api import GrvtAPI
from .config import Config
from .storage import (
    MAX_FRAME_BYTES,
    SegmentWriter,
    backoff_seconds,
    disk_status,
    utc_day,
    write_json_atomic,
    write_pause,
)

log = logging.getLogger(__name__)
DepthEvent = dict


class SequenceGap(RuntimeError):
    pass


class ProtocolViolation(SequenceGap):
    pass


class QueueOverflow(SequenceGap):
    pass


class DiskHardStop(RuntimeError):
    pass


class DepthBook:
    def __init__(self, instrument: str):
        self.instrument = instrument
        self.session_id = None
        self.first_sequence = None
        self.last_sequence = None
        self.ready = False
        self.bids = {}
        self.asks = {}
        self.receipt_index = 0
        self.last_received_ns = None

    def acknowledge(self, session_id: str, first_sequence: int):
        self.session_id = session_id
        self.first_sequence = first_sequence

    def apply(self, message: dict, received_ns: int) -> list[DepthEvent]:
        if self.session_id is None or self.first_sequence is None:
            raise ProtocolViolation("feed before selector ACK")
        try:
            feed = message["feed"]
            if feed["instrument"] != self.instrument:
                raise ValueError("instrument/selector mismatch")
            sequence = int(message["sequence_number"])
            snapshot = message["sequence_number"] == "0"
            if snapshot and self.ready:
                raise ProtocolViolation("unsolicited repeated snapshot")
            if not snapshot:
                if not self.ready:
                    raise ProtocolViolation("delta before selector snapshot")
                expected = (
                    self.first_sequence
                    if self.last_sequence is None
                    else self.last_sequence + 1
                )
                if sequence != expected:
                    raise SequenceGap(
                        f"{self.instrument}: expected {expected}, got {sequence}"
                    )
            # Validate the complete update before mutating state.
            sides = {}
            for side in ("bids", "asks"):
                sides[side] = []
                for level in feed[side]:
                    price, size = level["price"], level["size"]
                    if not isinstance(price, str) or not isinstance(size, str):
                        raise TypeError("price/size must preserve numeric strings")
                    p, s = Decimal(price), Decimal(size)
                    if not p.is_finite() or p <= 0 or not s.is_finite() or s < 0:
                        raise ValueError("invalid level")
                    sides[side].append((p, s, dict(level)))
            event_time = int(feed["event_time"])
            if not 0 <= event_time < 2**63:
                raise ValueError("event_time outside int64 nanosecond range")
        except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
            raise ProtocolViolation(f"malformed depth: {exc}") from exc
        for side, changes in sides.items():
            book = getattr(self, side)
            if snapshot:
                book.clear()
            for price, size, level in changes:
                if size == 0:
                    book.pop(price, None)
                else:
                    book[price] = level
            if len(book) > 100_000:
                raise ProtocolViolation("book level memory bound exceeded")
        self.ready = True
        if not snapshot:
            self.last_sequence = sequence
        self.receipt_index += 1
        self.last_received_ns = received_ns
        return [
            {
                "type": "snapshot" if snapshot else "delta",
                "instrument": self.instrument,
                "session_id": self.session_id,
                "received_ns": received_ns,
                "receipt_index": self.receipt_index,
                "event_time": event_time,
                "sequence_number": message["sequence_number"],
                "prev_sequence_number": message.get("prev_sequence_number"),
                "bids": [level for _, _, level in sides["bids"]],
                "asks": [level for _, _, level in sides["asks"]],
            }
        ]

    def checkpoint(self, received_ns: int) -> DepthEvent:
        return {
            "type": "book_checkpoint",
            "instrument": self.instrument,
            "received_ns": received_ns,
            "session_id": self.session_id,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "ready": self.ready,
            "last_update_received_ns": self.last_received_ns,
            "receipt_index": self.receipt_index,
            "bids": [dict(self.bids[p]) for p in sorted(self.bids, reverse=True)],
            "asks": [dict(self.asks[p]) for p in sorted(self.asks)],
        }


class Connection:
    def __init__(self, selectors: list[str]):
        if not 1 <= len(selectors) <= 20 or len(set(selectors)) != len(selectors):
            raise ValueError("connections require 1..20 unique selectors")
        if any(not s.endswith("@100") for s in selectors):
            raise ValueError("only full L2 @100 selectors are supported")
        self.selectors = list(selectors)
        self.books = {}
        self.session_id = None
        self.request_id = 1
        self.acked = False
        self.last_received_ns = None

    def open(self, request_id: int = 1) -> dict:
        self.session_id = uuid.uuid4().hex
        self.request_id = request_id
        self.acked = False
        self.last_received_ns = None
        self.books = {s: DepthBook(s.rsplit("@", 1)[0]) for s in self.selectors}
        return {
            "jsonrpc": "2.0",
            "method": "subscribe",
            "id": request_id,
            "params": {"stream": "v1.book.d", "selectors": self.selectors},
        }

    def apply(self, message: dict, received_ns: int) -> list[DepthEvent]:
        if "result" in message or "error" in message:
            try:
                result = message["result"]
                if (
                    self.acked
                    or message["id"] != self.request_id
                    or result["stream"] != "v1.book.d"
                    or result["subs"] != self.selectors
                    or result.get("unsubs")
                    or result["num_snapshots"] != [1] * len(self.selectors)
                ):
                    raise ValueError("rejected/partial/unmatched ACK")
                first = [int(n) for n in result["first_sequence_number"]]
                latest = [int(n) for n in result["latest_sequence_number"]]
                if len(first) != len(self.selectors) or len(latest) != len(first):
                    raise ValueError("missing selector sequence bounds")
                if any(a != b + 1 for a, b in zip(first, latest)):
                    raise ValueError("inconsistent ACK sequence bounds")
                for selector, sequence in zip(self.selectors, first):
                    self.books[selector].acknowledge(self.session_id, sequence)
                self.acked = True
                self.last_received_ns = received_ns
                return []
            except (KeyError, TypeError, ValueError) as exc:
                raise ProtocolViolation(f"invalid subscribe ACK: {exc}") from exc
        if (
            message.get("stream") != "v1.book.d"
            or message.get("selector") not in self.books
        ):
            raise ProtocolViolation("unexpected stream/selector")
        events = self.books[message["selector"]].apply(message, received_ns)
        self.last_received_ns = received_ns
        return events

    def outages(self, reason: str, received_ns: int) -> list[dict]:
        return [
            {
                "type": "outage",
                "instrument": s.rsplit("@", 1)[0],
                "session_id": self.session_id,
                "reason": reason,
                "received_ns": received_ns,
                "start_received_ns": (
                    self.books[s].last_received_ns
                    if s in self.books
                    else self.last_received_ns
                ),
                "end_received_ns": None,
            }
            for s in self.selectors
        ]


class Capture:
    """Synchronous state machine called exclusively by the async queue consumer."""

    def __init__(self, config: Config):
        self.config = config
        self.writer = SegmentWriter(config.data_dir, "depth")
        self.connections = []
        self.pending_outages = {}
        self.last_received_ns = 0
        # Reserve durable index ranges so process restarts never reuse an index,
        # even if old segments have already been published and pruned.
        self.index_path = config.data_dir / "state" / "depth-receipt-index.json"
        prior = (
            json.loads(self.index_path.read_text()) if self.index_path.exists() else {}
        )
        self.receipt_index = prior.get("reserved_through", 0)
        self.index_limit = self.receipt_index + 1_000_000
        write_json_atomic(self.index_path, {"reserved_through": self.index_limit})
        self.writer.set_checkpoints(self.checkpoints)
        self.recovered = self.writer.recover()
        self.day = None
        for outage in self.recovered:
            for instrument in outage["instruments"]:
                self.pending_outages[instrument] = dict(outage, instrument=instrument)
            now = self.now()
            self.writer.append(
                dict(outage, received_ns=now, receipt_index=self.receipt_index)
            )

    def checkpoints(self):
        return {
            book.instrument: book.checkpoint(self.last_received_ns)
            for connection in self.connections
            for book in connection.books.values()
        }

    def now(self):
        self.last_received_ns = time.time_ns()
        self.receipt_index += 1
        if self.receipt_index > self.index_limit:
            self.index_limit += 1_000_000
            write_json_atomic(self.index_path, {"reserved_through": self.index_limit})
        self.writer.receipt_index = self.receipt_index
        return self.last_received_ns

    def record_outages(self, connection, reason):
        now = self.now()
        for outage in connection.outages(reason, now):
            outage["receipt_index"] = self.receipt_index
            self.pending_outages.setdefault(outage["instrument"], outage)
            self.writer.append(outage)

    def process(self, connection, message, wire_received_ns, direction="recv"):
        now = self.now()
        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="replace")
        # Opening a day segment happens before apply: its checkpoint is exactly
        # the pre-message state, never a book which already includes this delta.
        self.writer.append(
            {
                "type": "raw",
                "received_ns": now,
                "wire_received_ns": wire_received_ns,
                "session_id": connection.session_id,
                "receipt_index": self.receipt_index,
                "direction": direction,
                **(
                    {"message": message}
                    if isinstance(message, dict)
                    else {"raw_text": message}
                ),
            }
        )
        if not isinstance(message, dict):
            try:
                message = json.loads(message)
                if not isinstance(message, dict):
                    raise TypeError("non-object wire message")
            except (ValueError, TypeError) as exc:
                raise ProtocolViolation(f"invalid wire JSON: {exc}") from exc
        if direction == "send":
            return []
        events = connection.apply(message, now)
        for event in events:
            event["receipt_index"] = self.receipt_index
            if event["type"] == "snapshot":
                instrument = event["instrument"]
                pending = self.pending_outages.pop(instrument, None)
                if pending is None:
                    start = (
                        int(
                            datetime.combine(
                                utc_day(now), datetime.min.time(), UTC
                            ).timestamp()
                        )
                        * 10**9
                    )
                    pending = {
                        "type": "outage",
                        "instrument": instrument,
                        "start_received_ns": start,
                        "reason": "no_midnight_checkpoint",
                    }
                self.writer.append(
                    dict(
                        pending,
                        received_ns=now,
                        end_received_ns=now,
                        session_id=connection.session_id,
                        receipt_index=self.receipt_index,
                    )
                )
            self.writer.append(event)
        return events

    def rotate_now(self):
        receipt = self.writer.rotate_now()
        return receipt

    def status(self):
        now = self.now()
        status = {
            "received_ns": now,
            "disk": disk_status(self.config.data_dir, self.config),
            "instruments": {
                k: {
                    "session_id": v["session_id"],
                    "last_sequence": v["last_sequence"],
                    "ready": v["ready"],
                }
                for k, v in self.checkpoints().items()
            },
        }
        write_json_atomic(self.config.data_dir / "state" / "depth-status.json", status)
        if status["disk"]["cleanup_requested"]:
            write_json_atomic(
                self.config.data_dir / "state" / "cleanup-request.json",
                {"received_ns": now, "reason": "disk_critical"},
            )
        if status["disk"]["tier"] == "hard_stop":
            for connection in self.connections:
                self.record_outages(connection, "disk_hard_stop")
            self.writer.close()
            write_pause(self.config.data_dir, "disk_hard_stop")
            raise DiskHardStop("less than hard-stop free bytes")
        if status["disk"]["tier"] != "healthy":
            log.warning("depth disk tier: %s", status["disk"]["tier"])
        return status


async def _submit(queue, kind, *args):
    future = asyncio.get_running_loop().create_future()
    try:
        if kind == "outage":
            await queue.put((kind, args, future))
        else:
            queue.put_nowait((kind, args, future))
    except asyncio.QueueFull as exc:
        raise QueueOverflow("bounded apply queue overflow") from exc
    return await future


async def _connection_loop(connection, queue, config):
    attempt = 0
    while True:
        started = time.monotonic()
        try:
            async with connect(
                config.ws_url,
                ping_interval=20,
                ping_timeout=20,
                max_queue=32,
                max_size=8 * 1024**2,
                open_timeout=30,
                close_timeout=5,
            ) as socket:
                request = await _submit(queue, "open", connection)
                await socket.send(json.dumps(request))
                await _submit(
                    queue, "message", connection, request, time.time_ns(), "send"
                )
                # Only initial ACK + snapshots have a timeout. There is no
                # message-silence timeout once all books are ready: ping/pong
                # judges socket liveness even for quiet equities.
                handshake_deadline = time.monotonic() + 30
                while True:
                    if all(b.ready for b in connection.books.values()):
                        raw = await socket.recv()
                    else:
                        remaining = handshake_deadline - time.monotonic()
                        if remaining <= 0:
                            raise ProtocolViolation("ACK/snapshot handshake timeout")
                        raw = await asyncio.wait_for(socket.recv(), remaining)
                    wire_ns = time.time_ns()
                    await _submit(queue, "message", connection, raw, wire_ns, "recv")
        except asyncio.CancelledError:
            raise
        except (
            OSError,
            TimeoutError,
            WebSocketException,
            SequenceGap,
            ValueError,
            TypeError,
        ) as exc:
            await _submit(queue, "outage", connection, f"{type(exc).__name__}: {exc}")
            # Reset only after a sustained healthy run, never per successful ACK.
            if time.monotonic() - started >= 60:
                attempt = 0
            await asyncio.sleep(backoff_seconds(attempt))
            attempt += 1


def _discover(config):
    api = GrvtAPI(config)
    try:
        return api.instruments(active=True)
    finally:
        api.close()


def persist_discovery(data_dir: Path, day, instruments: list[dict]):
    path = data_dir / "state" / "discovery" / f"{day.isoformat()}.json"
    previous = json.loads(path.read_text())["instruments"] if path.exists() else []
    union = {row["instrument"]: row for row in previous + instruments}
    write_json_atomic(path, {"instruments": list(union.values())})


async def _wait_for_disk(config):
    config.data_dir.mkdir(parents=True, exist_ok=True)
    path = config.data_dir / "state" / "depth-pause.json"
    while True:
        if path.exists():
            marker = json.loads(path.read_text())
            delay = (marker["retry_after_ns"] - time.time_ns()) / 1e9
            if delay > 0:
                await asyncio.sleep(delay)
        if disk_status(config.data_dir, config)["tier"] != "hard_stop":
            path.unlink(missing_ok=True)
            return
        write_pause(config.data_dir, "disk_hard_stop")


async def collect_depth(config: Config) -> None:
    """Run until cancelled. Unix socket: one JSON line {"command":"rotate_now"}.

    Response is a JSON RotateReceipt or {"error":...}. status is also accepted.
    No credentials are read or passed to the streaming WebSocket.
    """
    if not 0 < config.depth_discovery_interval_seconds <= 86400:
        raise ValueError("discovery interval must be >0 and <=86400 seconds")
    if config.depth_rate_ms != 100:
        raise ValueError("this protocol contract requires 100ms depth")
    await _wait_for_disk(config)
    (config.data_dir / "state").mkdir(parents=True, exist_ok=True)
    queue = asyncio.Queue(maxsize=256)
    tasks = []
    socket_path = config.data_dir / "control.sock"
    # One collector owns a data root; flock is independent of the publisher lock.
    import fcntl

    lock = (config.data_dir / "state" / "depth.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise RuntimeError("collector already owns data root") from None

    try:
        capture = Capture(config)
    except BaseException:
        lock.close()
        raise
    control_tasks = set()

    async def control(reader, writer):
        control_tasks.add(asyncio.current_task())
        try:
            if len(control_tasks) > 8:
                raise ValueError("too many concurrent control clients")
            line = await asyncio.wait_for(reader.readline(), 5)
            request = json.loads(line)
            command = request.get("command")
            if command not in ("rotate_now", "status"):
                raise ValueError("unknown control command")
            if set(request) != {"command"}:
                raise ValueError(
                    "control takes only command; cutoff is collector-owned"
                )
            result = await _submit(queue, command)
            writer.write(json.dumps(result).encode() + b"\n")
            await asyncio.wait_for(writer.drain(), 5)
        except (OSError, TimeoutError, ValueError, TypeError, QueueOverflow) as exc:
            writer.write(json.dumps({"error": str(exc)}).encode() + b"\n")
            with contextlib.suppress(ConnectionError):
                await asyncio.wait_for(writer.drain(), 5)
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 2)
            except (ConnectionError, TimeoutError):
                writer.transport.abort()
            control_tasks.discard(asyncio.current_task())

    async def refresh():
        while True:
            try:
                instruments = await asyncio.to_thread(_discover, config)
                await _submit(queue, "discovery", instruments)
            except asyncio.CancelledError:
                raise
            except (OSError, RuntimeError, ValueError) as exc:
                log.warning("depth discovery failed: %s", exc)
            await asyncio.sleep(config.depth_discovery_interval_seconds)

    async def ticker():
        while True:
            await _submit(queue, "tick")
            await asyncio.sleep(1)

    server = None
    loop = asyncio.get_running_loop()
    owner = asyncio.current_task()
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    installed_sigterm = False
    try:
        loop.add_signal_handler(signal.SIGTERM, owner.cancel)
        installed_sigterm = True
    except (NotImplementedError, RuntimeError):
        pass  # Embedded collectors outside the main thread use cancellation.
    try:
        socket_path.unlink(missing_ok=True)
        server = await asyncio.start_unix_server(control, path=socket_path, limit=4096)
        socket_path.chmod(0o600)
        tasks.extend([asyncio.create_task(refresh()), asyncio.create_task(ticker())])
        known = set()
        metadata = []
        growth_paused = False
        discovery_day = None
        while True:
            kind, args, future = await queue.get()
            try:
                if kind == "open":
                    result = args[0].open()
                elif kind == "message":
                    result = capture.process(*args)
                elif kind == "outage":
                    result = capture.record_outages(*args)
                elif kind == "rotate_now":
                    result = capture.rotate_now().to_dict()
                elif kind == "status":
                    result = capture.status()
                elif kind == "tick":
                    status = capture.status()
                    tier = status["disk"]["tier"]
                    growth_paused = tier in ("critical", "hard_stop") or (
                        growth_paused and tier != "healthy"
                    )
                    today = datetime.now(UTC).date()
                    if today != discovery_day and metadata:
                        persist_discovery(config.data_dir, today, metadata)
                        discovery_day = today
                    # Rotate idle books at UTC midnight as well as on traffic.
                    if capture.writer._day is not None and capture.writer._day != today:
                        capture.rotate_now()
                        capture.writer.append(
                            {
                                "type": "day_boundary",
                                "received_ns": capture.now(),
                                "receipt_index": capture.receipt_index,
                            }
                        )
                    result = None
                elif kind == "discovery":
                    tier = capture.status()["disk"]["tier"]
                    growth_paused = tier in ("critical", "hard_stop") or (
                        growth_paused and tier != "healthy"
                    )
                    metadata = args[0]
                    today = datetime.now(UTC).date()
                    persist_discovery(config.data_dir, today, metadata)
                    discovery_day = today
                    added = sorted({r["instrument"] for r in metadata} - known)
                    if not growth_paused:
                        for offset in range(0, len(added), 20):
                            connection = Connection(
                                [f"{i}@100" for i in added[offset : offset + 20]]
                            )
                            capture.connections.append(connection)
                            tasks.append(
                                asyncio.create_task(
                                    _connection_loop(connection, queue, config)
                                )
                            )
                            known.update(
                                s.rsplit("@", 1)[0] for s in connection.selectors
                            )
                    result = None
                else:
                    raise ValueError(f"unknown queue operation: {kind}")
                if not future.done():
                    future.set_result(result)
            except (SequenceGap, ValueError, KeyError, TypeError) as exc:
                if not future.done():
                    future.set_exception(exc)
            except BaseException as exc:
                if not future.done():
                    future.set_exception(exc)
                raise
            finally:
                queue.task_done()
    finally:
        if server is not None:
            server.close()
        tasks.extend(control_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if server is not None:
            await server.wait_closed()
        try:
            for connection in capture.connections:
                capture.record_outages(connection, "collector_shutdown")
            capture.writer.close()
        finally:
            try:
                capture.writer.abort()
                socket_path.unlink(missing_ok=True)
            finally:
                lock.close()
                if installed_sigterm:
                    loop.remove_signal_handler(signal.SIGTERM)
                    signal.signal(signal.SIGTERM, previous_sigterm)


async def depth_control(data_dir: Path, command: str = "rotate_now") -> dict:
    """Read a full-universe control receipt (which may exceed asyncio's 64KiB default)."""
    if command not in ("rotate_now", "status"):
        raise ValueError("unknown control command")
    reader, writer = await asyncio.open_unix_connection(
        Path(data_dir) / "control.sock", limit=MAX_FRAME_BYTES
    )
    try:
        writer.write(json.dumps({"command": command}).encode() + b"\n")
        await asyncio.wait_for(writer.drain(), 5)
        response = json.loads(await asyncio.wait_for(reader.readline(), 30))
        if "error" in response:
            raise RuntimeError(response["error"])
        return response
    finally:
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), 2)
        except (ConnectionError, TimeoutError):
            writer.transport.abort()
