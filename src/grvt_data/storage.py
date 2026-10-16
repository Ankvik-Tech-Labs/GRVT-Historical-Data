"""Durable replace-on-success state and typed Parquet writes."""

import json
import os
import shutil
import struct
import tempfile
import time
import uuid
import zlib
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config

TRADE_SCHEMA = pa.schema(
    [
        pa.field("instrument", pa.string(), nullable=False),
        pa.field("trade_id", pa.string(), nullable=False),
        pa.field("event_time", pa.int64(), nullable=False),
        *[
            pa.field(name, pa.string(), nullable=False)
            for name in ("price", "size", "venue")
        ],
        pa.field("is_taker_buyer", pa.bool_(), nullable=False),
        *[
            pa.field(name, pa.string())
            for name in ("mark_price", "index_price", "interest_rate", "forward_price")
        ],
        pa.field("is_rpi", pa.bool_()),
    ]
)


def _atomic(path: Path, writer: Callable[[Path], None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp = Path(name)
    try:
        writer(temp)
        with temp.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temp.unlink(missing_ok=True)


def write_json_atomic(path: Path, payload: dict) -> None:
    _atomic(
        path,
        lambda temp: temp.write_text(
            json.dumps(payload, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        ),
    )


def write_parquet_atomic(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    _atomic(
        path,
        lambda temp: pq.write_table(
            pa.Table.from_pylist(rows, schema=schema), temp, compression="zstd"
        ),
    )


# Each frame is independently zlib-compressed: uint32 length, uint32 CRC32,
# then that many compressed bytes. A damaged tail never hides prior frames.
_FRAME = struct.Struct(">II")
MAX_FRAME_BYTES = 64 * 1024**2


def utc_day(received_ns: int) -> date:
    return datetime.fromtimestamp(received_ns // 1_000_000_000, UTC).date()


def _read_frame(stream):
    header = stream.read(_FRAME.size)
    if not header:
        return None
    if len(header) != _FRAME.size:
        raise ValueError("truncated frame header")
    size, crc = _FRAME.unpack(header)
    if not 0 < size <= MAX_FRAME_BYTES:
        raise ValueError("invalid frame length")
    payload = stream.read(size)
    if len(payload) != size or zlib.crc32(payload) != crc:
        raise ValueError("truncated or CRC-invalid frame")
    decoder = zlib.decompressobj()
    decoded = decoder.decompress(payload, MAX_FRAME_BYTES + 1)
    if len(decoded) > MAX_FRAME_BYTES or not decoder.eof or decoder.unused_data:
        raise ValueError("invalid compressed frame")
    row = json.loads(decoded)
    if not isinstance(row, dict):
        raise ValueError("frame must contain an object")  # noqa: TRY004 -- corrupt wire format
    return row


def read_segment(path: Path) -> Iterator[dict]:
    """Strict streaming reader for closed segments; corrupt data raises."""
    with path.open("rb") as stream:
        while (row := _read_frame(stream)) is not None:
            yield row


def _fsync_dir(path: Path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@dataclass(frozen=True)
class RotateReceipt:
    cutoff_received_ns: int
    checkpoints: dict[str, dict]
    closed_segments: list[str]
    cutoff_receipt_index: int = 0
    partition_clock: str = "received_ns"

    def to_dict(self) -> dict:
        return asdict(self)


class SegmentWriter:
    """Single-owner writer. Call only in the collector's serialized apply loop.

    Raw envelopes and normalized events share the same ordered segment. Every
    append fsyncs, including the raw record before normalization is attempted.
    Publishers must enumerate *.frames only, never *.open.
    """

    def __init__(self, data_dir: Path, dataset: str):
        if not dataset or Path(dataset).name != dataset:
            raise ValueError("dataset must be a path component")
        self.data_dir = Path(data_dir)
        self.dataset = dataset
        self.root = self.data_dir / "raw" / dataset
        self.root.mkdir(parents=True, exist_ok=True)
        _fsync_dir(self.root.parent)
        _fsync_dir(self.data_dir)
        self._stream = None
        self._path = None
        self._day = None
        self._checkpoints = dict
        self.receipt_index = 0

    def set_checkpoints(self, checkpoints):
        self._checkpoints = (
            checkpoints if callable(checkpoints) else lambda: checkpoints
        )

    def _write(self, row):
        payload = json.dumps(row, separators=(",", ":"), allow_nan=False).encode()
        if len(payload) > MAX_FRAME_BYTES:
            raise ValueError("frame exceeds maximum size")
        payload = zlib.compress(payload)
        if len(payload) > MAX_FRAME_BYTES:
            raise ValueError("compressed frame exceeds maximum size")
        self._stream.write(_FRAME.pack(len(payload), zlib.crc32(payload)))
        self._stream.write(payload)
        self.flush()

    def append(self, row: dict) -> None:
        received_ns = row["received_ns"]
        day = utc_day(received_ns)
        if self._day is not None and day != self._day:
            self.close()
        if self._stream is None:
            directory = self.root / day.isoformat()
            directory.mkdir(parents=True, exist_ok=True)
            self._path = directory / f"{received_ns}-{uuid.uuid4().hex}.open"
            self._stream = self._path.open("xb")
            self._day = day
            self._write(
                {
                    "type": "checkpoint",
                    "received_ns": received_ns,
                    "checkpoints": self._checkpoints(),
                    "receipt_index": self.receipt_index,
                    "partition_clock": "received_ns",
                }
            )
            _fsync_dir(directory)
            _fsync_dir(self.root)
        self._write(row)

    def flush(self):
        if self._stream is not None:
            self._stream.flush()
            os.fsync(self._stream.fileno())

    def close(self) -> list[str]:
        if self._stream is None:
            return []
        self.flush()
        self._stream.close()
        closed = self._path.with_suffix(".frames")
        os.replace(self._path, closed)
        _fsync_dir(closed.parent)
        self._stream = self._path = self._day = None
        return [str(closed.relative_to(self.data_dir))]

    def abort(self):
        """Close the descriptor without sealing (also useful to simulate a crash)."""
        if self._stream is not None:
            self._stream.close()
        self._stream = self._path = self._day = None

    def close_day(self, day: date) -> None:
        if self._day == day:
            self.close()

    def rotate_now(self) -> RotateReceipt:
        # No await or concurrent book mutation can occur during this operation.
        closed = self.close()
        cutoff = time.time_ns()
        receipt = RotateReceipt(cutoff, self._checkpoints(), closed, self.receipt_index)
        write_json_atomic(
            self.data_dir / "state" / f"{self.dataset}-checkpoint.json",
            receipt.to_dict(),
        )
        return receipt

    def recover(self) -> list[dict]:
        if self._stream is not None:
            raise RuntimeError("recover before appending")
        outages = []
        for path in sorted(self.root.rglob("*.open")):
            good = 0
            last_ns = None
            instruments = set()
            with path.open("r+b") as stream:
                while True:
                    try:
                        row = _read_frame(stream)
                        if row is None:
                            break
                        last_ns = row.get("received_ns", last_ns)
                        instruments.update(row.get("checkpoints", {}))
                        if row.get("instrument"):
                            instruments.add(row["instrument"])
                        good = stream.tell()
                    except (ValueError, zlib.error, UnicodeError):
                        break
                discarded = path.stat().st_size - good
                outage = {
                    "type": "outage",
                    "reason": "crash_recovery",
                    "received_ns": time.time_ns(),
                    "start_received_ns": last_ns,
                    "end_received_ns": None,
                    "instruments": sorted(instruments),
                    "segment": str(path.relative_to(self.data_dir)),
                    "discarded_bytes": discarded,
                }
                # Journal BEFORE truncation/rename. If recovery itself crashed,
                # retain the original discarded-tail evidence on the next retry.
                journal = self.data_dir / "state" / "outages" / f"{path.stem}.json"
                if journal.exists():
                    prior = json.loads(journal.read_text())
                    if prior["discarded_bytes"] >= discarded:
                        outage = prior
                write_json_atomic(journal, outage)
                stream.truncate(good)
                stream.flush()
                os.fsync(stream.fileno())
            if good:
                os.replace(path, path.with_suffix(".frames"))
            else:
                path.unlink()
            _fsync_dir(path.parent)
            outages.append(outage)
        return outages


def disk_status(data_dir: Path, config: Config | None = None) -> dict:
    config = config or Config()
    usage = shutil.disk_usage(data_dir)
    fraction = usage.free / usage.total
    tier = "healthy"
    if fraction < config.warn_free_pct:
        tier = "warning"
    if usage.free < max(
        usage.total * config.critical_free_pct, config.critical_free_bytes
    ):
        tier = "critical"
    if usage.free < config.hard_stop_free_bytes:
        tier = "hard_stop"
    return {
        "tier": tier,
        "free_bytes": usage.free,
        "total_bytes": usage.total,
        "free_fraction": fraction,
        "pause_universe_growth": tier in ("critical", "hard_stop"),
        "cleanup_requested": tier in ("critical", "hard_stop"),
    }


def backoff_seconds(attempt: int) -> int:
    return min(2 ** min(max(attempt, 0), 9), 300)


def write_pause(data_dir: Path, reason: str) -> dict:
    path = data_dir / "state" / "depth-pause.json"
    previous = json.loads(path.read_text()) if path.exists() else {}
    attempt = previous.get("attempt", -1) + 1
    marker = {
        "reason": reason,
        "attempt": attempt,
        "retry_after_ns": time.time_ns() + backoff_seconds(attempt) * 1_000_000_000,
    }
    write_json_atomic(path, marker)
    return marker
