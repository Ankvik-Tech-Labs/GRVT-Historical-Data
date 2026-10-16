"""Durable replace-on-success state and typed Parquet writes."""

import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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
