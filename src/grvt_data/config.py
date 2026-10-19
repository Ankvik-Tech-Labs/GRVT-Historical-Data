"""Shared runtime settings; deadlines use the monotonic clock."""

import time
from dataclasses import dataclass
from pathlib import Path

Instrument = dict[str, object]
Trade = dict[str, object]
PUBLIC_DATASETS = ("trades", "ohlcv", "funding", "ticker", "metadata", "ohlcv_extra")


@dataclass(frozen=True)
class Config:
    data_dir: Path = Path("data")
    repo: str = "Ankvik-Tech-Labs/GRVT-Historical-Data"
    rest_url: str = "https://market-data.grvt.io"
    ws_url: str = "wss://market-data.grvt.io/ws/full"
    depth_rate_ms: int = 100
    depth_discovery_interval_seconds: float = 60.0
    requests_per_second: float = 2.0
    request_timeout_seconds: float = 30.0
    max_retries: int = 4
    trade_page_limit: int = 1000
    instrument_limit: int = 100000
    warn_free_pct: float = 0.20
    critical_free_pct: float = 0.10
    critical_free_bytes: int = 10 * 1024**3
    hard_stop_free_bytes: int = 2 * 1024**3
    publish_delay_reserve_days: int = 1
    per_date_deadline_seconds: int = 3600
    archive_part_bytes: int = 1024**3 - 1
    release_asset_cap: int = 1000
    trade_indexing_lag_seconds: float = 30.0
    ohlcv_recent_pages: int = 4096
    ohlcv_backfill_pages: int = 128
    ohlcv_budget_seconds: float = 1800.0
    market_page_budget: int = 2200
    market_budget_seconds: float = 1500.0
    datasets: tuple[str, ...] = ("trades", "depth")

    def __post_init__(self):
        # JSON worker payloads contain lists; normalize them at the boundary.
        object.__setattr__(self, "datasets", tuple(self.datasets))
        if self.datasets not in (
            ("trades",),
            ("trades", "depth"),
            ("trades", "ohlcv"),
            PUBLIC_DATASETS,
        ):
            raise ValueError(
                "datasets must be trades, trades,depth, trades,ohlcv or all public data"
            )

        if (
            self.ohlcv_recent_pages < 1
            or self.ohlcv_backfill_pages < 1
            or self.ohlcv_budget_seconds <= 0
        ):
            raise ValueError("OHLCV budgets must be positive")
        if self.market_page_budget < 1 or self.market_budget_seconds <= 0:
            raise ValueError("market data budgets must be positive")

    @property
    def market_enabled(self):
        return "funding" in self.datasets

    @property
    def ohlcv_enabled(self):
        return "ohlcv" in self.datasets

    @property
    def depth_enabled(self):
        return "depth" in self.datasets


class DeadlineExceeded(TimeoutError):
    pass


def remaining_seconds(deadline: float | None, maximum: float = 30) -> float:
    remaining = (
        maximum if deadline is None else min(maximum, deadline - time.monotonic())
    )
    if remaining <= 0:
        raise DeadlineExceeded("per-date monotonic deadline exceeded")
    return remaining
