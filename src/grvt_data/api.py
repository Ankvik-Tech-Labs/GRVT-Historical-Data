"""Keyless production REST API with bounded retry and streamed deadlines."""

import json
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from .config import Config, Instrument, remaining_seconds


class APIError(RuntimeError):
    pass


class CursorRejected(APIError):
    pass


class GrvtAPI:
    def __init__(
        self,
        config: Config | None = None,
        *,
        deadline: float | None = None,
        client: httpx.Client | None = None,
    ):
        self.config = config or Config()
        if not 1 <= self.config.trade_page_limit <= 1000:
            raise ValueError("trade_page_limit must be 1..1000")
        if not 0 < self.config.requests_per_second <= 2:
            raise ValueError("requests_per_second must be > 0 and <= 2")
        if not 1 <= self.config.instrument_limit <= 100000:
            raise ValueError("instrument_limit must be 1..100000")
        if self.config.max_retries < 0 or self.config.request_timeout_seconds <= 0:
            raise ValueError("invalid retry or timeout configuration")
        self.deadline = deadline
        self.client = client or httpx.Client(
            headers={"User-Agent": "Mozilla/5.0 GRVT-Archive/0.1"}
        )
        self._next_request = 0.0

    def close(self):
        self.client.close()

    def check_deadline(self):
        remaining_seconds(self.deadline, self.config.request_timeout_seconds)

    def _sleep(self, seconds):
        if seconds > 0:
            time.sleep(min(seconds, remaining_seconds(self.deadline, seconds)))
        self.check_deadline()

    def _post(self, route: str, payload: dict) -> dict:
        for attempt in range(self.config.max_retries + 1):
            self._sleep(max(0, self._next_request - time.monotonic()))
            self._next_request = time.monotonic() + 1 / self.config.requests_per_second
            retry_after = 0.0
            try:
                with self.client.stream(
                    "POST",
                    f"{self.config.rest_url}/full/v1/{route}",
                    json=payload,
                    timeout=remaining_seconds(
                        self.deadline, self.config.request_timeout_seconds
                    ),
                ) as response:
                    chunks = []
                    for chunk in response.iter_bytes():
                        self.check_deadline()
                        chunks.append(chunk)
                    body = b"".join(chunks)
                    if response.status_code == 429 or response.status_code >= 500:
                        header = response.headers.get("Retry-After", "0")
                        try:
                            retry_after = max(0, float(header))
                        except ValueError:
                            try:
                                parsed = parsedate_to_datetime(header)
                                if parsed.tzinfo is None:
                                    parsed = parsed.replace(tzinfo=UTC)
                                retry_after = max(
                                    0, (parsed - datetime.now(UTC)).total_seconds()
                                )
                            except (ValueError, TypeError, OverflowError):
                                retry_after = 0.0
                        raise httpx.HTTPStatusError(
                            "retryable response",
                            request=response.request,
                            response=response,
                        )
                    if response.status_code >= 400:
                        message = body.decode(errors="replace")
                        if (
                            payload.get("cursor")
                            and response.status_code == 400
                            and "cursor" in message.lower()
                        ):
                            raise CursorRejected(message)
                        raise APIError(f"HTTP {response.status_code}: {message}")
                    result = json.loads(body)
                    unwrapped = {
                        "supported_assets": ("funding", "spot"),
                        "get_all_collateral_asset_info": ("assets",),
                        "margin_rules": ("instrument", "risk_brackets"),
                    }
                    if not isinstance(result, dict) or (
                        "result" not in result
                        and not (
                            route in unwrapped
                            and all(key in result for key in unwrapped[route])
                        )
                    ):
                        raise APIError(f"invalid API response: {result!r}")
                    return result
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                if attempt == self.config.max_retries:
                    raise APIError(f"retries exhausted: {exc}") from exc
                self._sleep(max(retry_after, min(2**attempt, 30)))
        raise AssertionError("unreachable")

    def instruments(self, *, active: bool) -> list[Instrument]:
        # The published endpoint has a limit but no cursor/offset. Ask for its
        # documented maximum, and fail closed if that cap is reached.
        response = self._post(
            "instruments",
            {
                "kind": ["PERPETUAL", "FUTURE", "CALL", "PUT", "STABLE_PERP"],
                "is_active": active,
                "limit": self.config.instrument_limit,
            },
        )
        rows = response["result"]
        if (
            not isinstance(rows, list)
            or len(rows) >= self.config.instrument_limit
            or response.get("next")
        ):
            raise APIError("instrument universe may be truncated")
        if any(not isinstance(row.get("instrument"), str) for row in rows):
            raise APIError("invalid instrument metadata")
        return rows

    def trade_pages(
        self, instrument: str, start_ns: int, end_ns: int, cursor: str = ""
    ) -> Iterator[dict]:
        seen = {cursor} if cursor else set()
        while True:
            response = self._post(
                "trade_history",
                {
                    "instrument": instrument,
                    "start_time": str(start_ns),
                    "end_time": str(end_ns),
                    "limit": self.config.trade_page_limit,
                    "cursor": cursor,
                },
            )
            if not isinstance(response["result"], list) or "next" not in response:
                raise APIError("trade response missing result/next")
            following = response["next"]
            if not isinstance(following, str) or (following and following in seen):
                raise APIError("repeated or invalid pagination cursor")
            yield response
            if not following:
                return
            seen.add(following)
            cursor = following

    def candle_page(
        self, instrument, interval, start_ns, end_ns, cursor="", *, price_type="TRADE"
    ):
        response = self._post(
            "kline",
            {
                "instrument": instrument,
                "interval": interval,
                "type": price_type,
                "start_time": str(start_ns),
                "end_time": str(end_ns),
                "limit": 1000,
                "cursor": cursor,
            },
        )
        if not isinstance(response["result"], list):
            raise APIError("candle response missing result list")
        response.setdefault("next", "")
        if not isinstance(response["next"], str):
            raise APIError("invalid candle cursor")
        return response

    def market_snapshot(self, route, instrument=None):
        allowed = {
            "all_instruments",
            "currency",
            "supported_assets",
            "get_all_collateral_asset_info",
            "margin_rules",
            "ticker",
            "mini",
        }
        if route not in allowed:
            raise ValueError("not a public non-depth snapshot endpoint")
        payload = {"instrument": instrument} if instrument is not None else {}
        if route == "all_instruments":
            payload = {
                "is_active": False,
                "kinds": ["PERPETUAL", "FUTURE", "CALL", "PUT", "STABLE_PERP"],
            }
        return self._post(route, payload)

    def funding_page(self, instrument, aggregation, start_ns, end_ns, cursor=""):
        response = self._post(
            "funding",
            {
                "instrument": instrument,
                "agg_type": aggregation,
                "start_time": str(start_ns),
                "end_time": str(end_ns),
                "cursor": cursor,
                "limit": 1000,
            },
        )
        if not isinstance(response["result"], list):
            raise APIError("funding response missing result list")
        response.setdefault("next", "")
        if not isinstance(response["next"], str):
            raise APIError("invalid funding cursor")
        return response
