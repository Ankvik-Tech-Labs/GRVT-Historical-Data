import json
from datetime import date

import pyarrow.parquet as pq
import pytest

from grvt_data.api import CursorRejected
from grvt_data.storage import write_json_atomic
from grvt_data.trades import day_universe, download_trades, window_bounds

DAY = date(2026, 10, 15)
START, END = window_bounds(DAY)


def trade(id, ts=START + 3600 * 10**9):
    return {
        "instrument": "BTC_USDT_Perp",
        "trade_id": id,
        "event_time": str(ts),
        "price": "81000.000000000001",
        "size": "0.00000001",
        "venue": "ORDERBOOK",
        "is_taker_buyer": True,
        "is_rpi": False,
    }


class API:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def trade_pages(self, instrument, start_ns, end_ns, cursor=""):
        self.calls.append(cursor)
        for page in self.pages[cursor]:
            if isinstance(page, BaseException):
                raise page
            yield page


def run(tmp_path, api, **kw):
    return download_trades(api, tmp_path, DAY, ["BTC_USDT_Perp"], **kw)


def result_rows(result):
    return pq.read_table(result["instruments"]["BTC_USDT_Perp"]["path"]).to_pylist()


def test_two_pages_keep_same_timestamp_distinct_ids(tmp_path):
    api = API(
        {
            "": [
                {
                    "result": [trade("d", END), trade("c", END - 1), trade("b")],
                    "next": "c",
                },
                {"result": [trade("b"), trade("a")], "next": ""},
            ]
        }
    )
    result = run(tmp_path, api)
    rows = result_rows(result)
    assert {r["trade_id"] for r in rows} == {"a", "b", "c"}
    assert rows[0]["price"] == "81000.000000000001"
    assert max(r["event_time"] for r in rows) == END - 1
    assert result["complete"]


def test_later_page_failure_resumes_without_false_completion(tmp_path):
    api = API(
        {"": [{"result": [trade("a")], "next": "c"}, RuntimeError("503 exhausted")]}
    )
    failed = run(tmp_path, api)
    assert not failed["complete"]
    assert failed["instruments"]["BTC_USDT_Perp"]["cursor"] == "c"
    resumed = API({"c": [{"result": [trade("b")], "next": ""}]})
    result = run(tmp_path, resumed)
    assert resumed.calls == ["c"]
    assert len(result_rows(result)) == 2
    assert result["complete"]


def test_delisted_market_remains_in_day_universe(tmp_path):
    write_json_atomic(
        tmp_path / "state/discovery/2026-10-15.json",
        {"instruments": [{"instrument": "OLD_USDT_Perp"}]},
    )
    assert day_universe(tmp_path, DAY, [{"instrument": "BTC_USDT_Perp"}]) == [
        "BTC_USDT_Perp",
        "OLD_USDT_Perp",
    ]


def test_pagination_completeness(tmp_path):
    # First execution hours after midnight is valid; cursor exhaustion proves coverage.
    result = run(tmp_path, API({"": [{"result": [trade("a")], "next": ""}]}))
    assert result["complete"]
    assert result["instruments"]["BTC_USDT_Perp"]["pagination_exhausted"]


def test_empty_day(tmp_path):
    result = run(tmp_path, API({"": [{"result": [], "next": ""}]}))
    assert result["complete"] and result["rows"] == 0
    assert result_rows(result) == []


def test_repeated_cursor(tmp_path):
    result = run(
        tmp_path,
        API({"": [{"result": [trade("a")], "next": "x"}, {"result": [], "next": "x"}]}),
    )
    assert not result["complete"]
    assert "cursor" in result["failures"]["BTC_USDT_Perp"]


def test_expired_resume_cursor_restarts_with_dedup(tmp_path):
    run(
        tmp_path,
        API({"": [{"result": [trade("a")], "next": "bad"}, RuntimeError("stop")]}),
    )
    api = API(
        {
            "bad": [CursorRejected("expired")],
            "": [{"result": [trade("a"), trade("b")], "next": ""}],
        }
    )
    result = run(tmp_path, api)
    assert api.calls == ["bad", ""]
    assert len(result_rows(result)) == 2 and result["complete"]


def test_page_resume_across_deadline_attempts(tmp_path):
    with pytest.raises(KeyboardInterrupt):
        run(
            tmp_path,
            API({"": [{"result": [trade("a")], "next": "c"}, KeyboardInterrupt()]}),
        )
    api = API({"c": [{"result": [trade("b")], "next": ""}]})
    assert run(tmp_path, api)["complete"]
    assert api.calls == ["c"]


def test_orphan_page_reconciled_after_checkpoint_crash(tmp_path, monkeypatch):
    import grvt_data.trades as module

    original = module.write_json_atomic

    def crash(path, payload):
        if path.name == "progress.json":
            raise KeyboardInterrupt()
        original(path, payload)

    monkeypatch.setattr(module, "write_json_atomic", crash)
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, API({"": [{"result": [trade("a")], "next": "c"}]}))
    monkeypatch.setattr(module, "write_json_atomic", original)
    api = API({"c": [{"result": [trade("b")], "next": ""}]})
    assert run(tmp_path, api)["rows"] == 2
    assert api.calls == ["c"]


def test_partial_page_temp_is_ignored(tmp_path):
    run(
        tmp_path,
        API({"": [{"result": [trade("a")], "next": "c"}, RuntimeError("stop")]}),
    )
    page_dir = next(tmp_path.glob("trades/*/*/*/pages"))
    (page_dir / ".partial.tmp").write_text("{broken")
    assert run(tmp_path, API({"c": [{"result": [trade("b")], "next": ""}]}))["complete"]


def test_invalid_row_does_not_advance_cursor(tmp_path):
    malformed = trade("a")
    malformed["price"] = 1.0
    result = run(tmp_path, API({"": [{"result": [malformed], "next": "c"}]}))
    assert not result["complete"]
    assert result["instruments"]["BTC_USDT_Perp"]["pages"] == 0


def test_query_window_identity_and_completed_noop(tmp_path):
    api = API({"": [{"result": [], "next": ""}]})
    run(tmp_path, api)
    assert run(tmp_path, API({}))["complete"]
    state = next(tmp_path.glob("trades/*/*/*/progress.json"))
    assert json.loads(state.read_text())["end_ns"] == END


def test_silently_ignored_resume_cursor_restarts_once(tmp_path):
    first = {"result": [trade("a")], "next": "saved"}
    run(tmp_path, API({"": [first, RuntimeError("interrupted")]}))
    api = API(
        {
            "saved": [{"result": [trade("a")], "next": "changed"}],
            "": [first, {"result": [trade("b", START)], "next": ""}],
        }
    )
    result = run(tmp_path, api)
    assert api.calls == ["saved", ""]
    assert result["complete"] and result["rows"] == 2


def test_ignored_cursor_cannot_loop_forever_or_claim_complete(tmp_path):
    first = {"result": [trade("a")], "next": "x"}
    api = API({"": [first, {"result": [trade("a")], "next": "y"}]})
    result = run(tmp_path, api)
    assert not result["complete"]
    assert api.calls == ["", ""]  # bounded to one fresh query per attempt


def test_activation_and_full_day_have_distinct_checkpoints(tmp_path, monkeypatch):
    from datetime import UTC, datetime

    import grvt_data.trades as module

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 15, 12, tzinfo=UTC)

    monkeypatch.setattr(module, "datetime", Clock)
    partial = run(
        tmp_path,
        API({"": [{"result": [trade("a")], "next": ""}]}),
        end_ns=START + 3600 * 10**9,
    )
    monkeypatch.setattr(module, "datetime", datetime)
    full = run(tmp_path, API({"": [{"result": [trade("b")], "next": ""}]}))
    assert partial["provisional"]
    assert (
        partial["instruments"]["BTC_USDT_Perp"]["path"]
        != full["instruments"]["BTC_USDT_Perp"]["path"]
    )
    assert full["rows"] == 1


def test_http_retry_after_exhaustion_preserves_previous_page(tmp_path, monkeypatch):
    import httpx

    from grvt_data.api import GrvtAPI
    from grvt_data.config import Config

    requests = []
    waits = []

    def handler(request):
        payload = json.loads(request.content)
        requests.append(payload)
        if not payload["cursor"]:
            return httpx.Response(200, json={"result": [trade("a")], "next": "saved"})
        return httpx.Response(
            429 if len(requests) == 2 else 503, headers={"Retry-After": "3"}
        )

    api = GrvtAPI(
        Config(max_retries=1),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    monkeypatch.setattr(api, "_sleep", waits.append)
    result = run(tmp_path, api)
    assert not result["complete"]
    assert result["instruments"]["BTC_USDT_Perp"]["cursor"] == "saved"
    assert len(requests) == 3 and 3 in waits
    assert requests[0]["start_time"] == str(START) and requests[0]["end_time"] == str(
        END
    )
    assert requests[0]["limit"] == 1000


def test_stream_deadline_does_not_commit_partial_page(tmp_path, monkeypatch):
    import httpx

    import grvt_data.config as module
    from grvt_data.api import GrvtAPI

    tick = [1.0]

    class Trickle(httpx.SyncByteStream):
        def __iter__(self):
            yield b'{"result":['
            tick[0] = 4
            yield b'] ,"next":""}'

    api = GrvtAPI(
        deadline=3,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=Trickle())
            )
        ),
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: tick[0])
    result = run(tmp_path, api)
    assert not result["complete"]
    assert result["instruments"]["BTC_USDT_Perp"]["pages"] == 0
    assert not list(tmp_path.glob("trades/*/*/*/pages/*.json"))


def test_instrument_limit_saturation_fails_closed(monkeypatch):
    import httpx

    from grvt_data.api import APIError, GrvtAPI
    from grvt_data.config import Config

    api = GrvtAPI(
        Config(instrument_limit=2),
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"result": [{"instrument": "A"}, {"instrument": "B"}]}
                )
            )
        ),
    )
    with pytest.raises(APIError, match="truncated"):
        api.instruments(active=True)


def test_transport_partial_page_retry_is_safe(tmp_path, monkeypatch):
    import httpx

    from grvt_data.api import GrvtAPI
    from grvt_data.config import Config

    class Broken(httpx.SyncByteStream):
        def __iter__(self):
            yield b'{"result":['
            raise httpx.ReadError("connection lost")

    attempts = []

    def handler(request):
        attempts.append(request)
        if len(attempts) == 1:
            return httpx.Response(200, stream=Broken())
        return httpx.Response(200, json={"result": [trade("a")], "next": ""})

    api = GrvtAPI(
        Config(max_retries=1),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    monkeypatch.setattr(api, "_sleep", lambda _: None)
    result = run(tmp_path, api)
    assert result["complete"] and result["rows"] == 1
    assert result["instruments"]["BTC_USDT_Perp"]["pages"] == 1


def test_atomic_json_and_parquet_preserve_previous_output_on_failure(
    tmp_path, monkeypatch
):
    from grvt_data import storage

    json_path = tmp_path / "state.json"
    write_json_atomic(json_path, {"cursor": "durable"})
    with pytest.raises(TypeError):
        write_json_atomic(json_path, {"cursor": object()})
    assert json.loads(json_path.read_text()) == {"cursor": "durable"}
    parquet_path = tmp_path / "trades.parquet"
    storage.write_parquet_atomic(parquet_path, [], storage.TRADE_SCHEMA)
    original_bytes = parquet_path.read_bytes()

    def broken_write(*args, **kwargs):
        raise OSError("disk write failed")

    monkeypatch.setattr(storage.pq, "write_table", broken_write)
    with pytest.raises(OSError):
        storage.write_parquet_atomic(parquet_path, [], storage.TRADE_SCHEMA)
    assert parquet_path.read_bytes() == original_bytes
    assert not list(tmp_path.glob(".*.tmp"))


def test_corrupt_page_failure_does_not_block_other_instruments(tmp_path):
    directory = (
        tmp_path / "trades" / str(DAY) / "BAD_USDT_Perp" / f"{START}-{END}" / "pages"
    )
    directory.mkdir(parents=True)
    (directory / "0000000000.json").write_text("{broken")
    result = download_trades(
        API({"": [{"result": [trade("a")], "next": ""}]}),
        tmp_path,
        DAY,
        ["BAD_USDT_Perp", "BTC_USDT_Perp"],
    )
    assert not result["complete"]
    assert "BAD_USDT_Perp" in result["failures"]
    assert result["instruments"]["BTC_USDT_Perp"]["complete"]


def test_expired_cursor_http_400_is_classified(monkeypatch):
    import httpx

    from grvt_data.api import GrvtAPI

    api = GrvtAPI(
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(400, json={"message": "cursor expired"})
            )
        )
    )
    with pytest.raises(CursorRejected):
        list(api.trade_pages("BTC_USDT_Perp", START, END, "old"))


def test_live_fixture_preserves_time_bound_and_visibility_evidence():
    from pathlib import Path

    probes = json.loads(
        (
            Path(__file__).parent / "fixtures/trades/production_probe_2026-10-16.json"
        ).read_text()
    )
    bounded = [entry for entry in probes if "start_time" in entry["payload"]]
    assert [bool(entry["response"]["result"]) for entry in bounded] == [
        True,
        True,
        True,
        False,
    ]
    assert all(entry["response"]["next"] == "" for entry in bounded)
    assert len(probes[0]["response"]["result"]) == 2
    assert len(probes[1]["response"]["result"]) == 184
    first_ts = int(probes[3]["response"]["result"][0]["event_time"])
    last = probes[-1]
    newest = last["response"]["result"][0]
    assert int(newest["event_time"]) > first_ts
    assert newest["trade_id"] == "207195872-1"
    assert last["received_ns"] - int(newest["event_time"]) == 7826196144
