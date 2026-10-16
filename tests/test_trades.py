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


def test_too_early_query_waits_indexing_grace_and_records_actual_start(
    tmp_path, monkeypatch
):
    import grvt_data.trades as module

    clock = [END + 5 * 10**9]
    slept = []
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])

    def sleep(seconds):
        slept.append(seconds)
        clock[0] += int(seconds * 10**9)

    monkeypatch.setattr(module.time, "sleep", sleep)
    api = API({"": [{"result": [], "next": ""}]})
    result = run(tmp_path, api)
    assert sum(slept) == 25
    assert result["query_started_at_ns"] == END + 30 * 10**9
    assert result["indexing_grace_seconds"] == 30
    assert result["complete"]


def test_expired_deadline_during_indexing_wait_never_completes(tmp_path, monkeypatch):
    import time

    import grvt_data.trades as module
    from grvt_data.config import DeadlineExceeded

    monkeypatch.setattr(module.time, "time_ns", lambda: END + 5 * 10**9)
    api = API({"": [{"result": [], "next": ""}]})
    api.deadline = time.monotonic() - 1
    with pytest.raises(DeadlineExceeded):
        run(tmp_path, api)
    assert not api.calls
    assert not list(tmp_path.rglob("*.parquet"))


def test_complete_cache_keeps_actual_early_query_and_cannot_be_recertified(
    tmp_path, monkeypatch
):
    import grvt_data.trades as module
    from grvt_data.archive import seal_revision
    from grvt_data.config import Config

    clock = [END + 10**9]
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])
    initial = API({"": [{"result": [], "next": ""}]})
    initial.config = Config(data_dir=tmp_path, trade_indexing_lag_seconds=0)
    run(tmp_path, initial)
    clock[0] = END + 31 * 10**9
    retry = API({"": [{"result": [trade("late")], "next": ""}]})
    retry.config = Config(data_dir=tmp_path)
    result = run(tmp_path, retry)
    assert retry.calls == []
    assert result["rows"] == 0
    assert result["query_started_at_ns"] == END + 10**9
    assert result["indexing_grace_seconds"] == 0
    with pytest.raises(ValueError, match="indexing lag"):
        seal_revision(retry.config, DAY, result)


def test_interrupted_page_resume_preserves_earliest_query_and_page_evidence(
    tmp_path, monkeypatch
):
    import grvt_data.trades as module
    from grvt_data.archive import seal_revision
    from grvt_data.config import Config

    clock = [END + 31 * 10**9]
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])
    first = API(
        {"": [{"result": [trade("a")], "next": "c"}, RuntimeError("interrupted")]}
    )
    result = run(tmp_path, first)
    page = next(tmp_path.rglob("pages/0000000000.json"))
    durable = page.read_bytes()
    assert not result["complete"]
    clock[0] = END + 61 * 10**9
    retry = API({"c": [{"result": [trade("b")], "next": ""}]})
    resumed = run(tmp_path, retry)
    assert retry.calls == ["c"] and resumed["complete"]
    assert resumed["query_started_at_ns"] == END + 31 * 10**9
    assert resumed["instruments"]["BTC_USDT_Perp"]["queried_pages"] == 2
    assert (
        json.loads(page.read_text())["query_provenance"]["query_started_at_ns"]
        == END + 31 * 10**9
    )
    assert page.read_bytes() == durable
    assert seal_revision(Config(data_dir=tmp_path), DAY, resumed).exists()


@pytest.mark.parametrize("partial", [False, True])
def test_missing_legacy_page_provenance_is_not_relabelled_on_reuse(
    tmp_path, monkeypatch, partial
):
    import grvt_data.trades as module
    from grvt_data.archive import seal_revision
    from grvt_data.config import Config

    clock = [END + 31 * 10**9]
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])
    pages = [{"result": [trade("a")], "next": "c" if partial else ""}]
    if partial:
        pages.append(RuntimeError("interrupted"))
    run(tmp_path, API({"": pages}))
    # Simulate a legacy page and a misleadingly fresh progress checkpoint.
    page = next(tmp_path.rglob("pages/0000000000.json"))
    envelope = json.loads(page.read_text())
    envelope.pop("query_provenance", None)
    write_json_atomic(page, envelope)
    progress = next(tmp_path.rglob("progress.json"))
    state = json.loads(progress.read_text())
    state.update(query_started_at_ns=END + 90 * 10**9, query_provenance_complete=True)
    write_json_atomic(progress, state)
    clock[0] = END + 91 * 10**9
    retry = API({"c": [{"result": [trade("b")], "next": ""}]})
    result = run(tmp_path, retry)
    assert result["query_started_at_ns"] is None
    assert not result["query_provenance_complete"]
    assert retry.calls == (["c"] if partial else [])
    with pytest.raises(ValueError, match="query-start|provenance"):
        seal_revision(Config(data_dir=tmp_path), DAY, result)


def test_interrupted_early_page_cannot_be_hidden_by_new_page_or_fresh_report(
    tmp_path, monkeypatch
):
    import grvt_data.trades as module
    from grvt_data.archive import seal_revision
    from grvt_data.config import Config

    clock = [END + 10**9]
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])
    initial = API(
        {"": [{"result": [trade("a")], "next": "c"}, RuntimeError("interrupted")]}
    )
    initial.config = Config(data_dir=tmp_path, trade_indexing_lag_seconds=0)
    assert not run(tmp_path, initial)["complete"]
    clock[0] = END + 31 * 10**9
    retry = API({"c": [{"result": [trade("b")], "next": ""}]})
    result = run(tmp_path, retry)
    assert retry.calls == ["c"] and result["complete"]
    assert result["query_started_at_ns"] == END + 10**9
    # Even an incorrectly rewritten top-level summary cannot hide the earlier
    # relevant instrument data from the sealer's independent per-state check.
    result["query_started_at_ns"] = clock[0]
    with pytest.raises(ValueError, match="instrument query.*indexing lag"):
        seal_revision(Config(data_dir=tmp_path), DAY, result)


def test_complete_cache_preserves_valid_provenance_and_makes_no_requests(
    tmp_path, monkeypatch
):
    import grvt_data.trades as module

    clock = [END + 31 * 10**9]
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])
    first = run(tmp_path, API({"": [{"result": [trade("a")], "next": ""}]}))
    clock[0] = END + 3600 * 10**9
    retry = API({})
    result = run(tmp_path, retry)
    assert result["complete"] and result["query_provenance_complete"]
    assert not retry.calls
    assert result["query_started_at_ns"] == first["query_started_at_ns"]
    assert result["instruments"] == first["instruments"]


def test_failed_legacy_resume_rebuilds_missing_provenance_before_sealing(
    tmp_path, monkeypatch
):
    import grvt_data.trades as module
    from grvt_data.archive import seal_revision
    from grvt_data.config import Config

    clock = [END + 31 * 10**9]
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])
    run(
        tmp_path,
        API(
            {"": [{"result": [trade("a")], "next": "c"}, RuntimeError("first failure")]}
        ),
    )
    page = next(tmp_path.rglob("pages/0000000000.json"))
    envelope = json.loads(page.read_text())
    assert envelope["state"]["query_provenance_complete"]
    del envelope["query_provenance"]
    write_json_atomic(page, envelope)
    clock[0] = END + 61 * 10**9
    retry = API({"c": [RuntimeError("resume failed before new page")]})
    result = run(tmp_path, retry)
    assert retry.calls == ["c"] and not result["complete"]
    state = result["instruments"]["BTC_USDT_Perp"]
    assert state["pages"] == state["queried_pages"] == 1
    assert state["query_started_at_ns"] is None
    assert not state["query_provenance_complete"]
    assert result["query_started_at_ns"] is None
    assert not result["query_provenance_complete"]
    progress = json.loads(next(tmp_path.rglob("progress.json")).read_text())
    assert (
        progress["query_started_at_ns"] is None
        and not progress["query_provenance_complete"]
    )
    with pytest.raises(ValueError, match="query-start provenance"):
        seal_revision(Config(data_dir=tmp_path), DAY, result)


@pytest.mark.parametrize("durable_write", [False, True])
def test_failed_page_write_recovers_only_actual_durable_page_evidence(
    tmp_path, monkeypatch, durable_write
):
    import grvt_data.trades as module
    from grvt_data.config import Config

    clock = [END + 31 * 10**9]
    monkeypatch.setattr(module.time, "time_ns", lambda: clock[0])
    run(
        tmp_path,
        API({"": [{"result": [trade("a")], "next": "c"}, RuntimeError("interrupted")]}),
    )
    original = module.write_json_atomic

    def fail(path, payload):
        if path.parent.name == "pages" and path.name == "0000000001.json":
            if durable_write:
                original(path, payload)
            raise OSError("page write reported failure")
        original(path, payload)

    monkeypatch.setattr(module, "write_json_atomic", fail)
    clock[0] = END + 61 * 10**9
    retry = API({"c": [{"result": [trade("b")], "next": ""}]})
    retry.config = Config(data_dir=tmp_path, trade_indexing_lag_seconds=0)
    result = run(tmp_path, retry)
    assert not result["complete"]
    state = result["instruments"]["BTC_USDT_Perp"]
    assert state["pages"] == state["queried_pages"] == (2 if durable_write else 1)
    assert state["rows"] == (2 if durable_write else 1)
    assert state["cursor"] == ("" if durable_write else "c")
    assert state["query_provenance_complete"]
    assert state["query_started_at_ns"] == END + 31 * 10**9
    assert state["indexing_grace_seconds"] == (0 if durable_write else 30)
    assert result["indexing_grace_seconds"] == (0 if durable_write else 30)
    monkeypatch.setattr(module, "write_json_atomic", original)
    continuation = API({"c": [{"result": [trade("b")], "next": ""}]})
    completed = run(tmp_path, continuation)
    assert completed["complete"] and completed["rows"] == 2
    assert continuation.calls == ([] if durable_write else ["c"])
    assert completed["instruments"]["BTC_USDT_Perp"]["queried_pages"] == 2
    assert completed["indexing_grace_seconds"] == (0 if durable_write else 30)


@pytest.mark.parametrize("durable_write", [False, True])
def test_first_page_write_failure_uses_durable_envelope_not_candidate(
    tmp_path, monkeypatch, durable_write
):
    import grvt_data.trades as module

    monkeypatch.setattr(module.time, "time_ns", lambda: END + 31 * 10**9)
    original = module.write_json_atomic

    def fail(path, payload):
        if path.parent.name == "pages":
            if durable_write:
                original(path, payload)
            raise OSError("first page write reported failure")
        original(path, payload)

    monkeypatch.setattr(module, "write_json_atomic", fail)
    result = run(tmp_path, API({"": [{"result": [trade("a")], "next": "c"}]}))
    state = result["instruments"]["BTC_USDT_Perp"]
    assert not result["complete"]
    assert state["pages"] == state["queried_pages"] == (1 if durable_write else 0)
    assert state["rows"] == (1 if durable_write else 0)
    assert state["query_started_at_ns"] == (END + 31 * 10**9 if durable_write else None)
    assert state["indexing_grace_seconds"] == (30 if durable_write else None)
    monkeypatch.setattr(module, "write_json_atomic", original)
    cursor = "c" if durable_write else ""
    continuation = API(
        {cursor: [{"result": [] if durable_write else [trade("a")], "next": ""}]}
    )
    completed = run(tmp_path, continuation)
    assert continuation.calls == [cursor]
    assert completed["complete"] and completed["rows"] == 1
    assert completed["query_provenance_complete"]


def test_valid_failed_resume_can_seal_only_incomplete_coverage(tmp_path, monkeypatch):
    import grvt_data.trades as module
    from grvt_data.archive import retry_queue, seal_revision
    from grvt_data.config import Config

    monkeypatch.setattr(module.time, "time_ns", lambda: END + 31 * 10**9)
    run(
        tmp_path,
        API(
            {"": [{"result": [trade("a")], "next": "c"}, RuntimeError("first failure")]}
        ),
    )
    result = run(tmp_path, API({"c": [RuntimeError("another failure")]}))
    assert not result["complete"] and result["query_provenance_complete"]
    config = Config(data_dir=tmp_path)
    bundle = seal_revision(config, DAY, result)
    manifest = json.loads((bundle / "manifest-rev01.json").read_text())
    assert not manifest["trades"]["full_day_complete"]
    assert manifest["trades"]["provisional"]
    assert not manifest["trades"]["instruments"]["BTC_USDT_Perp"]["complete"]
    assert str(DAY) in retry_queue(config)["dates"]
