import asyncio
import json
import time
import sys

import websockets

WS_URL = "wss://market-data.grvt.io/ws/full"
INSTRUMENTS = ["BTC_USDT_Perp", "AAOI_USDT_Perp"]
RATE_MS = 100
OUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/grvt_depth_capture.jsonl"
DURATION_S = int(sys.argv[2]) if len(sys.argv) > 2 else 180


async def subscribe(ws, instruments, req_id):
    selectors = [f"{inst}@{RATE_MS}" for inst in instruments]
    msg = {
        "jsonrpc": "2.0",
        "method": "subscribe",
        "params": {"stream": "v1.book.d", "selectors": selectors},
        "id": req_id,
    }
    await ws.send(json.dumps(msg))
    return msg


async def run():
    events = []

    def record(direction, payload, recv_ns):
        events.append({"t_recv_ns": recv_ns, "dir": direction, "msg": payload})

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
        sub_msg = await subscribe(ws, INSTRUMENTS, 1)
        record("send", sub_msg, time.time_ns())
        start = time.monotonic()
        ack_seen = False
        while time.monotonic() - start < DURATION_S:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue
            recv_ns = time.time_ns()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"_raw": raw}
            record("recv", payload, recv_ns)
            if not ack_seen and payload.get("method") == "subscribe":
                ack_seen = True
                print("ACK:", json.dumps(payload), file=sys.stderr)

        # Forced reconnect: close this socket mid-stream and resubscribe on a fresh one.
        record("close_forced", {"reason": "test_forced_reconnect"}, time.time_ns())

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws2:
        sub_msg2 = await subscribe(ws2, INSTRUMENTS, 2)
        record("send", sub_msg2, time.time_ns())
        start2 = time.monotonic()
        while time.monotonic() - start2 < 30:
            try:
                raw = await asyncio.wait_for(ws2.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue
            recv_ns = time.time_ns()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"_raw": raw}
            record("recv", payload, recv_ns)
            if payload.get("method") == "subscribe":
                print("RECONNECT ACK:", json.dumps(payload), file=sys.stderr)

    with open(OUT_PATH, "w") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"wrote {len(events)} events to {OUT_PATH}", file=sys.stderr)


asyncio.run(run())
