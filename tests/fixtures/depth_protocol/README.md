# GRVT `v1.book.d` sequence evidence

`book_d_btc_aaoi_reconnect.jsonl` is a trimmed, scrubbed capture of production
`wss://market-data.grvt.io/ws/full`, stream `v1.book.d`, selectors
`BTC_USDT_Perp@100` (liquid) and `AAOI_USDT_Perp@100` (quiet equity). It was
captured 2026-10-16 with `scripts/research/capture_depth_protocol.py` run
against the `websockets` package already present in
`/home/avik/freqtrade-multistrategy-orchestrator/.venv` — no product
dependency was installed in any global environment to produce this evidence.

Full, untrimmed capture: 3039 raw events over ~4.5 minutes (2361 BTC
messages, 673 AAOI messages, two subscribe ACKs, one forced reconnect). This
file keeps both ACKs, the forced-reconnect boundary, and the first ~90
(session 1) / ~60 (session 2) messages per instrument — enough to show the
snapshot, the first deltas after it, and the reconnect behavior, without
committing the full multi-minute capture.

## What the evidence shows

1. **ACK carries both bounds.** The subscribe response's `result` includes
   parallel arrays `first_sequence_number` and `latest_sequence_number`, one
   entry per selector, in selector order:
   ```json
   {"first_sequence_number": ["964934", "175024"],
    "latest_sequence_number": ["933301", "234504"]}
   ```
   For every instrument observed, `first_sequence_number == latest_sequence_number + 1`.
   `first_sequence_number` is exactly the `sequence_number` of the first delta
   that follows the snapshot.

2. **The snapshot's `sequence_number` is the literal string `"0"`.** This is
   not the sequence the book is "as of" — that is `latest_sequence_number`
   from the ACK. The zero is a sentinel marking "this message is a full
   snapshot," never a real per-update sequence value.

3. **Sequences are per-instrument (per selector), not per-connection.** Two
   instruments subscribed on the same socket had unrelated, non-overlapping
   sequence ranges (BTC in the 964934+ range, AAOI in the 175024+ range) and
   advanced independently. A gap on one instrument's sequence therefore only
   invalidates that instrument's book, not the whole multiplexed connection.

4. **Sequences are strictly contiguous per instrument within one session.**
   Across 2359 BTC and 671 AAOI post-snapshot deltas (2361 and 673 total
   messages per instrument include one sentinel snapshot per session), every
   delta's `sequence_number` was exactly `previous + 1`; zero gaps, zero
   duplicates, zero out-of-order messages were observed. **Gap predicate:** maintain
   `last_sequence` per `(session_id, instrument)`; the first delta after a
   snapshot must equal the ACK's `first_sequence_number` for that selector;
   every later delta must equal `last_sequence + 1`. Anything else — a skip
   forward, a repeat, or a value less than expected — is a gap and must
   trigger a fresh subscribe (new session) and a fresh snapshot for that
   instrument. Treat the sentinel `"0"` snapshot sequence as not participating
   in that arithmetic.

5. **Sequence numbers do not survive a reconnect and must never be compared
   across sessions.** After the forced close/resubscribe, BTC's new
   `first_sequence_number` (933302) was *lower* than the last sequence number
   seen on the old connection (933554) seconds earlier. The counter is scoped
   to the subscription session, not a single globally monotonic exchange-wide
   counter. Any resync logic that compares a new session's sequence against
   the previous session's last-seen value (instead of discarding it and
   waiting for the new ACK + snapshot) will misfire.

6. **Zero-size levels are deletions**, confirming the spec's existing
   assumption: delta messages publish `{"price": ..., "size": "0.0",
   "num_orders": 0}` for a removed level and a nonzero `size`/`num_orders`
   for an added or changed one.

7. **Quiet instruments go several seconds between book changes with no
   fault.** AAOI's largest gap between consecutive messages in this capture
   was 4.42s (vs. BTC's 2.72s), even though the subscription and connection
   were healthy throughout. A per-instrument "no message in N seconds ⇒
   stalled" check would false-positive on quiet equities at any N below
   roughly 5s observed here, and lower thresholds will misfire more often
   under normal conditions on genuinely illiquid instruments. This confirms
   R3: liveness must be judged by WebSocket ping/pong (or a gateway
   heartbeat), never by per-instrument message silence; sequence continuity
   alone judges book validity.

## Fields

Each line is `{"t_recv_ns": <int>, "dir": "send"|"recv"|"close_forced", "msg": {...}}`.
Delta/snapshot messages (`dir: "recv"`, `msg.selector` present) have the shape:

```json
{"stream": "v1.book.d", "selector": "<instrument>@<rate>",
 "sequence_number": "<string int, \"0\" for the snapshot>",
 "feed": {"event_time": "<nanosecond string>", "instrument": "<name>",
          "bids": [{"price": "...", "size": "...", "num_orders": <int>}, ...],
          "asks": [...]}}
```
