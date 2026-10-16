# GRVT `v1.book.d` sequence evidence

`book_d_btc_aaoi_reconnect_full.jsonl.gz` is the **full, untrimmed** capture
of production `wss://market-data.grvt.io/ws/full`, stream `v1.book.d`,
selectors `BTC_USDT_Perp@100` (liquid) and `AAOI_USDT_Perp@100` (quiet
equity). It was captured 2026-10-16 with
`scripts/research/capture_depth_protocol.py` run against the `websockets`
package already present in `/home/avik/freqtrade-multistrategy-orchestrator/.venv`
— no product dependency was installed in any global environment to produce
this evidence. Gzip is for size only (4.25 MB raw → 328 KB); every number
below is computed directly from this file, split by session at the recorded
`close_forced` boundary — not from the subset previously committed.

Each line is `{"t_recv_ns": <int>, "dir": "send"|"recv"|"close_forced", "msg": {...}}`.

## Two sessions

| | Session 1 (initial) | Session 2 (after forced reconnect) |
|---|---|---|
| Duration | ~4 min | ~30 s |
| ACK `first_sequence_number` | `["964934", "175024"]` | `["933302", "234505"]` |
| ACK `latest_sequence_number` | `["964933", "175023"]` | `["933301", "234504"]` |
| BTC deltas (post-snapshot) | 2106 | 253 |
| BTC last `sequence_number` | 967039 | 933554 |
| AAOI deltas (post-snapshot) | 588 | 83 |
| AAOI last `sequence_number` | 175611 | 234587 |
| Non-contiguous sequence jumps | 0 | 0 |
| Largest inter-message gap, BTC | 0.82 s | 0.50 s |
| Largest inter-message gap, AAOI | **4.42 s** | 2.30 s |

Total: 3039 raw events, 3030 post-snapshot deltas (2106+253 BTC, 588+83 AAOI)
across both sessions, **zero gaps, zero duplicates, zero out-of-order
messages**.

## What the evidence shows

1. **ACK carries both bounds, within one session.** The subscribe response's
   `result` includes parallel arrays `first_sequence_number` and
   `latest_sequence_number`, one entry per selector, in selector order. For
   every instrument in both sessions, `first_sequence_number ==
   latest_sequence_number + 1`. Verbatim session 1 ACK (do not mix fields
   across sessions — an earlier draft of this document spliced session 1's
   `first_sequence_number` with session 2's `latest_sequence_number`, which
   fails the `first == latest + 1` check; this corrected version was
   re-derived directly from the file):
   ```json
   {"first_sequence_number": ["964934", "175024"],
    "latest_sequence_number": ["964933", "175023"]}
   ```
   `first_sequence_number` is exactly the `sequence_number` of the first
   delta that follows the snapshot.

2. **The snapshot's `sequence_number` is the literal string `"0"`.** This is
   not the sequence the book is "as of" — that is `latest_sequence_number`
   from the ACK. The zero is a sentinel marking "this message is a full
   snapshot," never a real per-update sequence value.

3. **Sequences are per-instrument (per selector), not per-connection.** Two
   instruments subscribed on the same socket had unrelated, non-overlapping
   sequence ranges (BTC in the 960000s/930000s, AAOI in the 170000s/230000s)
   and advanced independently.

4. **Sequences are strictly contiguous per instrument within one session.**
   Across every post-snapshot delta in both sessions (table above), each
   delta's `sequence_number` was exactly `previous + 1`; zero gaps, zero
   duplicates, zero out-of-order messages were observed.

5. **A gap is only evidenced at connection granularity, not resolved
   per-selector.** This capture never exercised a single-selector gap on a
   live multi-instrument socket — the only reconnect behavior actually
   observed is a full connection close + a fresh `subscribe` for every
   selector, which produced a new, lower-valued sequence range (point 6).
   **Design decision (adopted, not inferred from this evidence):** on any
   sequence gap, reconnect the entire affected connection — not just the
   gapped instrument — open a new connection, require a fresh ACK and
   snapshot for every selector on it, and record an outage for every
   instrument that was on that connection. A delta that arrives for an
   instrument before that instrument's own snapshot on the new connection is
   a protocol violation and forces another reconnect. This is the only
   behavior this fixture evidences; a narrower per-selector resubscribe-on-a-
   live-socket mechanism remains unevidenced and is not implemented.
   `session_id` is the identity of the connection's subscribe ACK (one ACK
   covers every selector on that connection), not a per-instrument value.

6. **Sequence numbers do not survive a reconnect and must never be compared
   across sessions.** After the forced close/resubscribe, BTC's new
   `first_sequence_number` (933302) was far *lower* than the last sequence
   number seen on the old connection (967039) seconds earlier. The counter is
   scoped to the subscription session, not a single globally monotonic
   exchange-wide counter. Any resync logic that compares a new session's
   sequence against the previous session's last-seen value (instead of
   discarding it and waiting for the new ACK + snapshot) will misfire.

7. **Zero-size levels are deletions**, confirming the spec's existing
   assumption: delta messages publish `{"price": ..., "size": "0.0",
   "num_orders": 0}` for a removed level and a nonzero `size`/`num_orders`
   for an added or changed one.

8. **Quiet instruments go several seconds between book changes with no
   fault.** AAOI's largest gap between consecutive messages was 4.42s in
   session 1 (vs. BTC's 0.82s in the same session), even though the
   subscription and connection were healthy throughout. A per-instrument "no
   message in N seconds ⇒ stalled" check would false-positive on quiet
   equities at any N below roughly 5s observed here. Liveness must be judged
   by WebSocket ping/pong (or a gateway heartbeat), never by per-instrument
   message silence; sequence continuity alone judges book validity.

9. **Not evidenced by this capture, and not claimed:** whether deltas
   arriving between a fresh ACK and the fresh snapshot on a reconnect are
   buffered or discarded by the client — in both sessions here, the snapshot
   was the very first message after the ACK, so the client never had to
   choose. The design decision in point 5 (treat a pre-snapshot delta as a
   protocol violation) is adopted precisely because this ordering, while
   consistent in what was observed, was never stress-tested against a
   reordered arrival.

## Message schema

Delta/snapshot messages (`dir: "recv"`, `msg.selector` present) have the shape:

```json
{"stream": "v1.book.d", "selector": "<instrument>@<rate>",
 "sequence_number": "<string int, \"0\" for the snapshot>",
 "feed": {"event_time": "<nanosecond string>", "instrument": "<name>",
          "bids": [{"price": "...", "size": "...", "num_orders": <int>}, ...],
          "asks": [...]}}
```
