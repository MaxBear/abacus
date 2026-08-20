# The WebSocket layer

Design intent for abacus' chat transport, written at phase 1 and meant to hold through phase 6.
This is the target shape, not the current state — see [Phasing](#phasing) for what exists today.

The rule everything else follows from:

> **The socket is a view onto durable state. It never owns anything.**

A user asks a question, the LLM turns it into an `AnalysisRequest`, a worker spends between thirty
seconds and five minutes on a numpy solve, and the answer arrives. The connection cannot be the
thing that holds that work together. Laptops sleep, tunnels drop, pods roll during a deploy. If a
dropped socket can lose a five-minute solve, the product is broken in a way no amount of reconnect
logic repairs. So: every frame the client receives must be reconstructible from Postgres, and the
socket's only job is to deliver frames sooner than polling would.

That single constraint is what makes the rest of this document mostly bookkeeping.

## Why WebSocket rather than SSE

Server-sent events plus `POST /messages` is the credible alternative, and it is genuinely easier to
operate: it is ordinary HTTP, it reconnects itself with `Last-Event-ID`, and every proxy on the path
already understands it. It is worth naming that, because "we picked WebSocket" usually goes
undefended.

WebSocket wins here on three counts:

- **One stream, several event sources.** A turn produces assistant tokens, job state transitions
  (`queued → running → done`), and progress from the worker. With SSE these are one event stream
  that the client must correlate with a separate POST's response. Over a socket they are frames on
  the same ordered channel, with one sequence number space.
- **Cancellation is a client→server message.** Stopping an in-flight solve over SSE means a second
  HTTP call that has to find the right job by ID; over the socket it is `{"type":"cancel"}` on the
  connection that is already scoped to the session.
- **It is the phase-6 shape.** Progress streaming from workers is the direction this goes, and
  retrofitting a bidirectional transport later is worse than paying for it now.

The cost is real and is paid in the operational sections below: proxies idle-timeout silent
connections, load balancers do not drain them, and there is no `Last-Event-ID` — resume is ours to
build.

## Endpoint and identity

```
GET /ws/chat/{session_id}      Upgrade: websocket
```

`session_id` is a UUID minted by `POST /sessions`. It is in the path rather than negotiated after
`accept()` because it makes the connection's scope legible in access logs and lets authorization run
before the handshake completes.

### Authenticating before accept

Browsers cannot set headers on a WebSocket handshake — there is no `Authorization` on
`new WebSocket(...)`. Two options survive that:

1. **Cookie**, if the client is same-site. Free, and the browser sends it on the upgrade request.
2. **Ticket**: `POST /ws/ticket` returns a single-use, ~30-second token, handed over as
   `?ticket=…`. Query strings land in access logs, which is exactly why the ticket is short-lived
   and single-use — a leaked one is worthless by the time anyone reads the log.

Authorization runs **before** `websocket.accept()`. Rejecting after accept means the client sees a
successful connection and then a close, which reconnect logic will happily retry forever. Rejected
handshakes get HTTP 401/403; a connection that fails authorization after accept for any other reason
closes `1008` (policy violation), which the client must treat as terminal, not retryable.

**`Origin` is checked explicitly.** WebSockets are not covered by the same-origin policy and CORS
does not apply to them: any page on the internet can open a socket to this endpoint and the browser
will attach cookies. That is cross-site WebSocket hijacking, and an allowlist check on `Origin` is
the defense. This is easy to forget precisely because the HTTP endpoints next door are protected by
machinery that does not extend here.

## The frame protocol

JSON text frames. Every frame carries `v` (protocol version) and `type` (discriminator). Pydantic
models in `core/frames.py` own both directions, so the wire format has one definition and FastAPI's
validation applies to inbound frames the same way it does to request bodies.

Binary frames are unused today. If artifact payloads ever want them, they get a separate opcode
path — never a magic string inside a text frame.

A frame type is **defined here only when the phase that implements it lands**. `core/frames.py` carries
exactly the types the server actually honors — nothing is stubbed ahead of time, because a `cancel`
that silently does nothing is indistinguishable, from the client's side, from one that worked.

### Client → server

| type | payload | phase | notes |
| --- | --- | --- | --- |
| `user_message` | `text`, `client_msg_id` | 1a | `client_msg_id` is the client's idempotency key |
| `ping` | — | 1a | application-level, see [Heartbeats](#heartbeats-and-idle-timeouts) |
| `resume` | `last_seq` | 1b | sent immediately after reconnect, before anything else |
| `cancel` | `job_id` | 2 | best-effort; the worker may already be done |

### Server → client

| type | payload | phase | notes |
| --- | --- | --- | --- |
| `ack` | `client_msg_id`, `seq`, `message_id`, `reply_message_id` | 1b | the message is a durable row at `seq`; `reply_message_id` names the stream |
| `delta` | `message_id`, `chunk_index`, `text` | 1a | one chunk of an assistant turn |
| `done` | `message_id`, `seq` | 1b | the turn is complete and its text is final |
| `message` | `seq`, `message_id`, `role`, `status`, `text` | 1b | one stored message, replayed whole; only `resume` sends it |
| `error` | `code`, `message`, `retryable` | 1a | application error; the socket stays open |
| `pong` | — | 1a | |
| `job_status` | `seq`, `job_id`, `state`, `progress` | 2 | `queued`/`running`/`done`/`failed` |

`delta` carries `(message_id, chunk_index)` rather than a session `seq`, which is what lets several
turns stream concurrently on one socket without ambiguity. That question — whether deltas need their
own sequence space — was the one 1a deferred, and `persistence.md` settles it: a delta is not a fact,
it is a rendering detail of a message still being produced, so only rows get a `seq`. Resume replays
messages whole rather than re-streaming tokens, which is why a 500-token reply stays two rows and two
`seq` allocations instead of five hundred of each.

A turn is two rows, so `ack` carries two identities. `message_id` is the user's message, the thing
being acknowledged; `reply_message_id` is the assistant row `delta` and `done` will name, which is
what binds the stream to the message that caused it. `reply_message_id` is **absent** when the
`client_msg_id` was already recorded and no turn for it is running *on that connection* — which is
three situations, not one: the turn already finished there, it died with the socket that started it,
or it is running on another of this session's sockets. The frame does not distinguish them, because
the client's next move is the same in all three: stop expecting deltas here, and let `resume` say
what the row holds.

`error` is a frame, not a close. Closing the socket on a bad user message conflates "this request
was wrong" with "this connection is unusable", and clients reconnect on close — so a malformed
message would become a reconnect storm. Close codes are reserved for transport-level facts.

### Versioning

`v` is on every frame from the first commit, because adding it later requires a flag day. Servers
accept `v` at or below their own and respond at the client's version where the difference is
additive; a client asking for a version the server does not know closes `1008` at handshake, before
any state exists to reconcile.

## Sequence numbers and resume

Every server→client frame carries `seq`: a gap-free counter, monotonic **per session**, not per
connection. It is allocated when the frame's underlying fact is written to Postgres, not when the
frame is put on the wire — otherwise two replicas serving the same session would mint colliding
numbers.

Reconnect is then mechanical:

```
client                                   server
  │  GET /ws/chat/{id}                     │
  │─────────────────────────────────────▶  │  authorize, accept
  │  {"type":"resume","last_seq":41}       │
  │─────────────────────────────────────▶  │  read the session log from 42
  │  ◀───── frames 42..57 ───────────────  │  replay
  │  ◀───── live frames 58…  ────────────  │
```

This is at-least-once, deliberately. A frame can be delivered twice — the server crashed between
sending and recording the send — so **clients deduplicate on `seq`**, and every frame is idempotent
when applied to client state. Exactly-once would require an ack protocol on every frame; the
duplicate-tolerant client is a great deal cheaper and strictly more robust.

In the other direction, `client_msg_id` does the same job for user messages: a client that
reconnects unsure whether its message landed simply sends it again, and the server, finding the key
already recorded for that session, re-`ack`s the existing message rather than starting a second
turn. Without it, a reconnect during a submit silently doubles the user's question — and each
duplicate costs a five-minute solve.

Resume depth is bounded by `ws_resume_max_messages` (200). A client further behind than that gets
`{"type":"error","code":"resume_too_old"}` and reloads the session over plain HTTP. Unbounded replay
turns a week-old tab into a full history dump on a single reconnect, down a socket whose send queue
is 64 frames deep.

It refuses rather than truncates, and that is the whole point of the error existing. A truncated
replay hands the client a gap it cannot see: it would advance its cursor past frames it never
received and never ask for them again. Saying "your cursor is unusable" is the only answer that
sends it to a full reload instead. The server therefore asks for one row more than the bound, so
"further behind than we will replay" is answerable from the same query.

## Fan-out across replicas

Under more than one API replica — which is the deployed configuration from phase 6 — the pod holding
a session's socket is almost never the pod that learns the job finished. The worker publishes
completion to RabbitMQ; some arbitrary replica consumes it.

**Every API replica binds an exclusive, auto-delete queue to a fanout exchange** (`chat.events`) at
startup, and forwards each event to whichever connections for that session it happens to hold
locally. Replicas holding none discard it. RabbitMQ is already a dependency and already models
this; the alternative, Postgres `LISTEN`/`NOTIFY`, is one fewer moving part but caps payloads at
8000 bytes and delivers nothing to a listener that was disconnected at the moment of the `NOTIFY`.

Neither mechanism is durable, and neither needs to be. Durability lives in Postgres, and resume
covers every gap the bus leaves — which is exactly why the fan-out layer is allowed to be lossy and
cheap. Notably this also means **no sticky sessions are required at the load balancer**: any replica
can serve any reconnect, because state is in the database, not in the pod.

## Heartbeats and idle timeouts

Intermediaries kill quiet connections. nginx's `proxy_read_timeout` defaults to 60 seconds; AWS
ALB's idle timeout defaults to 60 seconds. A user who asks one question and waits four minutes for a
solve is, from the proxy's point of view, an idle connection to reap.

So the server sends a WebSocket ping every 20–30 seconds regardless of traffic, and treats two
missed pongs as a dead peer. The interval must stay comfortably under the tightest timeout on the
path; the ingress' `proxy_read_timeout` is raised in the same change that sets this, and the two
numbers are documented together in the manifest, because tuning one without the other is how this
breaks in staging only.

Application-level `ping`/`pong` frames exist alongside the protocol-level ones because some
intermediaries pass control frames through without resetting their own idle counters, and because
browser JavaScript cannot see protocol pings — a client that wants to measure liveness itself has no
other option.

The server-side reason for dead-peer detection is narrower but matters: a half-open connection whose
peer vanished without a FIN holds a task, a queue, and a session registry entry indefinitely.
Multiply by a deploy cycle and a pod leaks itself out of memory.

## Backpressure

A connection's outbound queue is **bounded**. A client on hotel wifi receiving token deltas from a
fast model will not drain as fast as the server produces, and an unbounded queue converts that into
a pod OOM — one slow reader taking down every session on that replica.

On overflow the server closes the connection with `1013` (try again later) rather than blocking the
producer or growing the buffer. This is safe precisely because of the durable log: the client
reconnects, sends `resume`, and gets the frames it missed. Backpressure handling is a one-liner only
because the hard problem was solved upstream.

Inbound gets the mirror treatment: a maximum frame size, a maximum message rate per connection, and
a cap on concurrent connections per session and per user. Without them, one client can pin an event
loop that is serving hundreds of others.

## Shutdown and deploys

Kubernetes does not drain WebSockets. On SIGTERM the pod has `terminationGracePeriodSeconds` and
then dies; long-lived connections that were doing nothing at that moment are cut mid-frame.

The drain sequence we want:

1. Fail `/readyz` immediately, so the load balancer stops sending new handshakes here. Open sockets
   keep working — readiness governs new traffic, not existing connections.
2. Send `{"type":"error","code":"going_away","retryable":true}`, then close `1012`
   (service restart) on every held connection.
3. Wait briefly for closes to flush.

Clients treat `1012` as "reconnect immediately, with jitter" — a deploy that rolls twenty pods must
not produce twenty synchronized reconnect waves. All of it has to fit inside the grace period, which
is why the exec-form `ENTRYPOINT` decision from phase 0 matters here: if uvicorn never receives
SIGTERM, none of this runs and every connection is severed by SIGKILL instead.

### Lifespan shutdown is the wrong hook, and this was measured

The obvious home for that sequence is the lifespan shutdown phase 0 already established. It does not
work there, and the reason is worth writing down because the code reads as though it should.

**Uvicorn closes every open WebSocket before lifespan shutdown runs**, and waits for those
connections to finish first. `websockets_impl.WebSocketProtocol.shutdown()` calls
`fail_connection(1012)` and then `transport.close()`. The observed ordering on SIGTERM:

```
INFO:     Shutting down
INFO:     connection closed                    ← socket already gone
INFO:     Waiting for application shutdown.    ← lifespan shutdown starts here
```

By the time a lifespan hook runs, the registry is empty and there is no socket left to write a frame
to. So step 1 is too late to matter, and step 2's `going_away` frame is undeliverable — the client
gets a bare `1012` close and nothing else.

The good news is that uvicorn's default is *almost* the documented behavior: `1012` is the correct
code, and a client that reconnects with jitter on `1012` recovers exactly as intended. What is lost
is the explanatory frame and the chance to flush in-flight deltas.

Closing that gap needs a hook that runs *before* uvicorn begins its own shutdown, which ASGI lifespan
does not provide. **Phase 6 resolves it with a Kubernetes `preStop` hook** — the standard shape:
preStop calls a drain endpoint, which fails readiness and runs steps 2–3, and only then does the
kubelet send SIGTERM. `ConnectionRegistry.drain()` is written and tested against that call; the
lifespan call site is a backstop that is a no-op under uvicorn and correct under a server that orders
these the other way.

### Close codes

| code | meaning | client behavior |
| --- | --- | --- |
| `1000` | normal | do not reconnect |
| `1008` | policy violation — auth, bad version | terminal; surface to the user |
| `1011` | server error | reconnect with backoff |
| `1012` | service restart (deploy) | reconnect immediately, jittered |
| `1013` | try again later (backpressure, overload) | reconnect with backoff |

Backoff is exponential with full jitter, capped around 30 seconds. A server having a bad minute must
not be handed a thundering herd by its own clients.

## Testing

The handshake, the frame protocol, and the close-code contract are testable without containers,
which keeps CI as it is today — no services block in the workflow:

- `TestClient.websocket_connect` for handshake, authorization rejection, and frame round-trips.
- The responder sits behind a Protocol, so a `StubResponder` emitting deterministic deltas covers
  streaming without an LLM, and phase 4 swaps the implementation rather than the tests.
- Sequence and resume logic is tested against a fake repository injected through
  `dependency_overrides`, the same seam `tests/test_health.py` uses for `Database`.

Fan-out across replicas is the one thing a fake cannot honestly cover; it gets an integration test
against a real broker, run outside the unit suite.

Two traps, both hit while writing 1a:

- **`TestClient.websocket_connect` has no receive timeout.** A test that asserts "the server sends an
  error and closes" will *hang forever* rather than fail if the limit it guards stops being enforced —
  a healthy connection simply has nothing to say. Send a `ping` first so there is always a frame to
  read, or assert against the component directly. The same applies to bare `await` on a writer task:
  use `asyncio.wait_for`, so a regression fails in two seconds instead of wedging CI.
- **Passing tests are not evidence the test works.** Every limit here was checked by breaking the
  code and confirming the suite went red. Three of them did not, at first — they hung.

## Phasing

| phase | what lands |
| --- | --- |
| **1a** | Endpoint, envelope types, connection lifecycle, origin check, backpressure, per-session and per-turn caps, in-memory registry, stub streaming responder. Messages do not survive the connection. |
| **1b** | Postgres persistence: `chat_sessions` / `chat_messages`, alembic bootstrap, `seq` allocation, `resume`, `client_msg_id` idempotency. |
| **2** | `job_status` frames sourced from the broker; `cancel`. |
| **3** | Worker progress events; the RabbitMQ fanout exchange and per-replica queues. |
| **4** | The stub responder is replaced by the LLM gateway. Frame shapes do not change — that is the point of designing them against a stub. |
| **6** | Ingress timeouts aligned with the heartbeat interval; the `preStop` drain hook, since lifespan shutdown runs too late to be one. |

## Open questions

- **Ticket versus cookie** is unresolved and depends on where the phase-5 UI is served from. Ticket
  is the safer default for a separately-hosted frontend.
- **Multi-tab semantics.** Two sockets on one session do *not* both receive every frame: a turn
  writes to the connection that started it, and `ConnectionRegistry.for_session` has no caller
  outside tests until the phase-3 fan-out lands. A second tab sees another tab's turn only by
  reconnecting or resuming. Whether it should be able to submit while a turn is in flight is a
  product question, not a transport one.
