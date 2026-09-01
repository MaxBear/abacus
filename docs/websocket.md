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

Server-sent events plus `POST /messages` is the credible alternative, and for most of what a chat
app does it is the better one: it is ordinary HTTP, every proxy on the path already understands it,
and it reconnects itself. **For a chat that only streams tokens, SSE is the right default.** It is
what the Anthropic and OpenAI streaming APIs use, and what their own web clients use. Picking
WebSocket for a chat app is a departure from the consensus shape and owes an argument.

The argument is not the usual one. Two reasons commonly given do not survive contact:

- **"Cancellation needs a client→server channel."** It does not. The client already holds the
  `job_id` — it arrived in `job_status` — so `POST /jobs/{id}/cancel` is exactly as cheap as the
  `POST /messages` that SSE needs anyway.
- **"It is the shape we will want later."** A bet on future requirements, dressed as a reason.

What survives is that **this stream is session-scoped and outlives any single request.** The vendor
APIs stream on a response body: it opens with the POST, closes with the turn, and carries only what
that turn produces. Here a second producer — the worker — emits into the session when no request is
in flight at all:

- `queued → running` fires when a worker reserves the job, which can be long after the POST
  returned, while the user sits idle.
- `failed → running` fires when a backoff elapses (`jobs.md:113`), potentially minutes later.
- `retire_lapsed` fires from a *different* process' maintenance loop, on behalf of a worker that
  died holding the lease.
- A `cancel` issued in one tab has to reach the others.

None of those have a request to ride back on, so the response-body shape cannot carry them. The
alternative that could is a second, long-lived, session-scoped SSE stream running alongside the
POST — at which point the operational simplicity SSE was chosen for is largely spent, and the two
event sources sit on two channels with two orderings for the client to reconcile.

**And `Last-Event-ID` buys less here than it appears to.** Worth being precise, because it is the
strongest thing SSE has and it is routinely overcredited: the vendor APIs do not implement it — a
dropped Messages API stream is simply lost, and the client retries the whole request. Where it *is*
implemented, what it saves is the reconnect loop and the cursor frame
(`{"type":"resume","last_seq":41}`), not the replay. Replay here is the merged scan over
`chat_messages` and `job_events` that `persistence.md` specifies, and that scan is identical work
whichever transport asks for it. `resume_too_old` has no vocabulary in `Last-Event-ID` either; it
would become a status code on reconnect — workable, but a thing to design rather than a thing
inherited.

So, plainly: **the socket does not earn itself in phase 1, and is not meant to.** A phase-1-only
abacus should have been SSE. It earns itself at phase 3, when `job_status` begins arriving from a
process the client never spoke to — and retrofitting the transport at that point, against a
persisted sequence space and a live client, is worse than paying for it at the start.

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
| `cancel` | `job_id` | 3 | best-effort; the worker may already be done. Authorized against the session — see [Cancellation](#cancellation) |

### Server → client

| type | payload | phase | notes |
| --- | --- | --- | --- |
| `ack` | `client_msg_id`, `seq`, `message_id`, `reply_message_id` | 1b | the message is a durable row at `seq`; `reply_message_id` names the stream |
| `delta` | `message_id`, `chunk_index`, `text` | 1a | one chunk of an assistant turn |
| `done` | `message_id`, `seq` | 1b | the turn is complete and its text is final |
| `message` | `seq`, `message_id`, `role`, `status`, `text` | 1b | one stored message, replayed whole; only `resume` sends it |
| `error` | `code`, `message`, `retryable` | 1a | application error; the socket stays open |
| `pong` | — | 1a | |
| `job_status` | `seq`, `job_id`, `state` | 3 | a transition: `queued`/`running`/`done`/`failed`/`dead`/`cancelled` |
| `job_progress` | `job_id`, `progress` | 5 | unnumbered and lossy; never replayed |

`job_status` and `job_progress` were one frame in this table until phase 3, carrying `progress` as a
fourth field alongside `seq`. They are two because **a percentage is not a durable fact** — it is in
no row, it cannot be reconstructed after a reconnect, and numbering it would mean either an insert
per tick to mint a number obsolete on arrival, or a hole in a sequence space whose gap-freeness
`persistence.md` treats as an invariant. `worker.md` argues it at length. The split is the same one
`delta` already makes below, applied to a second producer: the durable thing gets a number, the
rendering detail does not.

**`job_progress` is specified here and built in phase 5**, which is the split earning its keep
rather than a schedule slipping. Everything needed to carry a percentage is machinery phase 3 would
otherwise have to invent for a number nothing can yet render: the child's contract is exactly one
message (`worker/child.py:11`), `SolveProcess` reads the pipe once, and `Solver` is
`payload → Solved` with nowhere to report from. Worse, the only solver that exists is `synthetic`,
whose progress is exact *because its duration was an input* — designing the protocol against it
would prove nothing, which is the trap `worker.md` names about cooperative mocks. Phase 5 is the
first thing with a bar to fill and phase 4 the first with real work to count. Deferring is safe
precisely because the frames are separate: adding one later is additive under `v1` and touches
nothing, where a fourth field on `job_status` could not have been added without a version bump.

Note what a client can do without it. `job_status(running)` carries a `seq` and arrives at a known
moment, so elapsed time is computable client-side with no server support at all — which is most of
what a progress indicator is for, and honest in a way a synthesised percentage is not.

A transition earns its `seq` the ordinary way — it is a row, in `job_events`, allocated from the
session's counter at insert, and replayed by the same cursor that replays messages. It is *not* the
`seq` of the message that caused the job: one message spawns many transitions, and a shared number
can neither be ordered against the messages between them nor survive a client whose cursor has
already passed it. `persistence.md` carries the table and the merged resume scan.

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

Note which half of that is load-bearing. A transition is already a committed row by the time
anything is published — `job_events`, written in the same transaction as the state change — so the
bus is delivering news the database has already recorded. Losing a message costs latency and
nothing else. Progress is the opposite and has no durable half at all, which is exactly why it is a
separate frame carrying no `seq`: there is nothing for a resume to fall back on, and nothing that
needs one.

**Every API replica binds an exclusive, auto-delete queue to a fanout exchange** (`chat.events`) at
startup, and forwards each event to whichever connections for that session it happens to hold
locally. Replicas holding none discard it. RabbitMQ is already a dependency and already models
this; the alternative, Postgres `LISTEN`/`NOTIFY`, is one fewer moving part but caps payloads at
8000 bytes and delivers nothing to a listener that was disconnected at the moment of the `NOTIFY`.

Neither mechanism is durable, and neither needs to be. Durability lives in Postgres, and resume
covers every gap the bus leaves — which is exactly why the fan-out layer is allowed to be lossy and
cheap. Notably this also means **no sticky sessions are required at the load balancer**: any replica
can serve any reconnect, because state is in the database, not in the pod.

### Who publishes

**`RabbitMQJobQueue` publishes**, from inside the operations it already wraps — `enqueue`, `cancel`,
`claim`, `ack`, `nack`, `discard` — plus from `_maintain` for `retire_lapsed`. It calls
`PostgresJobStore` for all of them, so it sees every transition there is, and it is the one class in
the system that legitimately holds both a store and an AMQP channel.

**It is not a method on the `JobQueue` Protocol.** Publishing is a side effect of the existing
methods, invisible at the seam; `core/jobs.py:180-313` gains nothing. The Protocol's own rule
forbids it — *"nothing here is transport, storage, or scheduling policy"* — and a `publish_event` on
an interface whose other implementation is a dict would be exactly that. `MemoryJobQueue` simply
does not publish, which costs nothing, because it is the same seam decision that keeps `job_events`
below the contract.

Two alternatives, rejected:

- **`PostgresJobStore` publishes.** It is where the row is written and would be the smaller change,
  but a Postgres adapter has no business holding an exchange, and `test_layering.py` exists to say
  so.
- **An outbox relay** polling `job_events` for unpublished rows. Genuinely the most robust — it
  cannot lose a message even if the broker is down at the moment of the transition — and it is
  machinery this does not need. A lost message costs latency and nothing else, because the row is
  already committed and resume replays it.

Note the interaction with cancellation: because the API also holds a `RabbitMQJobQueue`
(`consume=False`, see [Cancellation](#cancellation)), the API-side `queued` transition is published
by the same class as every worker-side one. There is no second answer to invent for who publishes
what, which is the argument that settled both.

### The event on the wire

```
{v, session_id, job_id, seq, state}
```

**The event is the `job_status` frame, plus routing, plus a version.** `seq`, `job_id` and `state`
are exactly the frame's fields; `session_id` is how a replica decides whether the event is any of
its business; `v` is the version. Nothing else.

**It is minimal on purpose.** The alternative is shipping the whole `Job`, and `jobs.payload` is
JSONB that phase 4 fills with an `AnalysisRequest` — which the API needs none of. This is a *fanout*
exchange, so every byte is delivered to every replica: payload size is multiplied by the replica
count, on a path where most copies are discarded on arrival. Anything the API turns out to want
later is in the `job_events` row, and that row is what resume reads anyway, so the bus is never the
only way to learn something.

**`session_id` is on the wire rather than looked up from `job_id`.** Delivery is then a dict hit —
`ConnectionRegistry.for_session` — with no database round trip. That matters because this path runs
for every event on every replica, so a lookup here would be O(replicas × events) of work whose
usual answer is "not mine, discard."

**Defined in `core/events.py`**, which both the worker and the API import, and which cannot import
`aio_pika` (`test_layering.py:28`). The AMQP encoding — exchange, routing, headers — lives in
`adapters/rabbitmq/events.py`, on the same split as every other seam here.

**Versioned from the first commit**, for the reason frames are, and harder. Worker and API roll
independently, so during any deploy both versions are on the bus simultaneously; skew is the normal
condition rather than an incident. And an unknown `v` is a *discard*, not an error — which is safe
for exactly the reason the fan-out is allowed to be lossy at all: the transition is a committed row,
so a dropped event costs a client some latency until its next resume, and nothing more. Forward
incompatibility degrades to slowness instead of to a broken session.

### The API's broker connection

From PR 2 the API holds **two** users of the broker, not one: the `chat.events` consumer above, and
the producer-only `RabbitMQJobQueue` that [Cancellation](#cancellation) puts in the lifespan. Every
answer below follows from that pair, and from rules this service already has.

**One connection, two channels.** `RabbitMQJobQueue.start` takes a `url` and opens its own
`connect_robust` (`adapters/rabbitmq/job_queue.py:153`), which is right for a worker and wrong for an
API that is already holding one. It grows a `connection` parameter, the lifespan builds a single
robust connection, and the queue and the consumer each take a channel on it. A second TCP connection
would buy only what a second channel already gives — isolation between a consumer-side channel error
and publishing — while adding a second heartbeat stream and, worse, a second thing that can fail
independently. Sharing collapses "can publish but receives nothing" and its mirror back into one
fact, which is what lets readiness be a single honest check rather than a partial one. The worker is
unaffected and keeps `start(url=...)`.

**A pod whose broker is down starts anyway, and reports not-ready.** This is the treatment Postgres
already gets (`api/main.py:20`): the engine is constructed eagerly so a malformed URL kills the
process, but no socket is opened, so a dependency outage produces a not-ready pod rather than a
crash loop. The broker connection follows the same rule — lifespan starts it, a failure to connect
does not abort startup, and `aio_pika`'s robust connection keeps retrying in the background.
Refusing to boot would convert a recoverable broker outage into a rolling restart of every replica
at the moment the system is least able to absorb one.

**`/readyz` inspects the connection this pod holds, not a fresh one.** `broker.ping`
(`adapters/rabbitmq/broker.py:6`) opens a connection and closes it, deliberately, on the reasoning
that readiness should measure whether a *new* consumer could connect right now. That was the right
question while the API held nothing. It is the wrong one the moment the pod holds its own: a fresh
connect can succeed while this replica's connection is dead, and readiness would report a pod that
cannot do its job. So the check becomes local state — connection open and not reconnecting, channel
open, consumer tag held — which is also a probe that costs no handshake, on an endpoint every
replica serves every few seconds. `broker.ping` has no other caller and goes with the assumption
behind it.

Note **which half of a broken connection actually hurts**, because it is not the obvious one. A dead
consumer leaves the pod deaf: it accepts sockets and delivers no `job_status` to them, which is
degraded but self-healing, since every transition is a committed row and the next `resume` collects
what was missed. A dead producer leaves it *mute*: `cancel` fails, and from phase 4 so does
`enqueue`, so a user's message cannot become work at all. Nothing repairs that after the fact. Mute
is the worse failure, and it is the stronger reason the check watches the shared connection rather
than the consumer alone.

**On reconnect the queue comes back under a different name, and the gap is lost.** The per-replica
queue is exclusive and auto-delete with a server-generated name; `aio_pika`'s robust connection
redeclares on reconnect, so what returns is a *new* queue. Anything published while the connection
was down reached an exchange with no binding for this replica and is gone. That is harmless for the
reason everything in this section is harmless — the transition is a committed row and resume repairs
it — but it is written here because a queue name silently changing in the RabbitMQ console looks
exactly like a bug the first time someone sees it.

Note how the liveness/readiness split behaves during that gap. `/readyz` fails, so Kubernetes takes
the pod out of the Service and new connections go elsewhere; `/livez` checks nothing, so the pod is
not restarted; and the sockets already open stay open, missing events until their next resume. A
broker blip drains new traffic without killing live sessions, which is the whole point of the two
probes answering different questions.

Two consequences for existing code, so they are not discovered during the PR.
`scripts/verify-phase0.sh:15` asserts `/readyz` 200 with dependencies up, which now requires the
consumer to be bound by the time the script probes rather than merely the broker to be reachable —
startup ordering, or a retry in the script. And `tests/test_health.py` monkeypatches
`adapters.rabbitmq.broker.ping` by string in four places; with the check moving onto an object in
`app.state`, those become an injected fake, which is what `api/deps.py` describes that seam as being
for.

## Cancellation

`cancel` is the one client→server frame that is not about the chat: it names a `job_id` and asks the
queue to stop work a worker is doing. It lands in phase 3 rather than phase 2 for a reason that is
not scheduling — **a client learns a `job_id` only from `job_status`**, so a `cancel` frame shipped
any earlier would have had nothing to name.

**The API holds a producer-only queue.** `api/main.py`'s lifespan gains a
`RabbitMQJobQueue.start(..., consume=False)`: it declares topology and can `enqueue` / `cancel` /
`get`, but does not consume and does not run `_maintain`. The handler depends on the `JobQueue`
Protocol exactly as it already depends on `ChatRepository` and `Responder`, so `MemoryJobQueue` is
the test double and no new fake is written. Two consequences, stated rather than left to be
discovered:

- **The API does not run the orphan sweep.** `_maintain` republishes jobs whose row committed but
  whose publish did not — findable because `published_at` is null — and a `consume=False` process
  does not run it. That is fine, since maintenance is not owned by whoever produced the job and any
  worker repairs any job; but it means an API-side `enqueue` whose publish fails waits for a
  worker's sweep rather than repairing itself.
- **Neither `cancel` nor `get` touches the broker.** Both delegate straight to the store
  (`adapters/rabbitmq/job_queue.py:239` and `:236`); a cancelled job's message stays on the queue
  and is discarded when someone tries to claim it, because the claim is conditional and the row is
  the authority. So the AMQP half of this dependency sits idle until phase 4's `enqueue`.

**The handler authorizes; the queue cannot.** `JobQueue.cancel` takes a bare `job_id` and knows
nothing about sessions — correctly, it is the queue seam — and `jobs.session_id` is nullable. A
`cancel` naming another session's job, or a job belonging to no session, would otherwise simply
succeed. So the handler reads before it writes:

```python
job = await queue.get(job_id)
if job is None or job.session_id != connection.session_id:
    # refuse; the frame names something that is not this session's to stop
await queue.cancel(job_id)
```

Ownership is of the **session**, not of the connection: any of a session's sockets may cancel any of
its jobs. That is the same per-session treatment `job_status` gets, and the opposite of `delta`.

This is the only place an untrusted identifier enters either job path, and the asymmetry is worth
naming. On the fan-out side the `session_id` is read back off a `job_events` row that an
authenticated handler wrote at enqueue — it has never left the server's storage. On this side the
`job_id` arrives in a frame. **The whole argument rests on `jobs.session_id` being set from the
connection at enqueue and never from the request payload.** Phase 4 owns `enqueue` and must keep
that property: if a client ever supplies its own `session_id`, fan-out silently becomes
client-controlled routing and nothing here fails loudly.

**The result comes back the ordinary way.** `cancel` gets no bespoke reply. The transition writes a
`job_events` row and publishes to `chat.events`, and every connection the session holds — on this
replica or any other — receives `job_status(cancelled)` through the fan-out above. A client that
cancels twice across a reconnect gets the same answer both times, because `cancel` is a transition
and an already-terminal job is returned unchanged.

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
| **2** | ~~`job_status` frames sourced from the broker; `cancel`.~~ Neither landed. Phase 2 built the queue beneath them and stopped there, on the grounds `jobs.md` records: both frames need a worker to be about, and there was none until phase 3. Moved down a row rather than quietly dropped. |
| **3** | `job_events` and the merged resume scan; `job_status` on the wire; the `chat.events` fanout exchange and per-replica queues. `cancel` rides along, since a frame that stops a solve is only honest once something can be stopped. |
| **4** | The stub responder is replaced by the LLM gateway. Frame shapes do not change — that is the point of designing them against a stub. |
| **5** | `job_progress`, and the worker-to-child channel that carries it. Here rather than in phase 3 because the analyses that can count their own work are phase 4's, and the bar that renders the count is this phase's. |
| **6** | Ingress timeouts aligned with the heartbeat interval; the `preStop` drain hook, since lifespan shutdown runs too late to be one. |

## Open questions

- **Ticket versus cookie** is unresolved and depends on where the phase-5 UI is served from. Ticket
  is the safer default for a separately-hosted frontend.
- **Multi-tab semantics.** Two sockets on one session do *not* both receive every frame: a turn
  writes to the connection that started it, and a second tab sees another tab's turn only by
  reconnecting or resuming. Whether it should be able to submit while a turn is in flight is a
  product question, not a transport one.

  Phase 3 makes this **inconsistent rather than merely incomplete**, and deliberately so. The
  fan-out's first caller of `ConnectionRegistry.for_session` delivers `job_status` to *every*
  connection the replica holds for a session, because the worker publishing it has no connection to
  prefer — while `delta` and `done` still go only to the socket that started the turn. So after phase 3 one class of frame is per-session and another is
  per-connection. That is the honest consequence of who produces each, not a decision about tabs;
  making them agree means answering the product question above, which phase 5 is the first thing
  entitled to answer.
