# Roadmap

The phase plan. This is the document `scripts/verify-*.sh` means when it cites "the build plan's
Verification section" — before this file existed, that citation pointed at nothing in the repo.

Each phase gets three things, because a phase without all three is a wish rather than a plan:

- **What lands** — the artifact, concretely enough to tell when it is finished.
- **Acceptance** — the check that proves it. Ideally executable, and phase 0's is.
- **The decision it forces** — the thing this project exists to work through. If a phase forces no
  decision, it is not a phase; it is a chore that belongs inside one.

**Confidence varies by phase, and is marked.** Phases 0 and 1 are grounded in what is already in the
repo. Phases 2–6 are reconstructed from the one-line roadmap in `README.md` plus scattered code
comments; where intent could not be recovered from the repo it is called out as an open question
rather than invented. See [Open questions](#open-questions) — two of them affect numbering.

| phase | what | state |
| --- | --- | --- |
| 0 | Skeleton: FastAPI + Postgres + RabbitMQ, health split, prod-shaped image | **done** |
| 1 | Chat over WebSocket | **done** |
| 2 | The job broker on RabbitMQ | **done** |
| 3 | Containerized workers | in progress |
| 4 | The analytics engine, and the LLM gateway | planned, numbering unresolved |
| 5 | Chat UI | planned |
| 6 | Cloud | planned |

---

## Phase 0 — skeleton

**Done**, at `51133f6`. Grounded: everything below is in the repo.

**What landed.** FastAPI under compose with Postgres and RabbitMQ, `/livez` and `/readyz` split, a
multi-stage Dockerfile that runs non-root with an exec-form `ENTRYPOINT`, dependencies locked with
`uv.lock` rather than merely pinned, and dependencies wired explicitly through `app.state` and
FastAPI's `Depends` instead of module globals.

**Acceptance.** `make verify` → `scripts/verify-phase0.sh`, seven checks: `/livez` 200; `/readyz` 200
with dependencies up; `/readyz` 503 **while `/livez` stays 200** with Postgres stopped; image user is
`app`; `ENTRYPOINT` is exec-form; no secret-looking assignment in `docker history`; container stops
in under 10 seconds against a 30-second grace period.

**The decision it forced.** That operational correctness is designed, not discovered. Each of the
three decisions in the README is a specific production failure declined in advance — liveness that
checks a database converts a dependency blip into a fleet-wide crash loop; shell-form `ENTRYPOINT`
makes `/bin/sh` PID 1 and swallows SIGTERM, so Kubernetes SIGKILLs the container and drops in-flight
requests. `tests/test_health.py` is the regression guard for the first.

## Phase 1 — chat over WebSocket

**Done**, functionally, at `34654ca`. 1a and 1b are merged, the postgres-marked suite passes against
a real database — including `test_a_socket_killed_mid_turn_loses_nothing`, the acceptance test — and
the browser pass over `dev/chat.html` on 2026-08-20 confirmed the happy path, full replay, and the
caught-up resume that correctly returns nothing. What that pass found was a defect in the harness
rather than the server; see `dev/chat.html`'s note on `client_msg_id`.

**Transport design is settled** in [`websocket.md`](websocket.md), written ahead of the
code and holding through phase 6; this section is the summary, that document is authoritative.

Split in two, because persistence is a different concern from transport and phase 0's habit is one
concern per PR:

**1a — transport.** `/ws/chat/{session_id}`, versioned JSON frame envelope in `core/frames.py`,
connection lifecycle with heartbeat, an in-memory session registry, and a streaming stub responder
behind a Protocol. Messages do not survive the connection.

**1b — persistence.** Alembic bootstrap, `chat_sessions` / `chat_messages`, `seq` allocation,
`resume` on reconnect, `client_msg_id` idempotency. This is what `adapters/postgres/db.py:37` is waiting for:
*"Nothing calls this until phase 1 adds real queries."*

**Acceptance.** Handshake, authorization rejection, frame round-trips, and the close-code contract
under `TestClient.websocket_connect`, with the stub responder and a fake repository injected through
`dependency_overrides` — no containers, so CI stays service-free as it is today. Then the one that
matters: **kill the socket mid-turn, reconnect, and lose nothing.**

**The decision it forces.** That the socket is a view onto durable state and never owns anything. A
solve runs 30 seconds to five minutes; if a dropped connection can lose one, the product is broken in
a way no reconnect logic repairs. Deciding this at 1a is what lets backpressure simply close the
connection and lets the phase-3 fan-out bus be lossy — both are cheap only because durability sits
underneath them.

## Phase 2 — the job broker on RabbitMQ

Done. **The contract and the lease semantics are settled** in [`jobs.md`](jobs.md), written ahead of
the implementation; this section is the summary, that document is authoritative.

The centerpiece, and the reason the project describes itself as a cloud-to-scheduler bridge. It is
also the first of this repo's two learning goals: how RabbitMQ and distributed messaging behave in
the context of a job scheduler. (The second is AI integration, which is phase 4.)

**What landed.** A `JobQueue` Protocol in `core/`, the `jobs` table as the system of record behind
it, and a RabbitMQ implementation on quorum queues — leases as row timestamps, fencing on a lease
token, retry through a per-message-TTL delay queue and a dead-letter exchange, and the two periodic
repairs the broker-plus-table split makes necessary. `MemoryJobQueue` in `tests/` is the reference
semantics and the second implementation.

**Acceptance, met.** One suite, run twice — `tests/test_job_queue.py` is parametrized over `memory`
and `rabbitmq`, 35 contract tests apiece, and that both pass *unmodified* is the proof the Protocol
is a real seam. What a fake cannot falsify — competing consumers under a genuine race, redelivery
after a consumer is cut off mid-job, the dual write's repair — is asserted separately in
`tests/test_rabbitmq_job_queue.py`.

**Not built: a Postgres `JobQueue`, and the benchmark comparing the two.** Deliberately, and not as
a cut for time. This repo exists to learn RabbitMQ and distributed messaging, and a
`SELECT … FOR UPDATE SKIP LOCKED` implementation would have taught neither — it is the *absence* of
a broker, which is the thing being studied. A backend bake-off is a different project.

**The decision it forced,** which survives without the measurement: when a database is a sufficient
queue and when it is not. The received answer is "never use your database as a queue," repeated far
more often than it is measured; `SKIP LOCKED` is genuinely good, and buys transactional enqueue with
the job's own state — no dual write, no outbox. Against that, RabbitMQ brings real delivery
semantics and does not consume connection-pool slots. Jobs here are 30 seconds to five minutes and
low-rate, exactly the regime where the folklore is least likely to apply.

That argument is not hypothetical here, because building the RabbitMQ side made its costs
executable rather than rhetorical. `_maintain`'s two periodic Postgres queries exist *only* because
the broker cannot answer what a row can: it has no per-message deadline, so a lapsed lease has to be
polled for, and it cannot enlist in a transaction, so a dual write has to be swept for. The
comparison's conclusion is in the code, in a form a benchmark report could not have delivered — and
[`jobs.md`](jobs.md#the-benchmark) keeps the measurement's design, unrun, including the thresholds
that were written down in advance.

## Phase 3 — containerized workers

In progress. **Designed ahead of the code** in [`worker.md`](worker.md); this section is the
summary, that document is authoritative.

**What lands.** `worker/` — the job consumer and solver runner (`README.md:59`), as a stateless
container that pulls work, executes a CPU-bound solve, writes artifacts to object storage, and
reports terminal state. Object storage becomes a real adapter here. Worker progress events reach
connected clients through the RabbitMQ fanout described in `websocket.md`, which is also where
per-replica event queues arrive.

**The shape it takes.** Two processes per container: an `asyncio` supervisor holding the AMQP
connection and the lease, and a `spawn`ed child doing the solve. A CPU-bound computation cannot
share a thread with an AMQP heartbeat — RabbitMQ closes a connection after two missed 60-second
intervals, so a five-minute solve on the event loop is torn down by the broker on the grounds that
the worker is dead. The same process boundary is what makes cancellation possible at all, since a
Python thread cannot be interrupted from outside and a process can be signalled.

**Acceptance.** Kill a worker mid-solve; the job is redelivered and completes elsewhere. No partial
artifact is ever visible as a finished one.

**The decision it forces.** What "stateless" costs in practice — where a five-minute CPU-bound solve
keeps its intermediate state, how cancellation reaches a process that is busy inside numpy and not
checking a queue, and how at-least-once delivery is squared with solves that must not silently run
twice.

## Phase 4 — the analytics engine, and the LLM gateway

> **Open — numbering conflict.** Two places in the repo assign phase 4 to different work.
> `README.md:26` says phase 4 is *the analytics engine*; `README.md:63` and `.env.example:9` both say
> `gateway/` — the LLM gateway, sole holder of `ANTHROPIC_API_KEY` — is phase 4. These are separate
> pieces of work sharing a number. Both are prerequisites for the phase-5 UI, so the ordering is
> unaffected; only the label is. Written below as 4a/4b pending your call on whether to split the
> number or renumber 5 and 6.

**4a — the analytics engine.** The actual numerical content: backtests, Monte Carlo, correlation,
drawdown, over data from Stooq and FRED. This is the "compiled code computes" half of the thesis, and
the only phase where the answer's *correctness* — not the system's — is what is being tested.

**4b — the LLM gateway.** `gateway/`, the only holder of the Anthropic key, never present in a worker
container. Turns English into an `AnalysisRequest`, selects the analysis, and explains the result
afterward. It performs zero calculation, which is the constraint the whole architecture is arranged
to enforce. It replaces the phase-1 stub responder behind the Protocol that stub already implements —
**frame shapes do not change**, which is the entire reason phase 1 was designed against a stub.

**Acceptance.** For 4a, known-answer tests: analytics whose results can be checked against an
independent computation, not against a previous run of themselves. For 4b, that the stub swap
requires no change to `api/chat.py` or to any frame type, and that the key is absent from every
worker image and manifest.

**The decision it forces.** Where the boundary between the model and the computation actually sits
under pressure — specifically what happens when the model requests an analysis that is well-formed
but meaningless for the supplied portfolio. Validation belongs somewhere, and the tempting answer
(let the model check) is the one that ends the separation this project is built to demonstrate.

> **Open — who calls `JobQueue.enqueue`.** Not settled anywhere in the repo, and it is the seam most
> likely to make 4b's "the stub swap changes no frame" promise false. A gateway that enqueues needs
> the session id, because jobs deduplicate on `(session_id, idempotency_key)` and `job_status` has to
> route back to a session — and `core/responder.py` deliberately withholds exactly that: *"A
> responder is handed text and yields text; it never sees a Connection, a session id, or a frame."*
>
> Three ways out, none free. The gateway holds its own `JobQueue`, which breaks that narrowness and
> means the gateway can no longer be tested without a queue. Or `Responder` stops being `text →
> text` and yields a small discriminated union — a text chunk, or an analysis to run — leaving
> `enqueue` to the handler. Or the handler inspects the gateway's output, which is the same thing
> typed differently.
>
> **Leaning to the second.** It keeps the gateway a pure function of text → intent, testable with no
> queue and no database, and it puts `enqueue` next to the code that already owns session state:
> `_handle_user_message` allocates the `seq`, writes the rows, and knows the session id. It is also
> the honest description of a gateway, which genuinely produces two kinds of output — prose for the
> human and a request for the machine — and calling both "text" is what forces the awkwardness.
>
> Nothing in phase 2 depends on this: the Protocol and the `jobs` table are identical either way, and
> only the holder of the reference changes. But phase 4's whole premise is that the swap is cheap,
> which is only true if this is decided before the gateway is written rather than during it.

## Phase 5 — chat UI

**What lands.** The frontend: paste a portfolio, ask in English, watch tokens and job progress
stream, see charts. First real client of the phase-1 frame protocol.

**Acceptance.** Refresh the page mid-solve and lose nothing — the client's `resume` path exercised
by an actual browser rather than a test harness.

**The decision it forces.** Whether the frame protocol designed in phase 1 survives contact with a
real client. Multi-tab semantics land here too (`websocket.md`, open questions), because they are a
product question that only becomes answerable once there is a product.

> **Open.** Where the UI is served from is undecided and has a dependency: it determines the
> cookie-versus-ticket choice for WebSocket authentication, which `websocket.md` leaves open. A
> separately-hosted frontend makes the ticket the safer default.

## Phase 6 — cloud

**What lands.** `infra/` — Terraform and k8s manifests (`README.md:60`). The phase where phase 0's
operational choices are finally exercised by the thing that motivated them: real probes, real
rolling deploys, real SIGTERM.

Two WebSocket-specific items come due, per `websocket.md`: ingress `proxy_read_timeout` raised in the
same change that sets the heartbeat interval — tuning one without the other breaks in staging only —
and drain-on-SIGTERM verified against the actual grace period, since Kubernetes does not drain
WebSockets on its own.

**Acceptance.** A rolling deploy under load with connected clients mid-solve, dropping nothing: no
severed connection outlives its reconnect, no job is lost, no client sees an error that isn't a
jittered `1012` reconnect.

**The decision it forces.** Whether the liveness/readiness split, the exec-form `ENTRYPOINT`, and the
lifespan drain were correct — all three were chosen in phase 0 against failure modes that could not
be observed until here. This phase is where phase 0 gets graded.

---

## Open questions

Collected from above. The first two affect numbering or sequencing and are worth settling soon; the
rest can wait for the phase that needs them.

1. **Phase 4's numbering** — analytics engine and LLM gateway currently share the number. Split into
   4a/4b, or renumber 5 and 6 upward?
2. ~~**Phase 2's benchmark methodology**~~ — settled in [`jobs.md`](jobs.md#the-benchmark), then
   not run: the Postgres implementation it would compare against is out of scope, so there is
   nothing to compare. The design and its decision thresholds are kept as a record of what would
   have been measured. The narrower phase-2 questions that replaced it are in that document's Open
   section; quorum versus classic and the lease's granularity are both settled there now.
3. **Where the phase-5 UI is served from** — decides cookie versus ticket for WebSocket auth, which
   `websocket.md` is holding open.
4. **Whether `verify-phase0.sh` generalizes** to `verify.sh <phase>` once phase 1 has acceptance
   checks worth scripting, or stays one script per phase.

## Conventions

Each phase is a branch and a PR, matching the phase-0 pattern (`phase-0/uv-packaging`,
`phase-0/explicit-wiring`) — one concern per PR, not one phase per PR. Acceptance checks are
executable where they can be; where they cannot, they are written down here so that "done" is not
decided retroactively.
