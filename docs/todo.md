# Step 4 of phase 3 — the open questions

> **Transient, and tracked only so it survives a `git pull` on another machine.** Every answer here
> lands in `worker.md`, `websocket.md`, or `persistence.md`, and this file shrinks as they do. When
> the last question is settled it is deleted in the same commit that settles it. `.gitignore:20`
> exists because `TODO-queue-fixes.md` was never pruned; this one has an end condition written down.

Six left. Each has: the question, why it matters, the options, my lean, and which PR it blocks.

Step 4 is now **two PRs**:
- **PR 1 — the durable half.** `job_events` table, the event row written inside each of
  `PostgresJobStore`'s six state transitions, the merged resume scan. No broker.
- **PR 2 — the bus.** `core/events.py`, `adapters/rabbitmq/events.py`, the per-replica queue bound
  at API startup, `job_status` on the wire.

---

## Already settled (context — no action)

| | decision |
|---|---|
| Progress vs state | Two frames, not two fields of one |
| Where `job_status`'s `seq` comes from | **Fork A** — a `job_events` table, one row per transition, drawing from `chat_sessions.next_seq` |
| Where the event row is written | Inside `PostgresJobStore`, in the same transaction as the state change |
| `job_progress` | Specified in `websocket.md`, **deferred to phase 5** |
| SSE | Out of consideration |

All four are already written into the uncommitted doc changes.

---

## Q1 — Does `cancel` land in step 4?

**Status:** I wrote "yes" into `websocket.md`'s phasing table on my own judgement. Needs your
sign-off, because it is real scope and I may have been too free with it.

**Why it matters.** `websocket.md` originally assigned `cancel` to phase 2; it never landed, because
both it and `job_status` need a worker to be about. `core/frames.py:11` deliberately has no `Cancel`
frame today, on the rule that "a `cancel` that parses and silently does nothing is
indistinguishable, from the client's side, from one that worked." Right now a user cannot cancel
anything except through `dev/enqueue.py --cancel`.

**The dependency I under-flagged.** The API holds **no `JobQueue` at all**. `api/main.py`'s lifespan
builds `db`, `chat_repository`, `chat_registry`, `responder` — that is the whole list.

**Options**

- **(a) Yes, and the API holds `RabbitMQJobQueue.start(..., consume=False)`.** Producer-only:
  declares topology, can publish / `cancel` / `get`, does not consume and does not run `_maintain`.
  The handler depends on the `JobQueue` Protocol like everything else, and `MemoryJobQueue` is
  already the test double so no new fake is written. Also pre-solves phase 4's `enqueue` seam.
- **(b) Yes, but the API holds a bare `PostgresJobStore` and calls `.cancel()` directly.** Smaller,
  but bypasses the Protocol, needs a new narrow `JobCanceller` Protocol for the handler to depend
  on, and leaves a second answer to invent when phase 4 needs `enqueue` (which is a dual write and
  genuinely does need the broker). **I proposed this earlier and have retracted it.**
- **(c) No — defer `cancel` to phase 5**, with the UI that would offer the button.

**My lean: (a).** The API is getting an AMQP connection anyway for the `chat.events` consumer, so
"keep the broker out of the API" was a constraint I invented and it does not hold.

**Worth stating in the docs either way:** the orphan sweep (`unpublished`) is producer-side repair,
and a `consume=False` API would not run `_maintain`. That is fine — any process running maintenance
repairs any job, and the workers do — but it should be written down rather than discovered.

**Blocks:** PR 2.

---

## Q2 — Where does `job_events_since` live?

**Why it matters.** Resume today is one call: `chat_handler.py:225` → `ChatRepository.messages_since`
→ one range scan. Fork A gives it a second source. Whatever shape this takes, `MockChatRepository`
has to follow, and that fake is what keeps the whole suite container-free.

**Options**

- **(a) Add `job_events_since` to the `ChatRepository` Protocol.** Handler calls both and merges.
  Simple, but it dilutes a name that was *just* deliberately narrowed — PR #14 was
  "core/repository.py is the chat repository, and now says so" — and it puts the merge in the
  handler, which then knows resume has two sources.
- **(b) A second Protocol** (`JobEventLog`, say). Honest separation, but the handler now holds two
  dependencies for one operation, and the suite grows a second fake.
- **(c) One method on `ChatRepository` that returns the merged log** — `log_since` — with the two
  queries and the merge inside `PostgresChatRepository`. The handler asks one question and gets the
  session log; it never learns there are two tables. Cost: the chat repository now reads
  `job_events`, crossing a domain line, though both already live in `adapters/postgres` and share
  `tables.py`.

**My lean: (c).** The merge belongs next to the two queries it merges, not in the handler, and
"the session log" is a real concept that both tables are part of. But (a) is defensible and cheaper.

**Blocks:** PR 1.

---

## Q3 — Does `MemoryJobQueue` grow job events?

**Why it matters.** `jobs.md`'s thesis is "one suite, run twice, and that both pass *unmodified* is
the proof the Protocol is a real seam" — 35 contract tests over `memory` and `rabbitmq`. If
`job_events` writing lives only in `PostgresJobStore`, the two implementations diverge.

**Options**

- **(a) No — `job_events` sits below the `JobQueue` contract.** It is a Postgres-side detail about
  the chat sequence space, and `MemoryJobQueue` has no `chat_sessions`, no `next_seq`, and no notion
  of a session log. The contract suite is untouched.
- **(b) Yes — add it to the contract.** Means inventing a per-session seq allocator inside a test
  double, which is a significant amount of fiction to maintain.

**My lean: (a)**, stated explicitly in the docs so it reads as a decision rather than an oversight.
Worth being deliberate: it is the first place the two implementations knowingly differ.

**Blocks:** PR 1.

---

## Q4 — Who publishes to `chat.events`?

**Why it matters.** I settled where the event **row** is written (`PostgresJobStore`, in-transaction).
I did not settle where the **message** is published, and it cannot be the same place — a Postgres
adapter has no business holding an AMQP exchange.

**Options**

- **(a) `RabbitMQJobQueue` publishes**, after each operation it already wraps, plus from `_maintain`
  for `retire_lapsed`. It calls the store for everything already, so it sees every transition.
- **(b) `PostgresJobStore` publishes.** Rejected — layering.
- **(c) An outbox relay** polling `job_events` for unpublished rows. Most robust, most machinery,
  and unnecessary given a lost message costs only latency.

**My lean: (a)** — and note it interacts with Q1. If the API holds a `consume=False`
`RabbitMQJobQueue`, then (a) covers the API-side `enqueue` path uniformly. If the API holds a bare
`PostgresJobStore` instead, the `queued` transition has no publisher and Q4 needs a second answer.
**Decide Q1 first.**

**Blocks:** PR 2.

---

## Q5 — The event's wire format

**Why it matters.** A new service-to-service contract between the worker and every API replica.
Nothing specifies it.

**Open sub-questions**

- **Where.** `core/events.py`, presumably — it cannot import `aio_pika` (`test_layering.py:28`).
- **Versioned?** Frames carry `v` from the first commit because retrofitting one needs a flag day.
  The same argument applies here, and arguably harder: worker and API roll independently, so a
  version skew between them is normal rather than exceptional.
- **What it carries.** The whole `Job`, or the minimum? `jobs.payload` is JSONB and can be large;
  the API needs none of it.

**My lean:** minimal and versioned — `{v, session_id, job_id, seq, state}`. Everything else the API
might want is in the row, and the row is what resume reads anyway.

**Blocks:** PR 2.

---

## Q6 — The API's broker connection lifecycle

**Why it matters.** Today `api/` touches RabbitMQ only through `broker.ping` (`api/health.py:34`),
which deliberately opens and closes rather than pooling — "readiness should measure whether a *new*
consumer could connect right now." A long-lived consumer changes that.

**Open sub-questions**

- **Does `/readyz` change?** Check that the consumer is bound and alive, or keep the fresh-connect
  probe?
- **Startup with the broker down.** Does the pod start and report not-ready — the treatment Postgres
  gets at `api/main.py:20`, chosen so a pod does not crash-loop — or refuse to boot?
- **Reconnect.** `aio_pika`'s robust connection redeclares on reconnect, so the exclusive queue comes
  back under a *new* server-generated name and anything published during the gap is gone. Harmless
  given the bus is lossy, but nothing says so yet.

**My lean:** start not-ready rather than crash-loop (consistency with Postgres); `/readyz` checks the
consumer; document the reconnect behaviour explicitly rather than leaving it to be discovered.

**Blocks:** PR 2.

---

## Not blocking

**Multi-tab.** Already written into `websocket.md` as a deliberate inconsistency — `job_status` goes
to every connection the replica holds for a session, `delta` and `done` still go only to the socket
that started the turn. Resolving it means answering a product question phase 5 owns.
