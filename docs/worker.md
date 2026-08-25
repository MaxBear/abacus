# Phase 3 — the worker, and what "stateless" actually costs

Design-first, as [`jobs.md`](jobs.md) and [`websocket.md`](websocket.md) were. This phase is where
the queue stops being a contract and starts holding a process that computes for five minutes, and
almost everything interesting about it follows from one fact:

> **The thing phase 2 built is `asyncio`. The thing phase 3 runs is not.**

A CPU-bound solve and an AMQP connection cannot share a thread. That sentence is the whole design;
the rest of this document is what falls out of it.

[`roadmap.md`](roadmap.md#phase-3--containerized-workers) sets the acceptance: **kill a worker
mid-solve, the job is redelivered and completes elsewhere, and no partial artifact is ever visible
as a finished one.** The first half phase 2 already proves in
`test_a_consumer_cut_off_mid_job_has_its_work_finished_elsewhere` — the broker requeues what a dead
channel held, and the row's lease decides when someone else may take it. The second half is new, and
is the part that needs object storage to exist.

## The shape: a supervisor and a child

A worker container is **two processes**, not one.

- **The supervisor** owns the AMQP connection, the `RabbitMQJobQueue`, the lease, and the clock. It
  is `asyncio` and it never computes anything.
- **The solve child** is a separate OS process. It computes, writes its artifact, and exits.

The obvious alternative — one process, solve inside `run_in_executor` — fails twice, and the two
failures are worth separating because only one of them is about the GIL.

**It blocks the event loop.** `numpy` releases the GIL for many kernels, but a solve loop is Python
between those kernels, and the loop is what runs the AMQP heartbeat. RabbitMQ's default heartbeat is
60 seconds and two missed intervals close the connection, so a worker that stops scheduling for
two minutes has its channel torn down *by the broker, on the grounds that it is dead*. Everything the
channel held is requeued. A five-minute solve therefore does not merely risk this: it guarantees it,
and does so in the shape of a broker fault, which is the second time in this project that a lease
problem would have arrived wearing a disguise. It also stops `extend` from running, so the lease
lapses, so `_maintain` on another replica republishes and a second worker starts the same job — the
duplicate the design tolerates, arrived at for no reason.

**It cannot be stopped.** This is the one that decides the architecture. There is no way to interrupt
a Python thread from outside; `Thread` has no `kill`, and a `concurrent.futures` cancel only prevents
a task that has not started. A solve that has entered a single long call is unreachable for as long
as that call runs. A *process* can be signalled. So the boundary drawn to keep the event loop
scheduling is the same boundary that makes cancellation possible at all — which is why this is one
decision and not two.

`multiprocessing` with an explicit `spawn` start method, not `fork`: a forked child inherits the
parent's asyncio loop, its open AMQP socket, and its Postgres pool, all in a state no library
promises to survive duplication. The child needs none of them. It receives a job payload and a
destination, and returns a result or dies.

## The lease, extended for real

`extend` has been in the Protocol since phase 2 with no caller that meant it. `jobs.md` records what
it is: the row's deadline moves and the broker is not involved, because there is no AMQP way to say
"still working" about one delivery. The worker is the first thing that has to say it.

The supervisor extends on a timer while the child runs — comfortably inside the lease, so a slow
`extend` is not itself the thing that loses the job. Three cases, and only the first is ordinary:

- **`extend` returns.** It returns the *row's* job, not the lease's, which is how the worker learns
  it was cancelled without a second call. See below.
- **`extend` raises `StaleLease`.** Someone took the job: this worker's lease is void, the child's
  output is void, and there is exactly one correct action, which is to **kill the child now** rather
  than let it finish and discard. Finishing costs CPU that a competing worker is also spending, and
  — worse — a child that completes writes an artifact, and an artifact nobody will point at is
  garbage somebody has to collect. This is #9's fix arriving as a live requirement rather than a
  code review finding.
- **`extend` fails to reach Postgres.** Not the same as `StaleLease` and must not be treated as it.
  The lease may still be perfectly valid; the database is merely unreachable. Retry inside the
  remaining lease, and give up only when the lease is genuinely spent — at which point the honest
  move is to kill the child, because continuing to compute against a lease you can no longer defend
  is how two workers write two artifacts.

**A cap the lease cannot express.** A supervisor that is healthy while its child is wedged will
extend forever, and the row will look permanently `running`. So the solve gets a wall-clock deadline
of its own, independent of the lease and enforced by the supervisor: past it, kill the child and
`nack` with a real error. The lease answers "is this worker still alive?"; it was never able to
answer "is this job ever going to finish?", and phase 2's `_LAPSED` predicate cannot see the
difference.

## Cancelling something that is not listening

`jobs.md` splits `cancel` into the easy half — a state transition on an unreserved job, which phase 2
finished — and the hard half, which is this phase's. Three layers, in increasing order of violence:

1. **Between units of work.** `cancel_requested` on the row, learned from `extend`'s return value.
   A solve loop that iterates checks this and stops cleanly. `nack` then computes `cancelled` from
   the row rather than from anything the worker says, so a cancelled job is never reported as a
   failure.
2. **Inside a long call.** Nothing above helps: the child is not checking anything. The supervisor
   signals it. `SIGTERM` first, so a solver that wants a handler can have one.
3. **`SIGKILL` after a grace period.** Because the point of a separate process is that this always
   works.

The trap `jobs.md` names is worth repeating, because it is the failure mode of the obvious
implementation: **a cooperative `asyncio` mock cancels trivially, so a cancel built against the mock
passes its tests and then fails against real work.** The flag is slower and less impressive and it is
the version that survives contact with numpy. Layers 2 and 3 exist precisely because layer 1 is not
sufficient, and a test suite that only exercises layer 1 has proved nothing about this phase.

## Running twice, and the difference between that and *counting* twice

The roadmap asks how at-least-once is squared with solves that must not silently run twice. The
honest answer is that it is not, and cannot be, because **a lease expiring is a guess about a
process rather than proof about one.** A worker whose box is swapping, whose child is deep inside a
BLAS call, or whose connection to Postgres has blinked is still computing while its deadline passes.
`_sweep` finds the job in `lapsed_reservable` and offers it again, another worker claims it, and
nothing has gone wrong — there are simply two processes solving one job.

This is not the unbuilt Postgres backend showing through. It is the shipped implementation, where
two redelivery paths overlap and neither can see the other's evidence: **the channel**, when a
worker's process dies and RabbitMQ requeues everything it held without Postgres being consulted, and
**the row**, when a worker is alive but past its deadline — which the broker cannot see, because
`extend` has no broker half to tell it. `adapters/rabbitmq/job_queue.py` says as much in its own
docstring. Phase 2 chose the overlap knowingly; phase 3 is where the price is paid.

Phase 3 sharpens it rather than softening it. Phase 2's consumer was cooperative `asyncio` and
stopped the moment it was told to. Phase 3's solve is a `spawn`ed child inside numpy that checks
nothing until it returns, so between a supervisor learning its lease is stale and the child being
signalled and reaped, two live attempts exist by construction rather than by mishap.

What is preventable is **two executions having two effects**. Three things make that true, and they
are the same three that satisfy "no partial artifact is ever visible as a finished one":

- **The artifact key includes the lease id**, not just the job id. Two attempts write to two keys and
  never race for one. There is no last-writer-wins, because there is no shared destination.
- **Write, then ack.** The artifact is durable before the row is told about it, so a crash between
  them leaves an orphan — findable, deletable, and never mistaken for a result. The reverse order
  leaves a row pointing at an object that does not exist, which is the failure that surfaces to a
  user as a broken download three days later.
- **The row's `result_ref` is the only pointer that means anything.** Nothing lists the bucket to
  find results. Since `ack` is fenced on `lease_id`, exactly one attempt can ever install a pointer,
  and every other attempt's output is unreferenced by construction rather than by cleanup.

Single-object `PUT` is atomic in S3 and in MinIO — the object is not visible until the upload
completes — and an aborted multipart upload exposes nothing. So "partial artifact" is not a state a
reader can observe, and the acceptance criterion is met by the pointer discipline rather than by a
scan. Orphaned objects from lost races are a **cost, not a correctness problem**, and reaping them is
listed as open below rather than solved here.

## Statelessness, and its bill

The roadmap asks where a five-minute CPU-bound solve keeps intermediate state. In phase 3: **nowhere.
A killed solve restarts from zero.** That is what stateless costs, stated as a number rather than a
principle — the expected waste is a redelivery times the mean elapsed time before the kill, and with
`max_attempts` at 3 the worst case is three full solves for one result.

Checkpointing is deliberately not built, and the reason is not effort. A checkpoint written every N
seconds to object storage is a second dual write with the same three failure modes phase 2 spent a
`published_at` column repairing — and resuming from a checkpoint means resuming from *someone else's*
checkpoint, which needs the whole fencing argument again with a mutable object instead of a row. At
five minutes a solve is not worth that. At fifty it would be, and the doc should be reopened then
rather than pretended about now.

## Progress on the wire

`websocket.md` assigns phase 3 the worker's progress events, the `chat.events` fanout exchange, and
the per-replica queues. The mechanism is settled there: every API replica binds an exclusive,
auto-delete queue at startup, forwards each event to whichever connections for that session it holds
locally, and discards the rest. Lossy on purpose — durability is Postgres', and resume covers gaps.

What phase 3 has to settle is a tension the two documents create between them. `jobs.md` says
`job_status` carries `seq`, `job_id`, `state`, `progress` and "is a durable fact about a job", which
under `persistence.md`'s rule means it gets a `seq` and is replayable on resume. But **a percentage
is not a durable fact.** It is not in the row, it cannot be reconstructed after a reconnect, and
numbering it means either persisting every progress tick — an insert per tick per job, to make a
number obsolete on arrival — or leaving a hole in a sequence space whose gap-freeness
`persistence.md` treats as an invariant.

The resolution this document proposes, and marks open below: **state transitions and progress are
two different frames, not two fields of one.** A transition is durable, numbered, and reconstructible
from the row on resume; progress is unnumbered, lossy, and never replayed. A client that reconnects
mid-solve learns the state from the row and simply waits for the next progress tick. Merging them
into one frame with a sometimes-meaningful `seq` would put the exception inside the invariant, which
is the thing `persistence.md` exists to prevent.

## The stack

MinIO joins compose as the S3-compatible local object store, with an `adapters/s3/` package behind a
Protocol in `core/`, matching how `postgres` and `rabbitmq` already sit behind `ChatRepository` and
`JobQueue`. The worker is a second image built from the same wheel — same `Dockerfile`, different
entrypoint — so that "the worker runs the same code the API imports" is a build property rather than
a promise.

`prefetch=1` stops being a docstring's claim about phase 3 and becomes phase 3's actual setting, for
the reason `start()` already gives: a larger window hands one consumer several five-minute jobs while
others idle, and if it dies they all wait for the channel to close.

## Work, in dependency order

One concern per PR, as in every phase before this.

1. **The object store**: a Protocol in `core/`, `adapters/s3/`, MinIO in compose, and the config to
   reach it. Its own tests, skipping when nothing answers — `conftest.py`'s existing pattern.
2. **The solve child**: `spawn`, the payload contract, the signal handling, and a synthetic solver
   with a configurable burn. Testable with no broker and no database, which is the point of doing it
   before the supervisor.
3. **The supervisor**: reserve → spawn → extend loop → write → `ack`/`nack`, with the three `extend`
   outcomes and the wall-clock cap. This is the phase.
4. **The fanout**: `chat.events`, per-replica queues, and `job_status` on the wire.
5. **The acceptance check**: a script that kills a worker mid-solve and asserts the job completes
   elsewhere, in the shape of `verify-phase0.sh`.

Steps 1 and 2 are independent and could land in either order; 3 needs both.

## Testing

The contract suite does not grow here — phase 3 adds no queue semantics. What it adds is a process,
and the tests worth writing are the ones a single-process design would pass by accident:

- **The heartbeat survives a solve.** Burn CPU in the child for longer than two heartbeat intervals,
  then assert the connection is still open and the lease was extended throughout. This is the test
  that fails the moment someone "simplifies" the child back into an executor, which is the most
  likely future regression in this phase.
- **Kill mid-solve, complete elsewhere** — the acceptance, and phase 2's version of it with a real
  process instead of a closed connection.
- **`StaleLease` mid-solve kills the child**, and no pointer is installed. Assert on the row's
  `result_ref`, not on the bucket.
- **Cancel inside a long call.** The solver must be one that ignores the flag, or the test proves
  only that layer 1 works.
- **A wedged child hits the cap** and the job reaches `failed` with a real error rather than being
  extended forever.

Every wait bounded, for the reason this suite keeps rediscovering: a test asserting "and then it is
redelivered" hangs rather than fails when redelivery never happens.

## Settled

- **Two processes, `spawn`, not a thread pool.** The event loop must keep scheduling, and a solve
  must be killable; one boundary buys both.
- **The artifact key carries the lease id**, so competing attempts never share a destination.
- **Write, then ack.** Orphans are a cost; dangling pointers are a bug.
- **No checkpointing.** A five-minute solve restarts from zero, and the alternative is a second dual
  write plus the fencing argument again.
- **The solve gets a wall-clock cap** independent of the lease, because the lease can only report on
  the worker, never on the work.

## Open

- **Do progress and state share a frame?** Proposed above as no. It is `websocket.md`'s call, and it
  changes the phasing table there.
- **Reaping orphaned artifacts.** Every lost race leaves an object nothing points at. A bucket
  lifecycle rule keyed on age is the cheap answer; a sweep that joins against `result_ref` is the
  exact one. Neither is needed for correctness, and the cheap one probably wins.
- **What `kind` selects.** The Protocol treats it as opaque and phase 4 gives it meaning. Until then
  the worker needs a registry of one, and it is worth deciding now whether an unknown `kind` is
  `dead` immediately or retried — it will never succeed, so retrying it burns three solves' worth of
  lease time to reach the same answer.
- **Does the worker need its own readiness probe?** It has no port to serve one on. Kubernetes will
  ask in phase 6.
