# Phase 1b — persistence

Draft. The transport half is settled in [`websocket.md`](websocket.md) and this does not change a
frame shape it defines; it fills in the two fields that document deliberately left unallocated
(`seq`, and durable `ack`) and adds `resume`.

The acceptance test for the whole phase is one sentence from
[`roadmap.md`](roadmap.md): **kill the socket mid-turn, reconnect, and lose nothing.**

## First: the open question, settled

`websocket.md` deferred this to 1b, "where it is testable":

> **Whether `delta` frames get their own sequence space.** Persisting one row per token to allocate a
> `seq` is absurd; the likely answer is that deltas carry `(message_id, chunk_index)` and only the
> terminal `done` consumes a session `seq`, with resume replaying completed messages whole rather
> than re-streaming them.

**Adopt that answer, with one correction: `seq` is a property of a row in `chat_messages`, and user
messages get one too.**

The rule that makes it coherent is already in `websocket.md:125`: a `seq` is allocated *when the
frame's underlying fact is written to Postgres*. A delta is not a fact — it is a rendering detail of
a message still being produced. A message is a fact. So:

| frame | carries `seq`? | why |
| --- | --- | --- |
| `ack` | yes — the user message's | the user's message is a durable row; that is what makes the ack meaningful |
| `delta` | **no** | addressed by `(message_id, chunk_index)`, never replayed, never stored per-chunk |
| `done` | yes — the assistant message's | the reply is complete and its text is final |
| `error` | no | not a durable fact about the session |
| `job_status` | yes — the transition's | phase 3; a `job_events` row, and the reason that table exists |
| `job_progress` | **no** | phase 5; a percentage is in no row and cannot be reconstructed |

The doc said "only the terminal `done` consumes a session `seq`". That is right about deltas and
wrong about user messages: `client_msg_id` idempotency requires the user's message to be a row that
can be looked up, and a row that exists but has no `seq` cannot be replayed on resume. One `seq` per
row, allocated at insert, keeps a single rule instead of two.

The last two rows arrived in phase 3 and are the rule holding rather than bending — see
[`job_events`](#job_events-phase-3) below. It is worth noting what the rule *refused* there, because
it is the same refusal as `delta`: not "should jobs be on the socket" but "may a frame carry a
number that sometimes means something." The answer stayed no both times.

**What resume replays, therefore, is completed messages — not token streams.** A reconnect during a
turn gets the assistant row in whatever state it holds, then live deltas continue on the same
`message_id`. Re-streaming a half-finished reply token by token would buy nothing: the client
already renders text by appending, so handing it the accumulated text is the same end state in one
frame instead of four hundred.

This is also what keeps write volume sane. A 500-token reply is 2 rows and 2 `seq` allocations, not
500 of each.

## Schema

```sql
create table chat_sessions (
  id           uuid primary key,          -- the canonical uuid from the URL path
  next_seq     bigint      not null default 1,
  created_at   timestamptz not null default now(),
  last_seen_at timestamptz not null default now()
);

create table chat_messages (
  id            bigint generated always as identity primary key,
  session_id    uuid   not null references chat_sessions(id) on delete cascade,
  seq           bigint not null,
  message_id    uuid   not null,           -- the wire identity, server-minted
  role          text   not null,
  status        text   not null,
  text          text   not null default '',
  client_msg_id text,                      -- user rows only; the client's idempotency key
  created_at    timestamptz not null default now(),
  completed_at  timestamptz,

  constraint chat_messages_role_check   check (role   in ('user', 'assistant')),
  constraint chat_messages_status_check check (status in ('streaming', 'complete', 'failed')),
  constraint chat_messages_client_msg_id_role_check
    check ((role = 'user') = (client_msg_id is not null))
);

create unique index chat_messages_session_seq_key
  on chat_messages (session_id, seq);

create unique index chat_messages_message_id_key
  on chat_messages (message_id);

create unique index chat_messages_session_client_msg_id_key
  on chat_messages (session_id, client_msg_id)
  where client_msg_id is not null;
```

Choices worth stating:

- **`bigint identity` PK, with `message_id uuid` as a separate wire identity.** `message_id` is
  `uuid4` and random, so making it the primary key scatters inserts across the index and fragments
  it as `chat_messages` grows. The internal key stays sequential; the exposed one stays opaque.
- **`chat_sessions.id` is the uuid from the path**, not a surrogate. `api/chat.py:93` already
  refuses anything but the canonical form precisely so this key is unambiguous.
- **`timestamptz`, `text`, `bigint`** throughout — no `timestamp`, no `varchar(n)`, no `int`.
- **`status` and `role` are text with check constraints** rather than Postgres enums, so adding a
  state later is a constraint change and not a type migration.
- **The third check constraint ties `client_msg_id` to `role`**: user rows must have one, assistant
  rows must not. Without it the partial unique index silently permits an assistant row to carry a
  client's key.
- **`(session_id, seq)` is the composite the resume query wants** — equality column first, range
  column second — and doubles as the required index on the foreign key.

## `seq` allocation, gap-free

A Postgres sequence is the wrong tool: it is global rather than per-session, and rollbacks burn
values, which is exactly the gap the contract forbids. Allocate from a counter column instead:

```sql
update chat_sessions
   set next_seq = next_seq + 1, last_seen_at = now()
 where id = $1
returning next_seq - 1 as seq;
```

The `UPDATE` takes a row lock, so concurrent turns on one session serialize on that row and cannot
mint the same number. A rolled-back transaction rolls back the increment too, so a failed insert
consumes nothing — which is what makes it gap-free rather than merely unique. Contention is scoped
to a single session, which `ws_max_concurrent_turns = 4` already bounds.

**Allocate and insert in the same transaction, and keep it short** — the row lock is held to commit,
so no LLM call, no socket write, and no `await` on anything but the database may sit inside it.

Both properties were checked against the real Postgres in `compose.yaml` rather than argued: a
rolled-back allocation left the counter untouched, and 40 concurrent allocations across 40
connections produced 40 distinct, contiguous numbers.

## `client_msg_id` idempotency

```sql
insert into chat_messages (session_id, seq, message_id, role, status, text, client_msg_id)
values ($1, $2, $3, 'user', 'complete', $4, $5)
on conflict (session_id, client_msg_id) where client_msg_id is not null
do nothing
returning seq, message_id;
```

No row returned means the key was already recorded, so the handler re-reads the existing row and
re-`ack`s it with the original `seq` and `message_id` rather than starting a second turn
(`websocket.md:147-152`). `do nothing` in preference to a no-op `do update`: the update form always
returns the row but writes a dead tuple on every duplicate, and duplicates here are the rare path.

This is the one place a check-then-insert would be a real bug rather than a style problem — two
reconnecting tabs replaying the same message is precisely the race, and each duplicate costs a
five-minute solve.

## Resume

```sql
select seq, message_id, role, status, text
  from chat_messages
 where session_id = $1 and seq > $2
 order by seq
 limit $3;
```

Bounded by `$3` (`ws_resume_max_messages`, 200). There are two ways a cursor can be unusable, and
only one of them is reachable today:

- **Too far behind.** More rows exist after `last_seq` than the bound will replay. This is what is
  implemented: ask for `limit + 1`, and a full extra row means the client is past the bound. It
  refuses rather than truncating, because a truncated replay hands the client a gap it cannot see —
  it advances its cursor past frames it never received and never asks again.
- **Below the oldest retained row.** Unreachable until something deletes, since `seq` 1 is always
  still there. It becomes the second trigger the day a retention window lands; see Open.

Either way the client gets `resume_too_old` and reloads over plain HTTP. The `(session_id, seq)` index serves this as
a single range scan, confirmed against the real database — no sort node, since the index already
supplies the order:

```
Limit
  ->  Index Scan using chat_messages_session_seq_key on chat_messages
        Index Cond: ((session_id = '…'::uuid) AND (seq > 1))
```

`resume` must be sent **before** anything else on a reconnected socket. Nothing on the server
enforces that — it cannot, since a first frame is a first frame — but a client that submits first
interleaves its own `ack` into the replay and has to reconcile the two orderings itself.

Replayed rows arrive as `message` frames, one per row, carrying `(seq, message_id, role, status,
text)` — the select list above, unchanged. `status` is on the wire because a replayed assistant row
is not always finished: a reply whose socket died mid-turn comes back `failed`, and a client that
assumed otherwise would render a truncated answer as though the model meant to stop there.

## `job_events` (phase 3)

An amendment, written when phase 3 needed it. Everything above assumed the session log had exactly
one source, because in 1b it did. `worker.md` broke that assumption by owing the socket a
`job_status` frame that is a durable fact and therefore, under this document's own rule, a numbered
row — and a job transition is not a `chat_messages` row. It has no role, no text, no
`client_msg_id`, and the check constraints tying those together would have to be loosened to admit
it. Loosening a constraint so a foreign thing can pretend to be a message is how a schema starts
lying about what it holds.

```sql
create table job_events (
  id         bigint generated always as identity primary key,
  job_id     uuid   not null references jobs(id)          on delete cascade,
  session_id uuid   not null references chat_sessions(id) on delete cascade,
  seq        bigint not null,                 -- from chat_sessions.next_seq, as everything else
  state      text   not null,
  created_at timestamptz not null default now(),

  unique (session_id, seq),
  check (state in ('queued','running','done','failed','dead','cancelled'))
);
```

**One row per transition, not one per job.** The tempting economy is to give a job a single `seq` at
enqueue and let every transition reuse it, and it fails twice over. `seq` is a position in a log
rather than a name for a thing, so three transitions sharing a number cannot be ordered against the
messages that arrived between them — and a client resuming from past that number is never told the
job finished, because `where seq > $2` excludes it. Neither is a rendering nuisance; the second is a
spinner that never stops. Nor can the transitions borrow the seq of the assistant message that
caused the job: same arithmetic, and jobs need not have a message at all.

**`session_id` is denormalised** — reachable through `job_id`, carried anyway so resume is a range
scan on `(session_id, seq)` with no join, which is the same argument the `chat_messages` unique
constraint already makes. It is `not null` here although `jobs.session_id` is nullable: a job with no
session has nobody to tell, and writes no events.

**Written in the same transaction as the state change**, inside `PostgresJobStore`, where all six
transitions already live — `insert_or_get`, `cancel`, `claim`, `retire_lapsed`, `ack`, `nack`. That
placement is not tidiness. `retire_lapsed` runs in a maintenance loop with no worker watching and no
session context in scope, so any publisher bolted on further out would silently miss the transitions
of jobs whose consumer died — precisely the ones a waiting client most needs to hear about. Being
one transaction also means the row and the state can never disagree, which is what lets the
`chat.events` fanout be lossy without anything being lost.

The cost, stated rather than absorbed: **the worker now allocates in the chat sequence space.**
`PostgresJobStore` reaches `chat_sessions` for `next_seq`, taking the same row lock live turns take,
so the queue touches the schema `core/jobs.py:86` deliberately kept it independent of. Three or four
rows per five-minute job makes the contention theoretical; the coupling is real, and it is what a
replayable transition costs.

### Resume, merged

```sql
-- unchanged
select seq, message_id, role, status, text
  from chat_messages where session_id = $1 and seq > $2 order by seq limit $3;

-- and
select seq, job_id, state
  from job_events    where session_id = $1 and seq > $2 order by seq limit $3;
```

Two bounded range scans merged on `seq`, rather than one query returning a union of two row shapes
padded with nulls. The bound applies to the merged result, so each side asks for `limit + 1` and the
merge truncates — the `resume_too_old` test above is then unchanged, since it still asks whether
more rows exist after `last_seq` than the bound will replay.

Merging in the application rather than in SQL is a deliberate choice and a cheap one: both inputs
are sorted, both are bounded by the same constant, and the alternative buys a wider select list and
a `union all` whose column list has to be maintained in two places every time either row shape
grows.

## Work, in dependency order

1. **Alembic bootstrap.** No `alembic/` exists yet. Config, env, one initial revision. Decide
   up front whether migrations run in the container entrypoint or as a separate step; the latter is
   the only one that survives more than one replica.
2. **The tables above**, as that first revision.
3. **A repository behind a Protocol in `core/`**, implemented in `adapters/`. This is what
   `adapters/postgres/db.py:37`'s `session()` has been waiting for. The Protocol is what lets the tests keep
   running without containers, via `dependency_overrides` — the same seam `Responder` uses.
4. **`seq` on `ack` and `done`**, and the persistence calls in `ConnectionHandler`.
5. **`client_msg_id` idempotency** on the insert path.
6. **`resume`** as a client frame, plus `resume_too_old`.

Steps 4–6 each change `core/frames.py`, so they are additive frame changes under the existing `v1`
— no version bump, since a client that ignores `seq` still works.

## Testing

Per `websocket.md`'s Testing section and phase 0's habit, the suite stays container-free: a fake
repository behind the Protocol covers sequence and resume logic. Two things need a real Postgres and
should be a separate, marked suite:

- **Gap-free allocation under concurrency.** The row-lock argument is only a claim until two
  transactions race for the same session and both land. A fake repository cannot falsify it.
- **The idempotent insert.** `on conflict` semantics are the database's, not the fake's.

And the acceptance test itself — kill mid-turn, reconnect, lose nothing — wants the real thing, since
what it is really testing is that the durable state and the transport agree.

Note the trap `websocket.md` records: a test asserting "the server replays and then continues" hangs
rather than fails if replay silently returns nothing, because a healthy socket simply has nothing to
say. Use `asyncio.wait_for`.

## Open

- **Retention.** Nothing above deletes anything, so `chat_messages` grows without bound — and since
  phase 3, `job_events` alongside it. That no longer makes `resume_too_old` unreachable — the depth
  bound fires on its own — but it does leave the second trigger for that error unimplemented, and it
  is the input to whether these tables need partitioning by time later. Note that any retention
  policy has to cut both at the same `seq`, or a resume returns half a log.
- **Multi-tab writes.** Two sockets on one session now contend for the same `next_seq` row. Ordering
  stays correct; whether a second tab *should* be able to submit mid-turn is still the product
  question `websocket.md` flags, not a transport one.
- **The worker contends for `next_seq` too**, since phase 3. Same row lock, now taken from a second
  service on a schedule nobody coordinates. Correctness is unaffected — that is what the lock is for
  — but it is the first time the chat allocator has a writer outside the API, and it is the thing to
  look at first if session throughput ever becomes a question.
- **`Error` frames carry no `client_msg_id`**, so a client cannot tell which message was rejected.
  Adding it is an additive `v1` change and belongs here, with the rest of the idempotency work.
