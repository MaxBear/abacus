# abacus

Chat-driven portfolio analytics. You paste a portfolio, ask a question in English, and get back
computed analytics with charts — backtests, Monte Carlo simulations, correlation and drawdown.

**Analytics, not advice.** It computes statistics on data you supply. It does not recommend
securities. Market data comes from free public sources (Stooq, FRED).

## What this actually is

A **cloud-to-scheduler bridge**, with a numerical solver where an HPC scheduler would normally sit.

The LLM performs zero calculation. It turns English into an `AnalysisRequest`, decides which analysis
answers the question, and explains the result afterward. A job broker hands the work to a stateless
worker container, which runs a CPU-bound numpy/pandas solve for 30 seconds to five minutes and writes
artifacts to object storage.

That division — model orchestrates, compiled code computes, scheduler owns the handoff — is the
architecture of an LLM agent driving real computation, and the reason this project exists.

## Status

**Phase 0 — skeleton.** FastAPI + Postgres + RabbitMQ under compose, with the liveness/readiness
split and a production-shaped Dockerfile. No jobs, no agent, no frontend yet.

Roadmap: `1` chat over WebSocket → **`2` the job broker, built twice (Postgres and RabbitMQ,
benchmarked)** → `3` containerized workers → `4` the analytics engine → `5` chat UI → `6` cloud.

[`docs/roadmap.md`](docs/roadmap.md) gives each phase what it lands, how it is accepted, and the
decision it forces. Phase 1's transport is designed ahead of the code in
[`docs/websocket.md`](docs/websocket.md) — frame protocol, resume, fan-out across replicas, and
drain-on-deploy, through phase 6.

## Quick start

```bash
make up          # build + start api, postgres, rabbitmq
curl localhost:8000/livez      # {"status":"ok"}
curl localhost:8000/readyz     # {"status":"ready", ...}
make verify      # Phase 0 acceptance checks
make down        # stop, keeping the database
make reset       # stop and destroy the database
```

Local test run needs no containers. Requires [uv](https://docs.astral.sh/uv/)
(`brew install uv`); it downloads its own CPython 3.12, so no system Python is involved:

```bash
make install     # .venv from uv.lock, exactly
make test
make lint
```

Adding or bumping a dependency means editing `pyproject.toml` and running `make lock`.

RabbitMQ management UI: http://localhost:15672 (`abacus` / `abacus`).

`make chat` serves `dev/chat.html`, a browser client for `/ws/chat/{session_id}` — raw frames in
one pane, the assembled transcript in the other, and buttons for the paths a unit test cannot
reach from a browser: the connection cap, refused handshakes, close codes. It is a test harness,
not the phase-5 UI.

## Layout

```
api/         FastAPI routes and DI wiring only — endpoints, handshake policy.
core/        Domain: config, frame models, connection transport, the chat
             protocol handler, and later the JobQueue Protocol, sessions, jobs.
adapters/    Infrastructure behind interfaces: postgres, broker, object store.
docs/        Design notes written ahead of the code.
dev/         Local tooling. Not packaged, not served by the app.
worker/      Job consumer + solver runner.            (phase 3)
gateway/     LLM gateway — the only holder of the Anthropic key.  (phase 4)
infra/       Terraform + k8s manifests.                (phase 6)
```

## Three decisions worth knowing about

**Dependencies are locked, not just pinned.** `pyproject.toml` pins the eight direct
dependencies; `uv.lock` pins those *and* the ~30 transitives they drag in — starlette, anyio,
greenlet, the `uvicorn[standard]` extras — which the old `requirements.txt` left floating. Two
builds of the same commit a month apart used to be able to produce different images. The Docker
build runs `uv sync --locked`, which fails if the lock has drifted from `pyproject.toml` rather
than quietly resolving something new. `uv.lock` is the only lockfile: nothing here reads a
`requirements.txt`, and exporting one would just be a second copy of the same facts, free to go
stale. Anything that needs pip can generate one on demand with `uv export`.


**`/livez` and `/readyz` are different things.** Liveness checks nothing but that the process can
serve a request; readiness checks Postgres and RabbitMQ concurrently, with a timeout. If liveness
checked the database, a database blip would make the kubelet kill and restart every pod — converting
a recoverable dependency outage into a crash loop. `tests/test_health.py` guards this.

**The Dockerfile uses exec-form `ENTRYPOINT`.** In shell form, `/bin/sh` becomes PID 1 and does not
forward `SIGTERM`, so Kubernetes' termination signal is ignored and the container is `SIGKILL`ed
after the grace period — dropping in-flight requests. `make verify` asserts the container stops in
under 10 seconds against a 30-second grace period.

## License

Private, unlicensed. Market data belongs to its respective providers.
