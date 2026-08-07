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

## Quick start

```bash
make up          # build + start api, postgres, rabbitmq
curl localhost:8000/healthz    # {"status":"ok"}
curl localhost:8000/readyz     # {"status":"ready", ...}
make verify      # Phase 0 acceptance checks
make down
```

Local test run needs no containers:

```bash
make install
make test
make lint
```

RabbitMQ management UI: http://localhost:15672 (`abacus` / `abacus`).

## Layout

```
api/         FastAPI — HTTP + WebSocket. No business logic.
core/        Domain: config, and later the JobQueue Protocol, sessions, jobs.
adapters/    Infrastructure behind interfaces: postgres, broker, object store.
worker/      Job consumer + solver runner.            (phase 3)
gateway/     LLM gateway — the only holder of the Anthropic key.  (phase 4)
infra/       Terraform + k8s manifests.                (phase 6)
```

## Two decisions worth knowing about

**`/healthz` and `/readyz` are different things.** Liveness checks nothing but that the process can
serve a request; readiness checks Postgres and RabbitMQ concurrently, with a timeout. If liveness
checked the database, a database blip would make the kubelet kill and restart every pod — converting
a recoverable dependency outage into a crash loop. `tests/test_health.py` guards this.

**The Dockerfile uses exec-form `ENTRYPOINT`.** In shell form, `/bin/sh` becomes PID 1 and does not
forward `SIGTERM`, so Kubernetes' termination signal is ignored and the container is `SIGKILL`ed
after the grace period — dropping in-flight requests. `make verify` asserts the container stops in
under 10 seconds against a 30-second grace period.

## License

Private, unlicensed. Market data belongs to its respective providers.
