# Multi-stage: the build toolchain never reaches the runtime image.
FROM python:3.12-slim AS builder

# Pinned uv binary, not `pip install uv` — no resolver run, no build toolchain.
COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /usr/local/bin/uv

# uv defaults to .venv next to pyproject.toml; put it where the runtime expects.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Dependencies before source, so a source edit doesn't bust the install layer.
# --no-install-project is what makes that split possible: it resolves and
# installs the 8 deps (plus transitives) without needing abacus itself to build.
# --frozen fails rather than silently re-resolving if uv.lock is out of date.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Then the source, and abacus itself as a real wheel in /opt/venv.
COPY . .
RUN uv sync --frozen --no-dev --no-editable


FROM python:3.12-slim AS runtime

# Non-root. Matched by runAsNonRoot in the k8s pod spec later.
RUN groupadd --system app && \
    useradd --system --gid app --home-dir /app --shell /sbin/nologin app

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
# No secrets in ENV — ENV and ARG both persist in image history (`docker history`).
# Config arrives at runtime via env_file / k8s Secret.

# No source copy: abacus is installed into /opt/venv as a wheel, so api/, core/
# and friends import from site-packages. Copying the tree here as well would
# shadow that install (uvicorn puts cwd on sys.path) and ship tests/ besides.
USER app

EXPOSE 8000

# Exec form: uvicorn is PID 1 and receives SIGTERM directly.
# Shell form would make /bin/sh PID 1, which does not forward signals — the
# container would hang until terminationGracePeriodSeconds and get SIGKILLed,
# dropping in-flight requests.
# ENTRYPOINT fixes what the image does; CMD supplies overridable default args.
ENTRYPOINT ["uvicorn", "api.main:app"]
CMD ["--host", "0.0.0.0", "--port", "8000"]
