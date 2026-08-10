# Multi-stage: the build toolchain never reaches the runtime image.
FROM python:3.12-slim AS builder

# Pinned, not :latest — a floating builder tool would undo the point of a lockfile.
COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /usr/local/bin/uv

WORKDIR /app

# UV_PYTHON_PREFERENCE: use the interpreter this base image already ships. The
# project default is only-managed, which is right on a dev laptop with five
# pythons on PATH, but here it would download a sixth and build a venv whose
# symlinks point at a builder path that does not exist in the runtime stage.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-system

# Dependencies before source, so a source edit doesn't bust the install layer.
# --locked fails the build if uv.lock has drifted from pyproject.toml, rather
# than silently resolving something new.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev


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

COPY --chown=app:app . .
USER app

EXPOSE 8000

# Exec form: uvicorn is PID 1 and receives SIGTERM directly.
# Shell form would make /bin/sh PID 1, which does not forward signals — the
# container would hang until terminationGracePeriodSeconds and get SIGKILLed,
# dropping in-flight requests.
# ENTRYPOINT fixes what the image does; CMD supplies overridable default args.
ENTRYPOINT ["uvicorn", "api.main:app"]
CMD ["--host", "0.0.0.0", "--port", "8000"]
