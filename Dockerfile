# Multi-stage: the build toolchain never reaches the runtime image.
FROM python:3.12-slim AS builder

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Dependencies before source, so a source edit doesn't bust the install layer.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


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
