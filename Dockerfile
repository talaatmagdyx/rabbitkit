# syntax=docker/dockerfile:1
# Multi-stage build for rabbitkit consumers/operators.
#
# The build stage installs the package (with the desired extras) into a clean
# venv; the runtime stage copies only the venv + app code onto a slim image that
# runs as a non-root user. No secrets are baked in — operators mount/inject
# credentials via Kubernetes Secrets or environment variables at runtime.

ARG PYTHON_VERSION="3.12"

# ── build stage ────────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS build

# Build dependencies: nothing heavy required (pure-Python wheel build). If you
# add compiled deps, install build-essential + the relevant dev headers here.
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install uv for fast, reproducible installs.
RUN pip install --no-cache-dir uv

WORKDIR /app

# Install dependencies first (better layer caching than copying everything).
COPY pyproject.toml README.md LICENSE ./
COPY src ./src

# Install the package with the extras your deployment needs. Use [all] for the
# full feature set, or a narrower group (e.g. [sync,redis,pydantic]) to keep
# the image small. Adjust EXTRA_INDEX_URL here if you need a private index.
ARG INSTALL_EXTRAS="[all]"
RUN uv venv /opt/venv \
    && VIRTUAL_ENV=/opt/venv uv pip install ".${INSTALL_EXTRAS}"

# ── runtime stage ──────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:${PATH}" \
    RABBITMQ_BLOCKED_CONNECTION_TIMEOUT=60

# Create a non-root user and give it the venv.
RUN groupadd --system --gid 1001 rabbitkit \
    && useradd --system --uid 1001 --gid rabbitkit --create-home --home-dir /home/rabbitkit rabbitkit

COPY --from=build --chown=root:root /opt/venv /opt/venv

WORKDIR /home/rabbitkit
USER rabbitkit

# Operators override ENTRYPOINT/CMD with their consumer entrypoint, e.g.
#   command: ["python", "-m", "myapp.consumer"]
# The placeholder keeps the image runnable for inspection without executing
# anything sensitive by default.
ENTRYPOINT ["python", "-c"]
CMD ["import rabbitkit; print('rabbitkit', rabbitkit.__version__)"]

# No secrets baked in: all runtime config (RABBITMQ_HOST, credentials, ...) is
# injected via environment variables / mounted Secrets at deploy time.
