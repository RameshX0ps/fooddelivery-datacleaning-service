# Use a slim Python base image
FROM python:3.11-slim AS base

# Install system dependencies (for psycopg2, pandas, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv globally
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY fooddelivery ./fooddelivery
COPY tests ./tests

# Sync dependencies (installs into .venv)
RUN uv sync --frozen --no-dev

# Ensure .venv binaries (python, uv) are on PATH
ENV PATH="/app/.venv/bin:$PATH"

# Default run command
# Entrypoint script switches between modes
CMD if [ "$APP_MODE" = "api" ]; then \
      python -m uvicorn fooddelivery.api:app --host 0.0.0.0 --port 8000; \
    else \
      python -m fooddelivery.main; \
    fi