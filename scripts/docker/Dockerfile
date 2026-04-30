FROM python:3.12-slim

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy dependency manifests first for layer caching
COPY pyproject.toml uv.lock ./

# Install production dependencies (all extras, no dev tools)
RUN uv sync --no-dev --all-extras --frozen

# Copy application source
COPY sluice/ ./sluice/

# Data and config directories are expected to be bind-mounted at runtime.
# Create them so the container starts cleanly even without mounts.
RUN mkdir -p /app/configs /app/data

ENTRYPOINT ["uv", "run", "sluice"]
