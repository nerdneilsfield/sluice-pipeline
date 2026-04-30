FROM python:3.12-slim

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy dependency manifests first for layer caching
COPY pyproject.toml uv.lock README.md ./

# Install production dependencies (all extras, no dev tools) without the
# editable project first, so this layer stays cacheable across source edits.
RUN uv sync --no-dev --all-extras --frozen --no-install-project

# Copy application source
COPY sluice/ ./sluice/

# Install the project entry point after copying source.
RUN uv sync --no-dev --all-extras --frozen

# Data and config directories are expected to be bind-mounted at runtime.
# Create them so the container starts cleanly even without mounts.
RUN mkdir -p /app/configs /app/data

ENTRYPOINT ["uv", "run", "--no-sync", "sluice"]
