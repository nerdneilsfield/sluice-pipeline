FROM python:3.12-slim

RUN pip install --no-cache-dir uv
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock README.md LICENSE ./
# Install runtime extras only; test/dev extras are not part of the image.
RUN uv sync --locked --no-dev --extra web --extra ui --extra prefect --no-install-project

COPY src/ ./src/
RUN uv sync --locked --no-dev --extra web --extra ui --extra prefect --no-editable

RUN mkdir -p /app/configs /app/data
ENTRYPOINT ["sluice"]
CMD ["--help"]
