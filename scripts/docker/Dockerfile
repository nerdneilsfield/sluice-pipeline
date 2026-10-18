FROM python:3.12-slim

RUN pip install --no-cache-dir uv==0.7.10
# Keep uv's download cache out of the image layers.
ENV UV_NO_CACHE=1
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock README.md LICENSE ./
# Install runtime extras only; test/dev extras are not part of the image.
RUN uv sync --locked --no-dev --extra web --extra ui --extra prefect --no-install-project

COPY src/ ./src/
RUN uv sync --locked --no-dev --extra web --extra ui --extra prefect --no-editable

RUN groupadd --gid 10001 sluice \
 && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin sluice \
 && mkdir -p /app/configs /app/data \
 && chown 10001:10001 /app/configs /app/data
USER 10001
ENTRYPOINT ["sluice"]
CMD ["--help"]
