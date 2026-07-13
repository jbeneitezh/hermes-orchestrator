FROM ghcr.io/astral-sh/uv:0.11.28 AS uv

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --locked --no-dev --no-editable

RUN useradd --create-home --uid 10001 orchestrator && chown -R orchestrator:orchestrator /app
USER orchestrator

EXPOSE 8080

CMD ["sh", "-c", "uv run --no-sync alembic upgrade head && uv run --no-sync uvicorn hermes_orchestrator.main:app --host 0.0.0.0 --port 8080"]
