
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN pip install --no-cache-dir uv==0.11.28
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --locked --no-dev --no-editable

RUN useradd --create-home --uid 10001 orchestrator && chown -R orchestrator:orchestrator /app
USER orchestrator

ARG VCS_REF
LABEL org.opencontainers.image.source="https://github.com/jbeneitezh/hermes-orchestrator" org.opencontainers.image.revision=$VCS_REF

EXPOSE 8080

CMD ["sh", "-c", "uv run --no-sync alembic upgrade head && uv run --no-sync uvicorn hermes_orchestrator.main:app --host 0.0.0.0 --port 8080"]
