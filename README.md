# Hermes Orchestrator

Plano de control para coordinar instancias aisladas de Hermes Agent. Este primer vertical slice establece la API, la configuración tipada y PostgreSQL; todavía no implementa agentes ni tareas.

## Requisitos

- Python 3.12 o superior.
- [`uv`](https://docs.astral.sh/uv/).
- Docker y Docker Compose para la validación integrada.

## Desarrollo local

```powershell
uv sync
Copy-Item .env.example .env
docker compose up -d postgres
uv run alembic upgrade head
uv run uvicorn hermes_orchestrator.main:app --reload
```

La API queda disponible en `http://localhost:8080`:

- `GET /health`: comprueba API y conexión a PostgreSQL.
- `GET /v1/capabilities`: publica las capacidades implementadas por esta versión.
- `GET /docs`: OpenAPI interactivo generado por FastAPI.

## Calidad

```powershell
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

## Compose completo

```powershell
docker compose up --build -d
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/v1/capabilities
docker compose down -v
```

No guardes secretos en `.env.example` ni en el repositorio. La variable `HERMES_ORCHESTRATOR_DATABASE_URL` acepta la URL de PostgreSQL del entorno.

## Arquitectura

La decisión inicial está en [ADR-001](docs/adr/ADR-001-control-plane-foundation.md). El estado durable será PostgreSQL y las modificaciones de esquema se harán exclusivamente mediante Alembic.
