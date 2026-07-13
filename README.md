# Hermes Orchestrator

Plano de control para coordinar instancias aisladas de Hermes Agent. Mantiene catálogo, tareas/runs, ACL, MCP, reconciliación de flota y contabilidad de uso sobre PostgreSQL.

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
- `GET /v1/agents` y `GET /v1/agents/{id}`: catálogo observado.
- `POST /v1/agents/requests`: solicitud idempotente de alta; no crea contenedores.
- `GET /v1/execution-profiles`: perfiles efectivos permitidos.
- `POST /v1/tasks`, `GET /v1/tasks/{id}`: objetivo durable separado de sus intentos.
- `POST /v1/tasks/{id}/dispatch|comments|cancel`: comandos idempotentes del ciclo.
- `GET /v1/runs/{id}`, `POST /v1/runs/{id}/approval`: intento y gate revisable.
- `GET /v1/fleet/status`: estado del Compose observado por el runner privado.
- `POST /v1/fleet/reconcile-requests`: dry-run/apply idempotente con allowlists y approval independiente.
- `GET /v1/usage/summary`: rollups por operación, agente, perfil o día sin convertir desconocidos en cero.
- `GET /v1/usage/runs/{run_id}`: asiento contable durable de un run.
- `GET /v1/usage/control-status`: presupuestos, cuota, circuitos y auditoría de controles.
- `POST /v1/usage/circuits/{id}/reset`: reset auditado, reservado al operador/owner.
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

Las rutas gobernadas exigen `X-Actor-Id`. El rol se resuelve desde `HERMES_ORCHESTRATOR_ACTOR_ROLES`; el cliente no puede declarar ni elevar su rol. Las mutaciones exigen además `Idempotency-Key`. Esta resolución es el bootstrap de confianza para la red privada y se sustituirá por autenticación fuerte sin cambiar el servicio de políticas.

El API no monta Docker. `fleet-reconciler` es un proceso privado separado que valida el Compose renderizado y solo ejecuta `config`, `pull` y `up --no-deps` sobre workers allowlisted. El socket no se entrega al líder ni al operador.

Cada run terminal genera como máximo un asiento en `usage_ledger`. Antes de cada dispatch se evalúan concurrencia, fan-out, retry, cuota, presupuesto soft/hard y circuito worker/perfil. Los límites por defecto provienen de `HERMES_ORCHESTRATOR_USAGE_*`; la tabla `budgets` permite estrecharlos por proyecto, agente, perfil o categoría y el presupuesto de la Task puede estrecharlos aún más.

## Arquitectura

La decisión inicial está en [ADR-001](docs/adr/ADR-001-control-plane-foundation.md). El estado durable será PostgreSQL y las modificaciones de esquema se harán exclusivamente mediante Alembic.
