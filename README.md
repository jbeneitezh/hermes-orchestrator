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
- `GET /v1/agents/requests/{id}`: detalle y estado auditable de la solicitud.
- `POST /v1/agents/requests/{id}/decide`: aprobación o rechazo independiente e idempotente.
- `POST /v1/agents/requests/{id}/retire`: retirada lógica de una solicitud aplicada o fallida.
- `POST /v1/agents/requests/{id}/provision`: materializa una solicitud aprobada mediante plantilla allowlisted.
- `POST /v1/agents/requests/{id}/rollback`: detiene y retira del Compose managed sin borrar datos.
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
- `GET /v1/environments`: definiciones y despliegues históricos de local/dev/pre/prod-sim.
- `POST /v1/environments/deployments`: crea local o dev desde una rama y resuelve su SHA.
- `POST /v1/environments/promotions`: promueve dev a pre por SHA y pre a prod-sim por tag, con aprobación independiente.
- `POST /v1/environments/{environment}/rollback`: recupera un candidato inmutable anterior sin reescribir historial.
- `GET /operations`: mesa de operaciones read-only, sin acceso directo a DB o Hermes.
- `GET /v1/operations/{fleet,tasks,timeline,usage,approvals,quota}`: seis proyecciones públicas filtrables para UI y watchdog.
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

Una `AgentRequest` transita de `pending` a `approved` o `rejected`. La aprobación nunca puede hacerla el solicitante. Cuando `HERMES_ORCHESTRATOR_AGENT_POLICY_ENABLED=true`, el `workflow-coordinator` usa el actor estable `system:agent-policy` para validar perfiles versionados, rechazar cualquier elevación y entregar al provisionador únicamente solicitudes allowlisted. El proceso comprueba rol, capabilities, secret refs, comunicación, perfil de mounts, modelo, presupuesto y capacidad de flota; el replay no repite la aplicación. Las decisiones externas siguen bajo control de su aprobador original. La retirada cambia el estado a `retired` y conserva solicitud, decisiones y eventos de auditoría.

El catálogo v3 incluye `data_steward` y `risk_manager`. El segundo se materializa desde un template versionado con manifiesto, SOUL, fundamento y contexto; recibe Knowledge escribible en rama y mounts read-only para dataset y Tradix. No dispone de producto write, órdenes, capital, secretos, Docker, promoción, merge ni autoaprobación.

El API no monta Docker. `fleet-reconciler` es un proceso privado separado que valida el Compose renderizado y solo ejecuta `config`, `pull` y `up --no-deps` sobre workers allowlisted. El socket no se entrega al líder ni al operador.

Cada run terminal genera como máximo un asiento en `usage_ledger`. Antes de cada dispatch se evalúan concurrencia, fan-out, retry, cuota, presupuesto soft/hard y circuito worker/perfil. Los límites por defecto provienen de `HERMES_ORCHESTRATOR_USAGE_*`; la tabla `budgets` permite estrecharlos por proyecto, agente, perfil o categoría y el presupuesto de la Task puede estrecharlos aún más.

Los entornos se registran en PostgreSQL y no mutan reglas remotas de GitHub. `local` deriva su identidad de una Task, recibe puerto de un pool gobernado y expira por TTL; `dev` conserva rama y SHA resuelto; `pre` queda congelado por SHA; `prod-sim` conserva tag y SHA. `live` se deniega en v1. Repositorios y pool local se configuran con `HERMES_ORCHESTRATOR_ENVIRONMENT_*`.

`operations-watchdog` consulta únicamente las rutas públicas de tareas y timeline. Cada 2,5 horas como máximo genera un rollup determinista cuando coinciden trabajo activo y eventos nuevos. En idle no crea Runs ni llamadas de modelo; su estado JSON mantiene `model_calls=0` y se proyecta en la ruta de quota.

## Arquitectura

La decisión inicial está en [ADR-001](docs/adr/ADR-001-control-plane-foundation.md). El estado durable será PostgreSQL y las modificaciones de esquema se harán exclusivamente mediante Alembic.
