# ADR-001 — Fundamentos del plano de control

- Estado: aceptada
- Fecha: 2026-07-13

## Contexto

Hermes Agent ofrece ejecución, sesiones y telemetría por instancia, pero no un dominio durable para gobernar varias identidades aisladas. Tradix necesita una API central que pueda crecer sin acoplar el núcleo cuantitativo al runtime de Hermes.

## Decisión

- Implementar el plano de control como aplicación Python con FastAPI.
- Usar configuración tipada y variables con prefijo `HERMES_ORCHESTRATOR_`.
- Usar PostgreSQL como verdad durable desde el primer vertical slice.
- Versionar todo cambio de esquema mediante Alembic.
- Mantener API, scheduler y consumidores en el mismo despliegue hasta que una medida real justifique separarlos.
- Publicar OpenAPI y capacidades explícitas; una capacidad no se anuncia hasta estar implementada.
- No crear todavía tablas de agentes o tareas: corresponden a los siguientes hitos.

## Consecuencias

- El entorno local necesita PostgreSQL, resuelto por Compose.
- La prueba de salud puede detectar que la API vive pero no está lista por pérdida de base de datos.
- El lockfile de `uv`, los checks y una imagen no root hacen reproducible el bootstrap.
- Se evita una arquitectura distribuida prematura, manteniendo una ruta clara para separar procesos en el futuro.
