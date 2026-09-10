# Entrega por Woodpecker

Los gates existentes se mantienen: gitleaks 8.30.1, uv 0.11.28 y lock,
ruff check/format, mypy y pytest con cobertura mínima 95%.
Las PR ejecutan checks sin credenciales de publicación. Push/manual de master
o de la rama de migración publica únicamente canary tras pasar los gates.
Las imágenes de API y fleet usan el SHA completo; GHCR deja de ser dependencia
de los Dockerfiles: uv se instala desde su versión fijada en PyPI.

El registry es `host.docker.internal:5443` para el agente y
`192.168.1.197:5443` para LAN. Repositorios: `tradix/orchestrator-canary`
y `tradix/fleet-canary`. Promoción por digest y revisión independiente.
No se cambia ningún servicio científico existente desde este pipeline.
