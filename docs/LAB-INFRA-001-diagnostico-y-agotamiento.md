# LAB-INFRA-001: diagnóstico HTTP y agotamiento de intentos

Complemento de `d7c0840`, conforme al encargo del supervisor del 10 de septiembre
de 2026. Implementación y pruebas locales; la revisión independiente del nuevo
commit corresponde al supervisor. El informe anterior en
`.pytest_cache/infra-review.md` se conserva sin cambios.

## 1. HTTP status y diagnóstico seguro

`HermesRunsAdapter._raise_for_response` conserva el status recibido en
`HermesAdapterError.http_status` y `as_dict()`. El dispatcher lo persiste en
`Run.error_details.http_status`, incluso cuando el worker proporciona su propio
código de error JSON. Un campo de status dentro del JSON no sustituye al status
HTTP real. Los errores sin respuesta HTTP no reciben un status inventado.

Sólo se aceptan los campos JSON `code` y `message` de tipo cadena, tanto en el
objeto `error` como en el formato plano existente. Se aplica la redacción previa
a la truncación, con límites de 100 caracteres para el código y 512 para el
mensaje. Otros campos y tipos inesperados no se convierten a texto ni se publican.
Un cuerpo vacío, HTML, texto o JSON malformado produce el diagnóstico genérico
`Hermes HTTP <status>`; su contenido no pasa al mensaje ni a la serialización del
error ni a los detalles persistidos. Las políticas de retry y autenticación
permanecen iguales.

Pruebas en `tests/test_hermes_adapter.py`: status real en atributo/serialización,
descarte de cuerpos arbitrarios en POST y SSE, campos inesperados, redacción,
longitud máxima y ausencia de status para errores no HTTP. Las pruebas del
dispatcher verifican además la persistencia de 403 y 503 con JSON propio, texto,
HTML, JSON malformado y campos JSON inesperados, sin publicar los datos privados.

## 2. Run ya activo con 24 intentos

`test_running_run_with_24_attempts_fails_safely_and_preserves_http_diagnostic`
parte de un run persistido en `running`, `worker_run_id=existing-run`,
`dispatch_attempts=24` y `Settings.usage_max_retries=1`. El worker informa que
continúa activo y el SSE devuelve un error HTTP controlado.

La adquisición normal suma un intento: el contador final es 25, no se reinicia
ni se reduce. El resultado local es `failed`, conserva el ID remoto, libera
todos los campos del lease y registra un único asiento de uso con resultado
`failed`. Una segunda pasada no vuelve a reclamarlo. El transporte de prueba
rechaza cualquier POST y verifica las únicas cuatro lecturas permitidas:
health, capabilities, estado del run y SSE. No se llama a `start_run` ni a stop.

### Limitación para el despliegue

El fallo local por agotamiento no demuestra que el worker haya terminado: en
este escenario sigue activo y no se le envía stop. Antes de desplegar sobre un
runtime con ese trabajo activo, esperar su final natural y conciliar el estado
terminal remoto, sus resultados y su uso con el registro local. No aumentar
`usage_max_retries`, reiniciar el contador ni forzar otro dispatch para sortear
el agotamiento. Esta entrega no ejecuta esa conciliación ni opera el runtime.

## Validación inicial de `ab5c22c`

```powershell
.venv/Scripts/python.exe -m pytest tests/test_hermes_adapter.py tests/test_run_dispatcher.py -k 'not retry_after' --no-cov -q --basetemp=.pytest_cache/infra-status-focused
.venv/Scripts/python.exe -m ruff check .
.venv/Scripts/python.exe -m ruff format --check .
.venv/Scripts/python.exe -m mypy --platform linux src
git diff --check
```

Resultado: 86 pruebas pasan, 36 de `Retry-After` excluidas; Ruff, formato y mypy
pasan. No se repite la suite Windows. Se utilizan el entorno local del checkout,
transportes de prueba y SQLite temporal, sin cambios científicos, runtime, PR
ni despliegue.

## Reparación H1/P1: lectura fallida de un error HTTP

La revisión íntegra conservada en `.pytest_cache/infra-review-final.md` requiere
cambios sobre `ab5c22c`. Su resultado y los repros originales permanecen intactos.
Se ha releído el alcance íntegro, con SHA256 de sus bytes
`A7C47A7D09DEB7B349512D31B40190FBBD601A3E1650465F6DBCDDBD2436AFB9`.

La lectura de una respuesta HTTP no exitosa captura ahora `httpx.TransportError`
y `httpx.DecodingError` dentro de `_raise_for_response`. Al fallar, descarta
cualquier cuerpo parcial y el texto de la excepción; conserva el status ya
recibido y usa `Hermes HTTP <status>`. La clasificación por status permanece:
401/403 no reintentables y con acción humana requerida; 503 reintentable.
El error HTTP no se convierte en `worker_disconnected` ni pierde el status.
El camino 2xx y su reconexión siguen intactos; no se amplía la captura general
del dispatcher.

Regresiones versionadas en `tests/test_hermes_http_failures.py`, derivadas de los
probes fallidos independientes: timeout, fallo de protocolo y gzip inválido;
clasificación 401/403/503, diagnóstico seguro y cierre del stream. Para el run
ya activo con 24 claims, prueban el claim productivo auditado 25, `failed`, ID
intacto, lease liberado, handoff conservado y un asiento fallido, sin POST. El
presupuesto de Task conserva `max_retries=0` como capa separada de Settings=1.

## Reparación H2/P2: diagnóstico durable antes del reintento

`RunDispatcher._handle_error` persiste la misma selección de campos seguros
(`code`, `message`, `retryable` y `http_status` cuando existe), junto al handoff,
antes de elegir entre retry y cierre terminal. La programación, estado y
contadores mantienen su política anterior. No se añade persistencia de
`human_action_required` ni `retry_after`, ni se modifica el retraso configurado.

La regresión de recuperación inspecciona una sesión nueva tras el primer error
503: run `running`, intento 1, lease liberado, diagnóstico y handoff presentes,
sin asiento terminal. Después reabre el SSE del mismo ID remoto y completa en
el intento 2, con un único evento terminal y un único asiento `completed`, sin
ningún POST. El presupuesto de Task tampoco se modifica.

Al completar, la finalización existente sustituye el diagnóstico transitorio
por el error terminal informado por el worker. En el éxito probado ese error
está vacío: desaparecen `code`, `message`, `retryable` y `http_status`; se conserva
`agent_handoff`. `error_details` representa el estado actual, no un historial
inmutable de rechazos. No se cambia ese contrato de finalización.

### Validación focal de H1/H2

- 13 regresiones versionadas pasan en `tests/test_hermes_http_failures.py`.
- Los cuatro repros antes fallidos de `.pytest_cache/test_infra_review_final.py`
  pasan, sin editar el archivo. Esta ejecución del autor no sustituye la nueva
  revisión independiente del supervisor.
- Cinco comprobaciones de compatibilidad pasan: éxito SSE sin consumo de cola,
  reconexión con cursor, reintento sin segundo POST, agotamiento normal con JSON
  503 y worker no disponible antes del dispatch.
- Ruff, formato y mypy con plataforma Linux pasan. No se repiten los 36 casos
  verdes de Retry-After ni la suite completa Windows.

Comando de las regresiones nuevas:

```powershell
.venv/Scripts/python.exe -m pytest tests/test_hermes_http_failures.py --no-cov -q --basetemp=.pytest_cache/infra-h1-h2-focused
```

El supervisor debe actualizar PR/CI y revisar independientemente el nuevo HEAD.
Los gates Linux y cobertura mínima 95 % permanecen sin cambios. La restricción
de despliegue anterior sigue vigente: esperar fin natural y conciliar el worker
activo antes de aplicar. Esta reparación no opera runtime ni workers.
