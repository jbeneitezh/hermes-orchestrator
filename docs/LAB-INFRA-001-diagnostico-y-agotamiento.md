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

## Validación de esta entrega

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
