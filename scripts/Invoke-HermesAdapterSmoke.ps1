[CmdletBinding()]
param(
    [string]$Image = "nousresearch/hermes-agent@sha256:4f0cf12465c50a12e6a747e319794640ab87ec1ce260b1ce9070c3c8950506c8",
    [string]$AuthStore = (Join-Path $env:USERPROFILE ".hermes\auth.json"),
    [string]$OrchestratorContainer = "hermes-orchestrator-f5-api-1",
    [int]$HostPort = 18642
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $AuthStore -PathType Leaf)) {
    throw "No existe el almacén OAuth Hermes: $AuthStore"
}

$containerName = "hermes-f8-adapter-$PID"
$apiKey = [Convert]::ToHexString(
    [Security.Cryptography.RandomNumberGenerator]::GetBytes(32)
).ToLowerInvariant()
$started = $false

$runner = @'
import json
import os
import uuid

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import create_database_engine, create_session_factory
from hermes_orchestrator.hermes_adapter import HermesRunsAdapter
from hermes_orchestrator.hermes_execution import execute_run_via_hermes, list_run_events

settings = Settings()
engine = create_database_engine(settings.database_url)
factory = create_session_factory(engine)
with factory() as session, HermesRunsAdapter(
    os.environ["F8_WORKER_URL"],
    os.environ["F8_WORKER_TOKEN"],
    timeout_seconds=180,
    max_reconnects=2,
) as adapter:
    run = execute_run_via_hermes(
        session,
        run_id=uuid.UUID(os.environ["F8_RUN_ID"]),
        adapter=adapter,
        input_text="Responde exactamente F8_REAL_OK, sin herramientas.",
    )
    events = list_run_events(session, run.id)
    print(json.dumps({
        "run_id": str(run.id),
        "worker_run_id_present": bool(run.worker_run_id),
        "status": run.status,
        "effective_profile_id": run.effective_profile_id,
        "summary": run.summary,
        "usage": run.usage_snapshot,
        "event_types": [event.event_type for event in events],
        "terminal_events": sum(1 for event in events if event.terminal),
    }))
engine.dispose()
'@

try {
    docker run -d --rm `
        --name $containerName `
        --tmpfs "/opt/data:rw,noexec,nosuid,size=256m" `
        --mount "type=bind,source=$AuthStore,target=/bootstrap/hermes-auth.json,readonly" `
        -e "HERMES_HOME=/opt/data" `
        -e "API_SERVER_ENABLED=true" `
        -e "API_SERVER_KEY=$apiKey" `
        -e "API_SERVER_HOST=0.0.0.0" `
        -e "API_SERVER_PORT=8642" `
        -e "TZ=Europe/Madrid" `
        -p "127.0.0.1:${HostPort}:8642" `
        $Image gateway run | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "No se pudo iniciar el worker F8" }
    $started = $true

    $ready = $false
    foreach ($attempt in 1..60) {
        try {
            Invoke-RestMethod "http://127.0.0.1:$HostPort/health" -TimeoutSec 1 | Out-Null
            $ready = $true
            break
        }
        catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { throw "El worker F8 no quedó saludable" }

    docker exec $containerName python -c "import os,pwd,shutil; shutil.copyfile('/bootstrap/hermes-auth.json','/opt/data/auth.json'); p=pwd.getpwnam('hermes'); os.chown('/opt/data/auth.json',p.pw_uid,p.pw_gid); os.chmod('/opt/data/auth.json',0o600)" | Out-Null
    docker exec $containerName hermes config set model.default gpt-5.3-codex-spark | Out-Null
    docker exec $containerName hermes config set model.provider openai-codex | Out-Null
    docker exec $containerName hermes config set model.base_url https://chatgpt.com/backend-api/codex | Out-Null
    docker exec $containerName hermes config set agent.reasoning_effort low | Out-Null

    $taskHeaders = @{
        "X-Actor-Id" = "agent:leader"
        "Idempotency-Key" = "f8-real-task-$PID"
    }
    $taskBody = @{
        objective = "Validar el adaptador Runs contra Hermes real"
        acceptance_criteria = @("Hermes responde F8_REAL_OK", "El run local queda terminal")
        assignee_actor_id = "agent:developer"
        independent_review = $false
        budget = @{ purpose = "f8-real-smoke" }
    } | ConvertTo-Json -Depth 4
    $task = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8080/v1/tasks" -Headers $taskHeaders -ContentType "application/json" -Body $taskBody

    $dispatchHeaders = @{
        "X-Actor-Id" = "agent:leader"
        "Idempotency-Key" = "f8-real-dispatch-$PID"
    }
    $dispatchBody = @{
        worker_actor_id = "agent:developer"
        requested_profile_id = "spark-low"
        timeout_seconds = 300
        requires_approval = $false
    } | ConvertTo-Json
    $dispatch = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8080/v1/tasks/$($task.id)/dispatch" -Headers $dispatchHeaders -ContentType "application/json" -Body $dispatchBody

    $result = $runner | docker exec -i `
        -e "F8_RUN_ID=$($dispatch.run.id)" `
        -e "F8_WORKER_URL=http://host.docker.internal:$HostPort" `
        -e "F8_WORKER_TOKEN=$apiKey" `
        $OrchestratorContainer /app/.venv/bin/python -
    if ($LASTEXITCODE -ne 0) { throw "La ejecución real del adaptador F8 falló" }
    $result
}
finally {
    if ($started) { docker rm -f $containerName 2>$null | Out-Null }
    $apiKey = $null
}
