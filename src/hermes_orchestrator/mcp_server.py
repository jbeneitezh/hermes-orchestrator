from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Literal, cast

from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import Agent, CommunicationEdge, Run, Task
from hermes_orchestrator.policy import Permission, actor_is_allowed, communication_is_allowed
from hermes_orchestrator.repositories import AgentRepository
from hermes_orchestrator.schemas import AgentRequestCreate, TaskCreate
from hermes_orchestrator.services import IdempotencyConflictError, request_agent
from hermes_orchestrator.task_services import (
    LifecycleError,
    add_comment,
    create_task,
    dispatch_task,
    get_task,
    handoff_task,
)
from hermes_orchestrator.usage_services import summarize_usage

TOOL_PERMISSIONS: dict[str, Permission] = {
    "agents_list": Permission.AGENTS_READ,
    "agent_request": Permission.AGENTS_REQUEST,
    "task_create": Permission.TASKS_CREATE,
    "task_dispatch": Permission.TASKS_DISPATCH,
    "task_get": Permission.TASKS_READ,
    "task_comment": Permission.TASKS_COMMENT,
    "task_block": Permission.TASKS_COMMENT,
    "task_complete": Permission.TASKS_COMMENT,
    "usage_summary": Permission.RUNS_READ,
}
ROLE_TOOL_ALLOWLIST: dict[str, set[str]] = {
    "leader": set(TOOL_PERMISSIONS),
    "operator": {"agents_list", "task_get", "usage_summary"},
    "researcher": {"task_create", "task_get", "task_comment", "task_block", "task_complete"},
    "developer": {"task_create", "task_get", "task_comment", "task_block", "task_complete"},
    "validator": {"task_get", "task_comment", "task_block", "task_complete"},
    "data_steward": {"task_get"},
}


class McpAgentRequest(AgentRequestCreate):
    idempotency_key: str = Field(min_length=8, max_length=160)


class McpTaskCreate(TaskCreate):
    idempotency_key: str = Field(min_length=8, max_length=160)


class McpTaskDispatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: uuid.UUID
    worker_agent_id: uuid.UUID
    requested_profile_id: str = Field(min_length=1, max_length=80)
    idempotency_key: str = Field(min_length=8, max_length=160)
    timeout_seconds: int = Field(default=900, ge=1, le=86400)
    requires_approval: bool = False
    approval_action: str = "dispatch"
    approval_ttl_seconds: int = Field(default=900, ge=1, le=86400)


class McpTaskGet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: uuid.UUID


class McpTaskComment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: uuid.UUID
    idempotency_key: str = Field(min_length=8, max_length=160)
    body: str = Field(min_length=1, max_length=10000)


class McpTaskBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: uuid.UUID
    idempotency_key: str = Field(min_length=8, max_length=160)
    block_type: Literal["dependency", "clarification", "access", "environment", "external"]
    summary: str = Field(min_length=1, max_length=2000)
    needed_action: str = Field(min_length=1, max_length=2000)
    references: list[str] = Field(default_factory=list, max_length=20)


class McpTaskComplete(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: uuid.UUID
    idempotency_key: str = Field(min_length=8, max_length=160)
    summary: str = Field(min_length=1, max_length=4000)
    outputs: list[str] = Field(min_length=1, max_length=20)
    evidence: list[str] = Field(default_factory=list, max_length=20)


class McpUsageSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_id: uuid.UUID | None = None


@dataclass(frozen=True)
class McpIdentity:
    agent: Agent
    actor_id: str
    role: str


class McpDomainError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _tool(name: str, description: str, schema: type[BaseModel] | None = None) -> types.Tool:
    input_schema = (
        schema.model_json_schema() if schema else {"type": "object", "additionalProperties": False}
    )
    return types.Tool(name=name, description=description, inputSchema=input_schema)


TOOLS: dict[str, types.Tool] = {
    "agents_list": _tool("agents_list", "Lista agentes visibles para la identidad actual."),
    "agent_request": _tool(
        "agent_request",
        "Solicita capacidad de agente de forma idempotente; no la aplica.",
        McpAgentRequest,
    ),
    "task_create": _tool("task_create", "Crea una tarea durable e idempotente.", McpTaskCreate),
    "task_dispatch": _tool(
        "task_dispatch",
        "Despacha una tarea por un edge de comunicación permitido.",
        McpTaskDispatch,
    ),
    "task_get": _tool("task_get", "Consulta una tarea visible y sus runs.", McpTaskGet),
    "task_comment": _tool(
        "task_comment", "Añade un comentario durable a una tarea visible.", McpTaskComment
    ),
    "task_block": _tool(
        "task_block", "Bloquea el trabajo activo con una necesidad tipada.", McpTaskBlock
    ),
    "task_complete": _tool(
        "task_complete",
        "Entrega un handoff terminal estructurado sin autoaprobarlo.",
        McpTaskComplete,
    ),
    "usage_summary": _tool(
        "usage_summary", "Agrega tokens por operación, agente y perfil.", McpUsageSummary
    ),
}


def _identity_from_request(
    session: Session, request: Request | None, settings: Settings
) -> McpIdentity:
    raw_id = request.headers.get("X-Agent-Id") if request is not None else None
    try:
        agent_id = uuid.UUID(raw_id or "")
    except ValueError as exc:
        raise McpDomainError("authentication_required", "X-Agent-Id no es válido") from exc
    agent = AgentRepository(session).get(agent_id)
    if agent is None or agent.desired_state in {"disabled", "decommissioned"}:
        raise McpDomainError("authentication_required", "Identidad de agente no activa")
    actor_id = f"agent:{agent.slug}"
    configured_role = settings.actor_roles.get(actor_id)
    if configured_role is None or configured_role != agent.role:
        raise McpDomainError("authentication_required", "Identidad de agente no confiable")
    return McpIdentity(agent=agent, actor_id=actor_id, role=configured_role)


def _allowed_tools(identity: McpIdentity, settings: Settings) -> list[types.Tool]:
    role_tools = ROLE_TOOL_ALLOWLIST.get(identity.role, set())
    return [
        tool
        for name, tool in TOOLS.items()
        if name in role_tools
        and actor_is_allowed(identity.actor_id, TOOL_PERMISSIONS[name], settings.actor_roles)
    ]


def _require_tool(identity: McpIdentity, tool_name: str, settings: Settings) -> None:
    if tool_name not in {tool.name for tool in _allowed_tools(identity, settings)}:
        raise McpDomainError("permission_denied", "Herramienta no autorizada")


def _active_outgoing_target_ids(session: Session, identity: McpIdentity) -> set[uuid.UUID]:
    edges = list(
        session.scalars(
            select(CommunicationEdge).where(CommunicationEdge.source_agent_id == identity.agent.id)
        )
    )
    visible: set[uuid.UUID] = {identity.agent.id}
    for edge in edges:
        if communication_is_allowed(
            session,
            edge.source_agent_id,
            edge.target_agent_id,
            "visibility",
            "read",
        ):
            visible.add(edge.target_agent_id)
    return visible


def _agent_summary(agent: Agent) -> dict[str, Any]:
    return {
        "id": str(agent.id),
        "slug": agent.slug,
        "role": agent.role,
        "description": agent.description,
        "desired_state": agent.desired_state,
        "capabilities": agent.capabilities,
    }


def _task_is_visible(task: Task, identity: McpIdentity) -> bool:
    if identity.role in {"leader", "operator"}:
        return True
    participants = {
        task.requester_actor_id,
        task.assignee_actor_id,
        task.reviewer_actor_id,
        *(run.worker_actor_id for run in task.runs),
    }
    return identity.actor_id in participants


def _run_summary(run: Run) -> dict[str, Any]:
    return {
        "id": str(run.id),
        "attempt_number": run.attempt_number,
        "worker_actor_id": run.worker_actor_id,
        "requested_profile_id": run.requested_profile_id,
        "effective_profile_id": run.effective_profile_id,
        "status": run.status,
        "summary": run.summary,
        "error_code": run.error_code,
        "requested_runtime": run.requested_runtime,
        "observed_runtime": run.observed_runtime,
        "runtime_fallback": run.runtime_fallback,
        "usage": run.usage_snapshot,
    }


def _task_summary(task: Task) -> dict[str, Any]:
    return {
        "id": str(task.id),
        "operation_id": str(task.operation_id),
        "status": task.status,
        "objective": task.objective,
        "acceptance_criteria": task.acceptance_criteria,
        "assignee_actor_id": task.assignee_actor_id,
        "reviewer_actor_id": task.reviewer_actor_id,
        "priority": task.priority,
        "budget": task.budget,
        "runs": [_run_summary(run) for run in task.runs],
    }


def _parse(model: type[BaseModel], arguments: dict[str, Any]) -> BaseModel:
    try:
        return model.model_validate(arguments)
    except ValidationError as exc:
        raise McpDomainError("invalid_input", "Argumentos no válidos") from exc


def _execute_tool(
    session: Session,
    identity: McpIdentity,
    tool_name: str,
    arguments: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    _require_tool(identity, tool_name, settings)
    if tool_name == "agents_list":
        visible_ids = _active_outgoing_target_ids(session, identity)
        agents = [
            _agent_summary(agent)
            for agent in AgentRepository(session).list()
            if agent.id in visible_ids and agent.desired_state not in {"disabled", "decommissioned"}
        ]
        return {"agents": agents}

    if tool_name == "agent_request":
        parsed = _parse(McpAgentRequest, arguments)
        payload = parsed.model_dump(mode="json")
        idempotency_key = str(payload.pop("idempotency_key"))
        try:
            request_result = request_agent(
                session,
                actor_id=identity.actor_id,
                idempotency_key=idempotency_key,
                payload=payload,
            )
        except IdempotencyConflictError as exc:
            raise McpDomainError(
                "idempotency_conflict", "La clave ya se usó con otra solicitud"
            ) from exc
        return {
            "request": {
                "id": str(request_result.request.id),
                "status": request_result.request.status,
                "created_at": request_result.request.created_at.isoformat(),
            },
            "replayed": request_result.replayed,
        }

    if tool_name == "task_create":
        parsed = _parse(McpTaskCreate, arguments)
        payload = parsed.model_dump(mode="python")
        idempotency_key = str(payload.pop("idempotency_key"))
        task_result = create_task(
            session,
            actor_id=identity.actor_id,
            idempotency_key=idempotency_key,
            payload=payload,
        )
        return {
            "task": _task_summary(cast(Task, task_result.value)),
            "replayed": task_result.replayed,
        }

    if tool_name == "task_dispatch":
        parsed = McpTaskDispatch.model_validate(arguments)
        target = AgentRepository(session).get(parsed.worker_agent_id)
        if target is None:
            raise McpDomainError("not_found", "Agente ejecutor no encontrado")
        if not communication_is_allowed(session, identity.agent.id, target.id, "task", "dispatch"):
            raise McpDomainError("communication_denied", "Edge de dispatch no permitido")
        dispatch_result = dispatch_task(
            session,
            task_id=parsed.task_id,
            actor_id=identity.actor_id,
            idempotency_key=parsed.idempotency_key,
            payload={
                "worker_actor_id": f"agent:{target.slug}",
                "requested_profile_id": parsed.requested_profile_id,
                "timeout_seconds": parsed.timeout_seconds,
                "requires_approval": parsed.requires_approval,
                "approval_action": parsed.approval_action,
                "approval_ttl_seconds": parsed.approval_ttl_seconds,
            },
            settings=settings,
        )
        return {
            "run": _run_summary(cast(Run, dispatch_result.value)),
            "replayed": dispatch_result.replayed,
        }

    if tool_name == "task_get":
        parsed = McpTaskGet.model_validate(arguments)
        task = get_task(session, parsed.task_id)
        if not _task_is_visible(task, identity):
            raise McpDomainError("not_found", "Tarea no encontrada")
        return {"task": _task_summary(task)}

    if tool_name == "task_comment":
        parsed = McpTaskComment.model_validate(arguments)
        task = get_task(session, parsed.task_id)
        if not _task_is_visible(task, identity):
            raise McpDomainError("not_found", "Tarea no encontrada")
        comment_result = add_comment(
            session,
            task_id=task.id,
            actor_id=identity.actor_id,
            idempotency_key=parsed.idempotency_key,
            body=parsed.body,
        )
        comment = cast(Any, comment_result.value)
        return {
            "comment": {
                "id": str(comment.id),
                "actor_id": comment.actor_id,
                "body": comment.body,
            },
            "replayed": comment_result.replayed,
        }

    if tool_name in {"task_block", "task_complete"}:
        schema = McpTaskBlock if tool_name == "task_block" else McpTaskComplete
        parsed = _parse(schema, arguments)
        payload = parsed.model_dump(mode="json")
        task_id = uuid.UUID(str(payload.pop("task_id")))
        idempotency_key = str(payload.pop("idempotency_key"))
        task = get_task(session, task_id)
        if not _task_is_visible(task, identity):
            raise McpDomainError("not_found", "Tarea no encontrada")
        handoff = {
            "type": "task_handoff",
            "outcome": "blocked" if tool_name == "task_block" else "completed",
            **payload,
        }
        handoff_result = handoff_task(
            session,
            task_id=task.id,
            actor_id=identity.actor_id,
            idempotency_key=idempotency_key,
            handoff=handoff,
        )
        approval = handoff_result.approval
        return {
            "task": _task_summary(handoff_result.task),
            "handoff": handoff_result.handoff,
            "approval": (
                {
                    "id": str(approval.id),
                    "action": approval.action,
                    "status": approval.status,
                }
                if approval is not None
                else None
            ),
            "replayed": handoff_result.replayed,
        }

    if tool_name == "usage_summary":
        parsed = McpUsageSummary.model_validate(arguments)
        return summarize_usage(
            session,
            group_by="operation",
            operation_id=parsed.operation_id,
        )

    raise McpDomainError("tool_not_found", "Herramienta desconocida")


def _error_result(error: McpDomainError) -> types.CallToolResult:
    payload = {"error": {"code": error.code, "detail": error.detail}}
    return types.CallToolResult(
        content=[types.TextContent(type="text", text="Operación MCP rechazada")],
        structuredContent=payload,
        isError=True,
    )


def build_mcp_server(
    settings: Settings, factory: sessionmaker[Session]
) -> tuple[Server[Any, Request], StreamableHTTPSessionManager]:
    server: Server[Any, Request] = Server(
        "hermes-orchestrator",
        version="0.1.0",
        instructions="Herramientas gobernadas para colaboración entre agentes.",
    )

    @server.list_tools()  # type: ignore[untyped-decorator,no-untyped-call]
    async def list_tools() -> list[types.Tool]:
        request = server.request_context.request
        with factory() as session:
            try:
                identity = _identity_from_request(session, request, settings)
                return _allowed_tools(identity, settings)
            except McpDomainError:
                return []

    @server.call_tool()  # type: ignore[untyped-decorator]
    async def call_tool(
        name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | types.CallToolResult:
        request = server.request_context.request
        with factory() as session:
            try:
                identity = _identity_from_request(session, request, settings)
                return _execute_tool(session, identity, name, arguments, settings)
            except LifecycleError as error:
                return _error_result(McpDomainError(error.code, error.detail))
            except (McpDomainError, ValidationError) as error:
                if isinstance(error, ValidationError):
                    error = McpDomainError("invalid_input", "Argumentos no válidos")
                return _error_result(error)
            except Exception:
                return _error_result(McpDomainError("internal_error", "Error interno redactado"))

    manager = StreamableHTTPSessionManager(
        app=server,
        json_response=True,
        stateless=True,
    )
    return server, manager
