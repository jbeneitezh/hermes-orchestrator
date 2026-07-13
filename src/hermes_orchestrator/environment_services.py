from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn

from sqlalchemy import select
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import (
    AuditEvent,
    EnvironmentAction,
    EnvironmentDeployment,
    Task,
)
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.repositories import AuditRepository
from hermes_orchestrator.schemas import (
    EnvironmentDeployCreate,
    EnvironmentDeploymentResponse,
    EnvironmentPromotionCreate,
    EnvironmentRollbackCreate,
)

ENVIRONMENT_DEFINITIONS = [
    {"name": "local", "ref_kind": "branch", "immutable": False, "scope": "task"},
    {"name": "dev", "ref_kind": "branch", "immutable": False, "scope": "shared"},
    {"name": "pre", "ref_kind": "sha", "immutable": True, "scope": "shared"},
    {"name": "prod-sim", "ref_kind": "tag", "immutable": True, "scope": "shared"},
    {"name": "live", "enabled": False, "reason": "fuera_de_alcance_v1"},
]
SAFE_TAG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,299}$")


class EnvironmentError(Exception):
    def __init__(self, code: str, detail: str, status_code: int) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


@dataclass(frozen=True)
class EnvironmentResult:
    deployment: EnvironmentDeployment
    replayed: bool = False


def _hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _audit(
    session: Session,
    *,
    actor_id: str,
    event_type: str,
    aggregate_id: str,
    payload: dict[str, Any],
) -> None:
    AuditRepository(session).append(
        AuditEvent(
            actor_id=actor_id,
            event_type=event_type,
            aggregate_type="environment",
            aggregate_id=aggregate_id,
            payload=payload,
        )
    )


def _begin_action(
    session: Session,
    *,
    action: str,
    environment: str,
    actor_id: str,
    idempotency_key: str,
    payload: dict[str, Any],
) -> tuple[EnvironmentAction, EnvironmentResult | None]:
    request_hash = _hash(payload)
    existing = session.scalar(
        select(EnvironmentAction).where(EnvironmentAction.idempotency_key == idempotency_key)
    )
    if existing is not None:
        if (
            existing.request_hash != request_hash
            or existing.requested_by_actor_id != actor_id
            or existing.action != action
        ):
            raise EnvironmentError(
                "idempotency_conflict", "La clave ya se usó con otra solicitud", 409
            )
        if existing.status == "rejected" or existing.deployment_id is None:
            raise EnvironmentError(
                existing.error_code or "environment_action_rejected",
                "La solicitud ya fue rechazada",
                409,
            )
        deployment = session.get(EnvironmentDeployment, existing.deployment_id)
        if deployment is None:
            raise EnvironmentError("deployment_not_found", "Despliegue no encontrado", 404)
        return existing, EnvironmentResult(deployment, True)
    record = EnvironmentAction(
        action=action,
        environment=environment,
        requested_by_actor_id=actor_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        request_payload=payload,
        status="requested",
    )
    session.add(record)
    session.flush()
    return record, None


def _reject(
    session: Session,
    record: EnvironmentAction,
    *,
    actor_id: str,
    code: str,
    detail: str,
    status_code: int,
) -> NoReturn:
    record.status = "rejected"
    record.error_code = code
    _audit(
        session,
        actor_id=actor_id,
        event_type="environment.action.rejected",
        aggregate_id=str(record.id),
        payload={"action": record.action, "environment": record.environment, "code": code},
    )
    session.commit()
    raise EnvironmentError(code, detail, status_code)


def _current(
    session: Session, environment: str, instance_key: str = "shared"
) -> EnvironmentDeployment | None:
    return session.scalar(
        select(EnvironmentDeployment)
        .where(
            EnvironmentDeployment.environment == environment,
            EnvironmentDeployment.instance_key == instance_key,
            EnvironmentDeployment.status == "active",
        )
        .order_by(EnvironmentDeployment.created_at.desc())
        .limit(1)
    )


def _approve(
    record: EnvironmentAction,
    *,
    requester: str,
    approval_actor: str | None,
    approval_reason: str | None,
    settings: Settings,
    operator_only: bool,
) -> tuple[str, str] | None:
    if approval_actor is None or approval_reason is None:
        return None
    if approval_actor == requester:
        return None
    if not actor_is_allowed(approval_actor, Permission.APPROVALS_DECIDE, settings.actor_roles):
        return None
    if operator_only and settings.actor_roles.get(approval_actor) not in {"owner", "operator"}:
        return None
    record.approval_actor_id = approval_actor
    record.approval_reason = approval_reason
    return approval_actor, approval_reason


def list_deployments(session: Session) -> list[EnvironmentDeployment]:
    return list(
        session.scalars(
            select(EnvironmentDeployment).order_by(EnvironmentDeployment.created_at.desc())
        )
    )


def deploy_environment(
    session: Session,
    *,
    body: EnvironmentDeployCreate,
    actor_id: str,
    idempotency_key: str,
    settings: Settings,
) -> EnvironmentResult:
    payload = body.model_dump(mode="json")
    action, replay = _begin_action(
        session,
        action="deploy",
        environment=body.environment,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        payload=payload,
    )
    if replay is not None:
        return replay
    if body.repository not in settings.environment_allowed_repositories:
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="repository_not_allowed",
            detail="Repositorio no permitido",
            status_code=403,
        )
    if settings.actor_roles.get(actor_id) == "developer" and body.environment != "local":
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="developer_scope_denied",
            detail="Developer solo puede desplegar entornos locales",
            status_code=403,
        )

    now = datetime.now(UTC)
    task_id: uuid.UUID | None = None
    port: int | None = None
    expires_at: datetime | None = None
    instance_key = "shared"
    if body.environment == "local":
        if body.task_id is None or session.get(Task, body.task_id) is None:
            _reject(
                session,
                action,
                actor_id=actor_id,
                code="local_task_required",
                detail="Local exige una tarea existente",
                status_code=422,
            )
        task_id = body.task_id
        instance_key = str(task_id)
        ttl = body.ttl_seconds or settings.environment_local_default_ttl_seconds
        if ttl > settings.environment_local_max_ttl_seconds:
            _reject(
                session,
                action,
                actor_id=actor_id,
                code="local_ttl_exceeded",
                detail="TTL local superior al máximo permitido",
                status_code=422,
            )
        active_locals = list(
            session.scalars(
                select(EnvironmentDeployment).where(
                    EnvironmentDeployment.environment == "local",
                    EnvironmentDeployment.status == "active",
                )
            )
        )
        for deployed in active_locals:
            if deployed.expires_at is not None and _aware(deployed.expires_at) <= now:
                deployed.status = "expired"
        if _current(session, "local", instance_key) is not None:
            _reject(
                session,
                action,
                actor_id=actor_id,
                code="local_already_active",
                detail="La tarea ya tiene un entorno local activo",
                status_code=409,
            )
        used_ports = {
            deployed.allocated_port
            for deployed in active_locals
            if deployed.status == "active" and deployed.allocated_port is not None
        }
        port = next(
            (
                candidate
                for candidate in range(
                    settings.environment_local_port_start,
                    settings.environment_local_port_end + 1,
                )
                if candidate not in used_ports
            ),
            None,
        )
        if port is None:
            _reject(
                session,
                action,
                actor_id=actor_id,
                code="local_port_pool_exhausted",
                detail="No quedan puertos locales gobernados",
                status_code=409,
            )
        expires_at = now + timedelta(seconds=ttl)
    elif body.task_id is not None or body.ttl_seconds is not None:
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="dev_local_fields_denied",
            detail="Dev no admite task_id ni TTL",
            status_code=422,
        )

    previous = _current(session, body.environment, instance_key)
    if previous is not None:
        previous.status = "superseded"
    deployment = EnvironmentDeployment(
        environment=body.environment,
        instance_key=instance_key,
        repository=body.repository,
        ref_kind="branch",
        ref_value=body.branch,
        resolved_sha=body.resolved_sha,
        task_id=task_id,
        allocated_port=port,
        expires_at=expires_at,
        requested_by_actor_id=actor_id,
    )
    session.add(deployment)
    session.flush()
    action.status = "applied"
    action.deployment_id = deployment.id
    action.previous_deployment_id = previous.id if previous is not None else None
    _audit(
        session,
        actor_id=actor_id,
        event_type="environment.deployed",
        aggregate_id=str(deployment.id),
        payload={
            "environment": body.environment,
            "ref_kind": "branch",
            "ref_value": body.branch,
            "resolved_sha": body.resolved_sha,
            "allocated_port": port,
        },
    )
    session.commit()
    session.refresh(deployment)
    return EnvironmentResult(deployment)


def expire_local(
    session: Session,
    *,
    deployment_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
) -> EnvironmentResult:
    payload = {"deployment_id": str(deployment_id)}
    action, replay = _begin_action(
        session,
        action="expire",
        environment="local",
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        payload=payload,
    )
    if replay is not None:
        return replay
    deployment = session.get(EnvironmentDeployment, deployment_id)
    if deployment is None or deployment.environment != "local":
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="local_deployment_not_found",
            detail="Entorno local no encontrado",
            status_code=404,
        )
    deployment.status = "expired"
    action.status = "applied"
    action.deployment_id = deployment.id
    _audit(
        session,
        actor_id=actor_id,
        event_type="environment.local.expired",
        aggregate_id=str(deployment.id),
        payload={"allocated_port": deployment.allocated_port},
    )
    session.commit()
    session.refresh(deployment)
    return EnvironmentResult(deployment)


def promote_environment(
    session: Session,
    *,
    body: EnvironmentPromotionCreate,
    actor_id: str,
    idempotency_key: str,
    settings: Settings,
) -> EnvironmentResult:
    payload = body.model_dump(mode="json")
    action, replay = _begin_action(
        session,
        action="promote",
        environment=body.target_environment,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        payload=payload,
    )
    if replay is not None:
        return replay
    if body.target_environment == "live":
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="live_environment_denied",
            detail="Live está fuera de alcance de v1",
            status_code=403,
        )
    source = session.get(EnvironmentDeployment, body.source_deployment_id)
    expected_source = "dev" if body.target_environment == "pre" else "pre"
    if source is None or source.status != "active" or source.environment != expected_source:
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="invalid_promotion_source",
            detail="La fuente no es el despliegue activo esperado",
            status_code=409,
        )
    if (
        body.repository not in settings.environment_allowed_repositories
        or body.repository != source.repository
        or body.resolved_sha != source.resolved_sha
    ):
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="immutable_ref_mismatch",
            detail="Repositorio o SHA no coincide con la fuente",
            status_code=409,
        )
    approval = body.approval
    approved = _approve(
        action,
        requester=actor_id,
        approval_actor=approval.actor_id if approval else None,
        approval_reason=approval.reason if approval else None,
        settings=settings,
        operator_only=body.target_environment == "prod-sim",
    )
    if approved is None:
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="independent_approval_required",
            detail="La promoción exige aprobación independiente autorizada",
            status_code=403,
        )
    if body.target_environment == "pre":
        if body.tag is not None:
            _reject(
                session,
                action,
                actor_id=actor_id,
                code="pre_tag_denied",
                detail="Pre se fija directamente por SHA",
                status_code=422,
            )
        ref_kind, ref_value = "sha", body.resolved_sha
    else:
        if body.tag is None or SAFE_TAG.fullmatch(body.tag) is None:
            _reject(
                session,
                action,
                actor_id=actor_id,
                code="immutable_tag_required",
                detail="Prod-sim exige un tag válido",
                status_code=422,
            )
        ref_kind, ref_value = "tag", body.tag
    previous = _current(session, body.target_environment)
    if previous is not None:
        previous.status = "superseded"
    deployment = EnvironmentDeployment(
        environment=body.target_environment,
        instance_key="shared",
        repository=body.repository,
        ref_kind=ref_kind,
        ref_value=ref_value,
        resolved_sha=body.resolved_sha,
        source_deployment_id=source.id,
        requested_by_actor_id=actor_id,
        approval_actor_id=approved[0],
    )
    session.add(deployment)
    session.flush()
    action.status = "applied"
    action.deployment_id = deployment.id
    action.previous_deployment_id = previous.id if previous is not None else None
    _audit(
        session,
        actor_id=actor_id,
        event_type="environment.promoted",
        aggregate_id=str(deployment.id),
        payload={
            "from": source.environment,
            "to": body.target_environment,
            "ref_kind": ref_kind,
            "ref_value": ref_value,
            "resolved_sha": body.resolved_sha,
            "approved_by": approved[0],
        },
    )
    session.commit()
    session.refresh(deployment)
    return EnvironmentResult(deployment)


def rollback_environment(
    session: Session,
    *,
    environment: str,
    body: EnvironmentRollbackCreate,
    actor_id: str,
    idempotency_key: str,
    settings: Settings,
) -> EnvironmentResult:
    payload = body.model_dump(mode="json") | {"environment": environment}
    action, replay = _begin_action(
        session,
        action="rollback",
        environment=environment,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        payload=payload,
    )
    if replay is not None:
        return replay
    if environment not in {"pre", "prod-sim"}:
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="rollback_environment_denied",
            detail="Solo se revierte pre o prod-sim",
            status_code=403,
        )
    target = session.get(EnvironmentDeployment, body.target_deployment_id)
    current = _current(session, environment)
    if target is None or target.environment != environment or current is None:
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="rollback_target_not_found",
            detail="Candidato de rollback no encontrado",
            status_code=404,
        )
    approved = _approve(
        action,
        requester=actor_id,
        approval_actor=body.approval.actor_id,
        approval_reason=body.approval.reason,
        settings=settings,
        operator_only=environment == "prod-sim",
    )
    if approved is None:
        _reject(
            session,
            action,
            actor_id=actor_id,
            code="independent_approval_required",
            detail="El rollback exige aprobación independiente autorizada",
            status_code=403,
        )
    current.status = "superseded"
    deployment = EnvironmentDeployment(
        environment=environment,
        instance_key="shared",
        repository=target.repository,
        ref_kind=target.ref_kind,
        ref_value=target.ref_value,
        resolved_sha=target.resolved_sha,
        source_deployment_id=target.source_deployment_id,
        rollback_of_deployment_id=current.id,
        requested_by_actor_id=actor_id,
        approval_actor_id=approved[0],
    )
    session.add(deployment)
    session.flush()
    action.status = "applied"
    action.deployment_id = deployment.id
    action.previous_deployment_id = current.id
    _audit(
        session,
        actor_id=actor_id,
        event_type="environment.rolled_back",
        aggregate_id=str(deployment.id),
        payload={
            "environment": environment,
            "from": str(current.id),
            "candidate": str(target.id),
            "resolved_sha": target.resolved_sha,
            "approved_by": approved[0],
        },
    )
    session.commit()
    session.refresh(deployment)
    return EnvironmentResult(deployment)


def to_environment_response(
    result: EnvironmentResult | EnvironmentDeployment,
) -> EnvironmentDeploymentResponse:
    replayed = isinstance(result, EnvironmentResult) and result.replayed
    deployment = result.deployment if isinstance(result, EnvironmentResult) else result
    return EnvironmentDeploymentResponse(
        id=deployment.id,
        environment=deployment.environment,  # type: ignore[arg-type]
        instance_key=deployment.instance_key,
        repository=deployment.repository,
        ref_kind=deployment.ref_kind,  # type: ignore[arg-type]
        ref_value=deployment.ref_value,
        resolved_sha=deployment.resolved_sha,
        task_id=deployment.task_id,
        allocated_port=deployment.allocated_port,
        expires_at=deployment.expires_at,
        status=deployment.status,
        source_deployment_id=deployment.source_deployment_id,
        rollback_of_deployment_id=deployment.rollback_of_deployment_id,
        requested_by_actor_id=deployment.requested_by_actor_id,
        approval_actor_id=deployment.approval_actor_id,
        replayed=replayed,
        created_at=deployment.created_at,
    )
