from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, NoReturn

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.fleet_runner import FleetRunner
from hermes_orchestrator.models import AuditEvent, FleetReconcileRecord
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.repositories import AuditRepository, FleetReconcileRepository
from hermes_orchestrator.schemas import FleetReconcileCreate, FleetReconcileResponse


class FleetReconcileError(Exception):
    def __init__(self, code: str, detail: str, status_code: int) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


@dataclass(frozen=True)
class FleetReconcileResult:
    record: FleetReconcileRecord
    replayed: bool


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _inside_root(path: str, roots: list[str]) -> bool:
    normalized = os.path.normpath(path)
    if not os.path.isabs(normalized):
        return False
    try:
        return any(
            os.path.commonpath((normalized, os.path.normpath(root))) == os.path.normpath(root)
            for root in roots
        )
    except ValueError:
        return False


def _desired_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "project_name": payload["project_name"],
        "compose_path": payload["compose_path"],
        "targets": sorted(payload["targets"], key=lambda item: item["name"]),
    }


def _diff(previous: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any]:
    previous_targets = {item["name"]: item for item in previous.get("targets", [])}
    desired_targets = {item["name"]: item for item in desired.get("targets", [])}
    added = sorted(desired_targets.keys() - previous_targets.keys())
    removed = sorted(previous_targets.keys() - desired_targets.keys())
    changed = sorted(
        name
        for name in desired_targets.keys() & previous_targets.keys()
        if desired_targets[name] != previous_targets[name]
    )
    return {
        "added": added,
        "changed": changed,
        "removed": removed,
        "has_changes": bool(added or changed or removed),
    }


def _policy_violation(body: FleetReconcileCreate, settings: Settings) -> tuple[str, str] | None:
    if body.project_name != settings.fleet_project_name:
        return "project_not_allowed", "El proyecto Compose no está permitido"
    if os.path.normpath(body.compose_path) != os.path.normpath(settings.fleet_compose_path):
        return "compose_path_not_allowed", "La ruta Compose no está permitida"
    names = [target.name for target in body.targets]
    if len(names) != len(set(names)):
        return "duplicate_service", "No se permiten servicios duplicados"
    for target in body.targets:
        if not target.name.startswith("worker-"):
            return "service_not_allowed", "Solo se reconcilian servicios worker"
        if target.image != settings.fleet_allowed_worker_image:
            return "image_not_allowed", f"Imagen no permitida para {target.name}"
        if target.privileged:
            return "privileged_denied", f"privileged está denegado para {target.name}"
        if target.network_mode not in (None, "private"):
            return "network_mode_denied", f"network_mode está denegado para {target.name}"
        if target.pid_mode is not None:
            return "pid_mode_denied", f"pid_mode está denegado para {target.name}"
        for mount in target.mounts:
            if not os.path.isabs(mount.target):
                return "mount_target_denied", f"Target de mount inválido para {target.name}"
            if not _inside_root(mount.source, settings.fleet_allowed_mount_roots):
                return "mount_root_denied", f"Mount externo denegado para {target.name}"
            if "docker.sock" in mount.source.lower() or "docker.sock" in mount.target.lower():
                return "docker_socket_denied", f"Socket Docker denegado para {target.name}"
            tradix_roots = [
                root for root in settings.fleet_allowed_mount_roots if root.endswith("/tradix")
            ]
            if tradix_roots and _inside_root(mount.source, tradix_roots) and not mount.read_only:
                return "product_mount_must_be_read_only", "Tradix solo puede montarse read-only"
    return None


def _append_audit(
    session: Session,
    record: FleetReconcileRecord,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    AuditRepository(session).append(
        AuditEvent(
            actor_id=record.requested_by_actor_id,
            event_type=event_type,
            aggregate_type="fleet_reconcile",
            aggregate_id=str(record.id),
            payload=payload,
        )
    )


def _raise_recorded(
    session: Session,
    record: FleetReconcileRecord,
    *,
    code: str,
    detail: str,
    status_code: int,
    event_type: str,
) -> NoReturn:
    record.status = "rejected" if status_code < 500 else "failed"
    record.runner_result = {"error_code": code}
    _append_audit(session, record, event_type, {"code": code})
    session.commit()
    raise FleetReconcileError(code, detail, status_code)


def to_fleet_response(
    record: FleetReconcileRecord, *, replayed: bool = False
) -> FleetReconcileResponse:
    return FleetReconcileResponse(
        id=record.id,
        status=record.status,
        mode=record.mode,
        risk=record.risk,
        desired_hash=record.desired_hash,
        diff=record.diff_snapshot,
        approval_actor_id=record.approval_actor_id,
        runner_result=record.runner_result,
        rollback_available=bool(record.rollback_payload),
        replayed=replayed,
        created_at=record.created_at,
    )


def request_fleet_reconcile(
    session: Session,
    *,
    settings: Settings,
    runner: FleetRunner,
    actor_id: str,
    idempotency_key: str,
    body: FleetReconcileCreate,
) -> FleetReconcileResult:
    payload = body.model_dump(mode="json")
    request_hash = _sha256(payload)
    repository = FleetReconcileRepository(session)
    existing = repository.get_by_idempotency_key(idempotency_key)
    if existing is not None:
        if existing.request_hash != request_hash or existing.requested_by_actor_id != actor_id:
            raise FleetReconcileError(
                "idempotency_conflict", "La clave ya se usó con otra solicitud", 409
            )
        if existing.status in {"rejected", "failed"}:
            code = str(existing.runner_result.get("error_code", "fleet_reconcile_rejected"))
            raise FleetReconcileError(code, "La solicitud ya fue rechazada", 409)
        return FleetReconcileResult(existing, True)

    desired = _desired_payload(payload)
    desired_hash = _sha256(desired)
    previous_record = repository.latest_applied(body.project_name, body.compose_path)
    previous = previous_record.request_payload if previous_record is not None else {}
    diff = _diff(previous, desired)
    record = FleetReconcileRecord(
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        requested_by_actor_id=actor_id,
        project_name=body.project_name,
        compose_path=body.compose_path,
        mode=body.mode,
        request_payload=desired,
        desired_hash=desired_hash,
        diff_snapshot=diff,
        risk="high" if diff["has_changes"] else "none",
        status="requested",
        rollback_payload=previous,
    )
    repository.add(record)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        concurrent = repository.get_by_idempotency_key(idempotency_key)
        if (
            concurrent is None
            or concurrent.request_hash != request_hash
            or concurrent.requested_by_actor_id != actor_id
        ):
            raise FleetReconcileError(
                "idempotency_conflict", "Colisión de idempotencia", 409
            ) from None
        return FleetReconcileResult(concurrent, True)

    _append_audit(
        session,
        record,
        "fleet.reconcile.requested",
        {"mode": body.mode, "risk": record.risk, "desired_hash": desired_hash},
    )
    violation = _policy_violation(body, settings)
    if violation is not None:
        _raise_recorded(
            session,
            record,
            code=violation[0],
            detail=violation[1],
            status_code=422,
            event_type="fleet.reconcile.rejected",
        )

    if body.mode == "apply" and diff["has_changes"]:
        approval = body.approval
        if (
            approval is None
            or approval.actor_id == actor_id
            or not actor_is_allowed(
                approval.actor_id, Permission.APPROVALS_DECIDE, settings.actor_roles
            )
        ):
            _raise_recorded(
                session,
                record,
                code="approval_required",
                detail="El apply con cambios exige aprobación independiente",
                status_code=409,
                event_type="fleet.reconcile.approval_required",
            )
        record.approval_actor_id = approval.actor_id
        record.approval_reason = approval.reason

    try:
        if body.mode == "dry_run" or not diff["has_changes"]:
            runner_result = runner.plan()
        else:
            runner_result = runner.apply([target.name for target in body.targets])
    except Exception as error:
        _raise_recorded(
            session,
            record,
            code="fleet_runner_failed",
            detail="El reconciler no pudo completar la operación",
            status_code=503,
            event_type="fleet.reconcile.failed",
        )
        raise AssertionError("unreachable") from error

    configured = set(runner_result.get("configured_services", []))
    requested = {target.name for target in body.targets}
    if not requested.issubset(configured):
        _raise_recorded(
            session,
            record,
            code="service_not_configured",
            detail="Un worker solicitado no existe en el Compose validado",
            status_code=422,
            event_type="fleet.reconcile.rejected",
        )
    record.runner_result = runner_result
    if body.mode == "dry_run":
        record.status = "dry_run"
        event_type = "fleet.reconcile.dry_run"
    elif not diff["has_changes"]:
        record.status = "no_change"
        event_type = "fleet.reconcile.no_change"
    else:
        record.status = "applied"
        event_type = "fleet.reconcile.applied"

    _append_audit(
        session,
        record,
        event_type,
        {"status": record.status, "desired_hash": desired_hash},
    )
    session.commit()
    return FleetReconcileResult(record, False)
