from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.agent_policy import AgentPolicyCoordinator, process_agent_request
from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import Agent, AgentRequestRecord, AuditEvent, Base
from hermes_orchestrator.provisioning import (
    ProvisionerResult,
    ProvisioningError,
    ProvisioningPayload,
)
from hermes_orchestrator.services import decide_agent_request, request_agent

SAFE_PAYLOAD = {
    "slug": "data-steward-policy-01",
    "role": "data_steward",
    "description": "Custodia datasets y conocimiento compartido",
    "policy_set": {
        "name": "data-steward-v3",
        "execution_profile_default": "sol-high",
        "allowed_profiles": ["sol-high"],
        "toolsets": ["terminal_read", "files_read", "git", "mcp"],
        "mcp_tools": ["task_get", "task_comment", "task_block", "task_complete"],
        "communication": ["agent:leader", "agent:researcher", "agent:validator"],
        "mount_profile": "knowledge-branch-workspace",
        "budget": {
            "hard_token_limit": 400_000,
            "max_concurrent_runs": 1,
            "max_iterations": 6,
            "max_sources": 20,
        },
    },
    "capabilities": [
        "dataset_read",
        "knowledge_read",
        "knowledge_write_branch",
        "git_read",
        "git_write_branch",
        "github_pr_create",
        "task_read",
    ],
    "secret_refs": ["secret://codex/broker-client", "secret://github/shared-agent"],
}

RISK_MANAGER_PAYLOAD = {
    "slug": "risk-manager-policy-01",
    "role": "risk_manager",
    "description": "Gestiona el marco de riesgos de Tradix sin autoridad operativa",
    "policy_set": {
        "name": "risk-manager-v3",
        "execution_profile_default": "sol-high",
        "allowed_profiles": ["sol-high"],
        "toolsets": ["terminal_read", "files_read", "git", "mcp"],
        "mcp_tools": ["task_get", "task_comment", "task_block", "task_complete"],
        "communication": [
            "agent:leader",
            "agent:trader",
            "agent:researcher",
            "agent:validator",
        ],
        "mount_profile": "risk-knowledge-dataset-tradix-readonly",
        "budget": {
            "hard_token_limit": 600_000,
            "max_concurrent_runs": 1,
            "max_iterations": 8,
            "max_sources": 30,
        },
    },
    "capabilities": [
        "dataset_read",
        "metrics_read",
        "knowledge_read",
        "knowledge_write_branch",
        "git_read",
        "git_write_branch",
        "github_pr_create",
        "task_read",
        "task_comment",
        "task_block",
        "task_complete",
    ],
    "secret_refs": ["secret://codex/broker-client", "secret://github/shared-agent"],
}


class FakeProvisioner:
    def __init__(self) -> None:
        self.apply_calls: list[ProvisioningPayload] = []

    def apply(self, payload: ProvisioningPayload) -> ProvisionerResult:
        self.apply_calls.append(payload)
        return ProvisionerResult(
            status="applied",
            service_name=f"worker-{payload.slug}",
            config_digest="a" * 64,
            health="healthy",
            credential_sha256="b" * 64,
        )

    def rollback(self, payload: ProvisioningPayload) -> ProvisionerResult:
        raise AssertionError(f"Rollback inesperado para {payload.slug}")


class FailingProvisioner(FakeProvisioner):
    def apply(self, payload: ProvisioningPayload) -> ProvisionerResult:
        raise ProvisioningError("controlled_failure", "Fallo controlado", 503)


@pytest.fixture
def policy_context(tmp_path: Path):
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'agent-policy.db').as_posix()}")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    settings = Settings(
        environment="test",
        agent_policy_enabled=True,
        agent_policy_actor_id="system:agent-policy",
        agent_policy_allowed_roles=["data_steward", "risk_manager"],
        agent_policy_max_active_agents=6,
        agent_policy_batch_size=10,
    )
    provisioner = FakeProvisioner()
    yield factory, settings, provisioner
    engine.dispose()


def create_request(
    factory: sessionmaker[Session],
    payload: dict[str, object] | None = None,
    *,
    actor_id: str = "agent:leader",
) -> uuid.UUID:
    with factory() as session:
        result = request_agent(
            session,
            actor_id=actor_id,
            idempotency_key=f"request-{uuid.uuid4()}",
            payload=payload or SAFE_PAYLOAD,
        )
        return result.request.id


def test_safe_request_is_approved_and_applied_by_system_actor(policy_context) -> None:
    factory, settings, provisioner = policy_context
    request_id = create_request(factory)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()

    assert len(result) == 1
    assert result[0].action == "apply" and result[0].status == "applied"
    assert result[0].profile_digest is not None
    assert len(provisioner.apply_calls) == 1
    with factory() as session:
        request = session.get(AgentRequestRecord, request_id)
        agent = session.scalar(select(Agent).where(Agent.slug == SAFE_PAYLOAD["slug"]))
        assert request is not None and request.status == "applied"
        assert request.requested_by_actor_id == "agent:leader"
        assert request.decided_by_actor_id == "system:agent-policy"
        assert request.applied_by_actor_id == "system:agent-policy"
        assert agent is not None and agent.desired_state == "active"


def test_risk_manager_profile_is_allowlisted_with_independent_approval(policy_context) -> None:
    factory, settings, provisioner = policy_context
    request_id = create_request(factory, RISK_MANAGER_PAYLOAD)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]

    assert result.action == "apply" and result.status == "applied"
    assert result.profile_digest is not None
    assert provisioner.apply_calls[0].role == "risk_manager"
    with factory() as session:
        request = session.get(AgentRequestRecord, request_id)
        agent = session.scalar(select(Agent).where(Agent.slug == RISK_MANAGER_PAYLOAD["slug"]))
        assert request is not None
        assert request.requested_by_actor_id == "agent:leader"
        assert request.decided_by_actor_id == "system:agent-policy"
        assert agent is not None and agent.role == "risk_manager"


@pytest.mark.parametrize(
    "capability",
    [
        "git_write_product",
        "order_submit",
        "capital_allocate",
        "docker_admin",
        "secret_read",
        "approval_decide",
        "self_approve",
    ],
)
def test_risk_manager_sensitive_authority_is_rejected(policy_context, capability: str) -> None:
    factory, settings, provisioner = policy_context
    payload = RISK_MANAGER_PAYLOAD | {
        "slug": f"risk-manager-{uuid.uuid4().hex[:8]}",
        "capabilities": [*RISK_MANAGER_PAYLOAD["capabilities"], capability],
    }
    create_request(factory, payload)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]

    assert result.status == "rejected"
    assert result.code in {"capability_not_allowlisted", "sensitive_authority_denied"}
    assert provisioner.apply_calls == []


def test_risk_manager_communication_and_mount_cannot_expand(policy_context) -> None:
    factory, settings, provisioner = policy_context
    forbidden = [
        RISK_MANAGER_PAYLOAD
        | {
            "slug": "risk-manager-operator",
            "policy_set": RISK_MANAGER_PAYLOAD["policy_set"]
            | {"communication": ["agent:operator"]},
        },
        RISK_MANAGER_PAYLOAD
        | {
            "slug": "risk-manager-host",
            "policy_set": RISK_MANAGER_PAYLOAD["policy_set"] | {"mount_profile": "host-root"},
        },
    ]
    for payload in forbidden:
        create_request(factory, payload)

    results = AgentPolicyCoordinator(factory, settings, provisioner).run_once()

    assert {result.code for result in results} == {
        "communication_not_allowlisted",
        "mount_not_allowlisted",
    }
    assert provisioner.apply_calls == []


def test_replay_does_not_apply_twice_or_duplicate_audit(policy_context) -> None:
    factory, settings, provisioner = policy_context
    request_id = create_request(factory)
    AgentPolicyCoordinator(factory, settings, provisioner).run_once()
    with factory() as session:
        before = session.scalar(select(func.count(AuditEvent.id)))
        replay = process_agent_request(
            session,
            request_id=request_id,
            settings=settings,
            provisioner=provisioner,
        )
        after = session.scalar(select(func.count(AuditEvent.id)))

    assert replay.replayed is True and replay.status == "applied"
    assert len(provisioner.apply_calls) == 1
    assert before == after


def test_role_outside_catalog_is_rejected(policy_context) -> None:
    factory, settings, provisioner = policy_context
    payload = SAFE_PAYLOAD | {
        "slug": "trader-policy-01",
        "role": "trader",
    }
    request_id = create_request(factory, payload)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]

    assert result.action == "reject" and result.code == "role_not_allowlisted"
    with factory() as session:
        request = session.get(AgentRequestRecord, request_id)
        assert request is not None and request.status == "rejected"
    assert provisioner.apply_calls == []


@pytest.mark.parametrize(
    ("patch", "code"),
    [
        ({"capabilities": ["dataset_read", "docker_admin"]}, "capability_not_allowlisted"),
        ({"secret_refs": ["secret://new/provider"]}, "secret_ref_not_allowlisted"),
        (
            {"policy_set": SAFE_PAYLOAD["policy_set"] | {"mount_profile": "host-root"}},
            "mount_not_allowlisted",
        ),
        (
            {"policy_set": SAFE_PAYLOAD["policy_set"] | {"communication": ["agent:operator"]}},
            "communication_not_allowlisted",
        ),
        (
            {"policy_set": SAFE_PAYLOAD["policy_set"] | {"budget": {"max_concurrent_runs": 2}}},
            "budget_limit_exceeded",
        ),
        (
            {"policy_set": SAFE_PAYLOAD["policy_set"] | {"live_trade": True}},
            "policy_field_not_allowlisted",
        ),
    ],
)
def test_capability_secret_mount_communication_budget_and_live_escalations_are_rejected(
    policy_context,
    patch: dict[str, object],
    code: str,
) -> None:
    factory, settings, provisioner = policy_context
    payload = SAFE_PAYLOAD | patch | {"slug": f"data-steward-{uuid.uuid4().hex[:8]}"}
    create_request(factory, payload)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]

    assert result.action == "reject" and result.code == code
    assert provisioner.apply_calls == []


def test_fleet_limit_rejects_before_provisioning(policy_context) -> None:
    factory, settings, provisioner = policy_context
    with factory() as session:
        session.add_all(
            [
                Agent(
                    slug=f"managed-{index}",
                    role="researcher",
                    description="Agente dinámico activo",
                    desired_state="active",
                    owner_actor_id="system:test",
                    policy_set={"managed_by": "agent-provisioner"},
                )
                for index in range(settings.agent_policy_max_active_agents)
            ]
        )
        session.commit()
    create_request(factory)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]

    assert result.action == "reject" and result.code == "fleet_limit_exceeded"
    assert provisioner.apply_calls == []


def test_policy_actor_cannot_approve_its_own_request(policy_context) -> None:
    factory, settings, provisioner = policy_context
    request_id = create_request(factory, actor_id=settings.agent_policy_actor_id)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]

    assert result.action == "reject" and result.code == "self_escalation_denied"
    with factory() as session:
        request = session.get(AgentRequestRecord, request_id)
        assert request is not None and request.status == "rejected"
        assert request.decided_by_actor_id == settings.agent_policy_actor_id


def test_disabled_policy_keeps_request_pending(policy_context) -> None:
    factory, settings, provisioner = policy_context
    request_id = create_request(factory)
    disabled = settings.model_copy(update={"agent_policy_enabled": False})

    assert AgentPolicyCoordinator(factory, disabled, provisioner).run_once() == []
    with factory() as session:
        request = session.get(AgentRequestRecord, request_id)
        assert request is not None and request.status == "pending"
    assert provisioner.apply_calls == []


def test_external_approval_is_not_claimed_by_automatic_policy(policy_context) -> None:
    factory, settings, provisioner = policy_context
    request_id = create_request(factory)
    with factory() as session:
        decide_agent_request(
            session,
            request_id=request_id,
            actor_id="agent:operator",
            idempotency_key=f"manual-{request_id}",
            decision="approve",
            reason="Decisión externa",
        )
        result = process_agent_request(
            session,
            request_id=request_id,
            settings=settings,
            provisioner=provisioner,
        )

    assert result.action == "defer" and result.code == "external_approval_owner"
    assert provisioner.apply_calls == []


@pytest.mark.parametrize(
    ("patch", "code"),
    [
        ({"slug": "wrong-prefix"}, "slug_profile_mismatch"),
        ({"capabilities": "dataset_read"}, "capability_not_allowlisted"),
        ({"secret_refs": "secret://codex/broker-client"}, "secret_ref_not_allowlisted"),
        (
            {"policy_set": SAFE_PAYLOAD["policy_set"] | {"name": "docker-admin"}},
            "sensitive_authority_denied",
        ),
        (
            {"policy_set": SAFE_PAYLOAD["policy_set"] | {"name": "unknown"}},
            "policy_name_not_allowlisted",
        ),
        (
            {
                "policy_set": SAFE_PAYLOAD["policy_set"]
                | {"execution_profile_default": "terra-medium"}
            },
            "model_policy_denied",
        ),
        (
            {
                "policy_set": SAFE_PAYLOAD["policy_set"]
                | {"allowed_profiles": ["sol-high", "terra-medium"]}
            },
            "model_policy_denied",
        ),
        (
            {"policy_set": SAFE_PAYLOAD["policy_set"] | {"budget": {"unknown": 1}}},
            "budget_not_allowlisted",
        ),
    ],
)
def test_malformed_or_unsigned_profile_shapes_are_rejected(
    policy_context,
    patch: dict[str, object],
    code: str,
) -> None:
    factory, settings, provisioner = policy_context
    payload = SAFE_PAYLOAD | patch
    if "slug" not in patch:
        payload = payload | {"slug": f"data-steward-{uuid.uuid4().hex[:8]}"}
    create_request(factory, payload)

    result = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]

    assert result.code == code and result.status == "rejected"


def test_duplicate_slug_and_role_capacity_are_rejected(policy_context) -> None:
    factory, settings, provisioner = policy_context
    with factory() as session:
        session.add(
            Agent(
                slug=SAFE_PAYLOAD["slug"],
                role="data_steward",
                description="Instancia ya activa",
                desired_state="active",
                owner_actor_id="agent:leader",
                policy_set={"managed_by": "agent-provisioner"},
            )
        )
        session.commit()
    create_request(factory)
    duplicate = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]
    assert duplicate.code == "slug_already_active"

    second_payload = SAFE_PAYLOAD | {"slug": "data-steward-policy-02"}
    create_request(factory, second_payload)
    with factory() as session:
        session.add(
            Agent(
                slug="data-steward-existing-02",
                role="data_steward",
                description="Segunda instancia activa",
                desired_state="active",
                owner_actor_id="agent:leader",
                policy_set={"managed_by": "agent-provisioner"},
            )
        )
        session.commit()
    role_limit = AgentPolicyCoordinator(factory, settings, provisioner).run_once()[0]
    assert role_limit.code == "role_limit_exceeded"


def test_missing_disabled_approved_unknown_and_apply_failure_are_closed(policy_context) -> None:
    factory, settings, provisioner = policy_context
    with factory() as session:
        missing = process_agent_request(
            session,
            request_id=uuid.uuid4(),
            settings=settings,
            provisioner=provisioner,
        )
    assert missing.code == "request_not_found"

    disabled_id = create_request(factory, SAFE_PAYLOAD | {"slug": "data-steward-disabled"})
    disabled = settings.model_copy(update={"agent_policy_enabled": False})
    with factory() as session:
        deferred = process_agent_request(
            session,
            request_id=disabled_id,
            settings=disabled,
            provisioner=provisioner,
        )
    assert deferred.code == "policy_disabled"

    approved_id = create_request(factory, SAFE_PAYLOAD | {"slug": "data-steward-approved"})
    with factory() as session:
        decide_agent_request(
            session,
            request_id=approved_id,
            actor_id=settings.agent_policy_actor_id,
            idempotency_key=f"agent-policy-decision:{approved_id}",
            decision="approve",
            reason="Aprobación recuperable",
        )
        approved_deferred = process_agent_request(
            session,
            request_id=approved_id,
            settings=disabled,
            provisioner=provisioner,
        )
    assert approved_deferred.code == "policy_disabled"

    unknown_id = create_request(factory, SAFE_PAYLOAD | {"slug": "data-steward-unknown"})
    with factory() as session:
        unknown = session.get(AgentRequestRecord, unknown_id)
        assert unknown is not None
        unknown.status = "apply_failed"
        session.commit()
        unknown_result = process_agent_request(
            session,
            request_id=unknown_id,
            settings=settings,
            provisioner=provisioner,
        )
    assert unknown_result.code == "state_not_eligible"

    failing_id = create_request(factory, SAFE_PAYLOAD | {"slug": "data-steward-failing"})
    with factory() as session:
        failed = process_agent_request(
            session,
            request_id=failing_id,
            settings=settings,
            provisioner=FailingProvisioner(),
        )
    assert failed.status == "apply_failed" and failed.code == "controlled_failure"
