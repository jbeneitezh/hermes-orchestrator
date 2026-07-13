"""Añade catálogo de agentes, perfiles, comunicación y auditoría.

Revision ID: 20260713_0002
Revises: 20260713_0001
Create Date: 2026-07-13
"""

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0002"
down_revision: str | None = "20260713_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "agents",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(length=80), nullable=False),
        sa.Column("role", sa.String(length=40), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("desired_state", sa.String(length=30), nullable=False),
        sa.Column("owner_actor_id", sa.String(length=160), nullable=False),
        sa.Column("policy_set", sa.JSON(), nullable=False),
        sa.Column("capabilities", sa.JSON(), nullable=False),
        sa.Column("secret_refs", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_agents_slug", "agents", ["slug"], unique=True)
    op.create_index("ix_agents_role", "agents", ["role"])
    op.create_index("ix_agents_owner_actor_id", "agents", ["owner_actor_id"])

    op.create_table(
        "agent_instances",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("container_ref", sa.String(length=200)),
        sa.Column("hermes_version", sa.String(length=80)),
        sa.Column("image_digest", sa.String(length=160)),
        sa.Column("internal_endpoint", sa.String(length=500)),
        sa.Column("config_revision", sa.String(length=120)),
        sa.Column("health", sa.String(length=30), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("reconciliation_state", sa.String(length=30), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["agent_id"], ["agents.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_agent_instances_agent_id", "agent_instances", ["agent_id"])

    profiles = op.create_table(
        "execution_profiles",
        sa.Column("id", sa.String(length=80), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=False),
        sa.Column("reasoning_effort", sa.String(length=20), nullable=False),
        sa.Column("service_tier", sa.String(length=40)),
        sa.Column("max_iterations", sa.Integer(), nullable=False),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("tool_policy", sa.JSON(), nullable=False),
        sa.Column("relative_cost", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "communication_edges",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_agent_id", sa.Uuid(), nullable=False),
        sa.Column("target_agent_id", sa.Uuid(), nullable=False),
        sa.Column("task_classes", sa.JSON(), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("approved_by_actor_id", sa.String(length=160), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_agent_id"], ["agents.id"]),
        sa.ForeignKeyConstraint(["target_agent_id"], ["agents.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_communication_edges_source_agent_id",
        "communication_edges",
        ["source_agent_id"],
    )
    op.create_index(
        "ix_communication_edges_target_agent_id",
        "communication_edges",
        ["target_agent_id"],
    )
    op.create_index("ix_communication_edges_expires_at", "communication_edges", ["expires_at"])

    op.create_table(
        "agent_requests",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=160), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("requested_by_actor_id", sa.String(length=160), nullable=False),
        sa.Column("request_type", sa.String(length=30), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_agent_requests_idempotency_key",
        "agent_requests",
        ["idempotency_key"],
        unique=True,
    )
    op.create_index(
        "ix_agent_requests_requested_by_actor_id",
        "agent_requests",
        ["requested_by_actor_id"],
    )

    op.create_table(
        "audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("actor_id", sa.String(length=160), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("aggregate_type", sa.String(length=80), nullable=False),
        sa.Column("aggregate_id", sa.String(length=160), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_audit_events_actor_id", "audit_events", ["actor_id"])
    op.create_index("ix_audit_events_event_type", "audit_events", ["event_type"])
    op.create_index("ix_audit_events_aggregate_type", "audit_events", ["aggregate_type"])
    op.create_index("ix_audit_events_aggregate_id", "audit_events", ["aggregate_id"])

    op.execute(
        """
        CREATE FUNCTION reject_audit_event_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'audit_events is append-only';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER audit_events_append_only
        BEFORE UPDATE OR DELETE ON audit_events
        FOR EACH ROW EXECUTE FUNCTION reject_audit_event_mutation()
        """
    )

    created_at = datetime.now(UTC)
    profile_defaults = {
        "provider": "openai-codex",
        "service_tier": None,
        "timeout_seconds": 300,
        "tool_policy": {},
        "enabled": True,
        "created_at": created_at,
    }
    op.bulk_insert(
        profiles,
        [
            profile_defaults
            | {
                "id": "spark-low",
                "model": "gpt-5.3-codex-spark",
                "reasoning_effort": "low",
                "max_iterations": 8,
                "relative_cost": 1,
            },
            profile_defaults
            | {
                "id": "luna-low",
                "model": "gpt-5.6-luna",
                "reasoning_effort": "low",
                "max_iterations": 8,
                "relative_cost": 2,
            },
            profile_defaults
            | {
                "id": "terra-medium",
                "model": "gpt-5.6-terra",
                "reasoning_effort": "medium",
                "max_iterations": 20,
                "timeout_seconds": 900,
                "relative_cost": 3,
            },
            profile_defaults
            | {
                "id": "sol-high",
                "model": "gpt-5.6-sol",
                "reasoning_effort": "high",
                "max_iterations": 30,
                "timeout_seconds": 1800,
                "relative_cost": 4,
            },
        ],
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS audit_events_append_only ON audit_events")
    op.execute("DROP FUNCTION IF EXISTS reject_audit_event_mutation")
    op.drop_table("audit_events")
    op.drop_table("agent_requests")
    op.drop_table("communication_edges")
    op.drop_table("execution_profiles")
    op.drop_table("agent_instances")
    op.drop_table("agents")
