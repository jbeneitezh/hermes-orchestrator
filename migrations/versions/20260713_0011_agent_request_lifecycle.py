"""Añade lifecycle gobernado y auditable a AgentRequest.

Revision ID: 20260713_0011
Revises: 20260713_0010
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0011"
down_revision: str | None = "20260713_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("agent_requests", sa.Column("decision_idempotency_key", sa.String(160)))
    op.add_column("agent_requests", sa.Column("decision_hash", sa.String(64)))
    op.add_column("agent_requests", sa.Column("decided_by_actor_id", sa.String(160)))
    op.add_column("agent_requests", sa.Column("decision_reason", sa.Text()))
    op.add_column("agent_requests", sa.Column("decided_at", sa.DateTime(timezone=True)))
    op.add_column("agent_requests", sa.Column("application_idempotency_key", sa.String(160)))
    op.add_column("agent_requests", sa.Column("application_hash", sa.String(64)))
    op.add_column("agent_requests", sa.Column("applied_by_actor_id", sa.String(160)))
    op.add_column("agent_requests", sa.Column("applied_at", sa.DateTime(timezone=True)))
    op.add_column("agent_requests", sa.Column("application_error_code", sa.String(100)))
    op.add_column("agent_requests", sa.Column("application_error_detail", sa.Text()))
    op.add_column("agent_requests", sa.Column("retirement_idempotency_key", sa.String(160)))
    op.add_column("agent_requests", sa.Column("retirement_hash", sa.String(64)))
    op.add_column("agent_requests", sa.Column("retired_by_actor_id", sa.String(160)))
    op.add_column("agent_requests", sa.Column("retirement_reason", sa.Text()))
    op.add_column("agent_requests", sa.Column("retired_at", sa.DateTime(timezone=True)))
    op.add_column(
        "agent_requests",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.execute("UPDATE agent_requests SET updated_at = CURRENT_TIMESTAMP")
    with op.batch_alter_table("agent_requests") as batch_op:
        batch_op.alter_column("updated_at", nullable=False)
    op.create_index(
        "ux_agent_requests_decision_idempotency_key",
        "agent_requests",
        ["decision_idempotency_key"],
        unique=True,
    )
    op.create_index(
        "ix_agent_requests_decided_by_actor_id",
        "agent_requests",
        ["decided_by_actor_id"],
    )
    op.create_index(
        "ux_agent_requests_application_idempotency_key",
        "agent_requests",
        ["application_idempotency_key"],
        unique=True,
    )
    op.create_index(
        "ix_agent_requests_applied_by_actor_id",
        "agent_requests",
        ["applied_by_actor_id"],
    )
    op.create_index(
        "ux_agent_requests_retirement_idempotency_key",
        "agent_requests",
        ["retirement_idempotency_key"],
        unique=True,
    )
    op.create_index(
        "ix_agent_requests_retired_by_actor_id",
        "agent_requests",
        ["retired_by_actor_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_agent_requests_retired_by_actor_id", table_name="agent_requests")
    op.drop_index("ux_agent_requests_retirement_idempotency_key", table_name="agent_requests")
    op.drop_index("ix_agent_requests_applied_by_actor_id", table_name="agent_requests")
    op.drop_index("ux_agent_requests_application_idempotency_key", table_name="agent_requests")
    op.drop_index("ix_agent_requests_decided_by_actor_id", table_name="agent_requests")
    op.drop_index("ux_agent_requests_decision_idempotency_key", table_name="agent_requests")
    op.drop_column("agent_requests", "updated_at")
    op.drop_column("agent_requests", "retired_at")
    op.drop_column("agent_requests", "retirement_reason")
    op.drop_column("agent_requests", "retired_by_actor_id")
    op.drop_column("agent_requests", "retirement_hash")
    op.drop_column("agent_requests", "retirement_idempotency_key")
    op.drop_column("agent_requests", "application_error_detail")
    op.drop_column("agent_requests", "application_error_code")
    op.drop_column("agent_requests", "applied_at")
    op.drop_column("agent_requests", "applied_by_actor_id")
    op.drop_column("agent_requests", "application_hash")
    op.drop_column("agent_requests", "application_idempotency_key")
    op.drop_column("agent_requests", "decided_at")
    op.drop_column("agent_requests", "decision_reason")
    op.drop_column("agent_requests", "decided_by_actor_id")
    op.drop_column("agent_requests", "decision_hash")
    op.drop_column("agent_requests", "decision_idempotency_key")
