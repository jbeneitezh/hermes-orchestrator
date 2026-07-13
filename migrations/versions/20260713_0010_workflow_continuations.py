"""Añade continuaciones durables e idempotentes por evento.

Revision ID: 20260713_0010
Revises: 20260713_0009
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0010"
down_revision: str | None = "20260713_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_continuations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("parent_task_id", sa.Uuid(), nullable=False),
        sa.Column("trigger_event_id", sa.Uuid(), nullable=False),
        sa.Column("target_actor_id", sa.String(length=160), nullable=False),
        sa.Column("action", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("idempotency_key", sa.String(length=160), nullable=False),
        sa.Column("context_snapshot", sa.JSON(), nullable=False),
        sa.Column("failure_code", sa.String(length=100)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dispatched_at", sa.DateTime(timezone=True)),
        sa.Column("failed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["parent_task_id"], ["tasks.id"]),
        sa.ForeignKeyConstraint(["trigger_event_id"], ["run_events.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "trigger_event_id",
            "target_actor_id",
            "action",
            name="uq_workflow_continuations_trigger_actor_action",
        ),
    )
    op.create_index(
        "ix_workflow_continuations_operation_id",
        "workflow_continuations",
        ["operation_id"],
    )
    op.create_index(
        "ix_workflow_continuations_parent_task_id",
        "workflow_continuations",
        ["parent_task_id"],
    )
    op.create_index(
        "ix_workflow_continuations_trigger_event_id",
        "workflow_continuations",
        ["trigger_event_id"],
    )
    op.create_index(
        "ix_workflow_continuations_target_actor_id",
        "workflow_continuations",
        ["target_actor_id"],
    )
    op.create_index(
        "ix_workflow_continuations_status",
        "workflow_continuations",
        ["status"],
    )
    op.create_index(
        "ix_workflow_continuations_idempotency_key",
        "workflow_continuations",
        ["idempotency_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_table("workflow_continuations")
