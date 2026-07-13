"""Añade entornos gobernados y promoción por referencia inmutable.

Revision ID: 20260713_0007
Revises: 20260713_0006
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0007"
down_revision: str | None = "20260713_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "environment_deployments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("environment", sa.String(length=30), nullable=False),
        sa.Column("instance_key", sa.String(length=160), nullable=False),
        sa.Column("repository", sa.String(length=240), nullable=False),
        sa.Column("ref_kind", sa.String(length=20), nullable=False),
        sa.Column("ref_value", sa.String(length=300), nullable=False),
        sa.Column("resolved_sha", sa.String(length=40), nullable=False),
        sa.Column("task_id", sa.Uuid()),
        sa.Column("allocated_port", sa.Integer()),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("source_deployment_id", sa.Uuid()),
        sa.Column("rollback_of_deployment_id", sa.Uuid()),
        sa.Column("requested_by_actor_id", sa.String(length=160), nullable=False),
        sa.Column("approval_actor_id", sa.String(length=160)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.ForeignKeyConstraint(["source_deployment_id"], ["environment_deployments.id"]),
        sa.ForeignKeyConstraint(["rollback_of_deployment_id"], ["environment_deployments.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "environment",
        "instance_key",
        "repository",
        "resolved_sha",
        "task_id",
        "allocated_port",
        "expires_at",
        "status",
        "source_deployment_id",
        "rollback_of_deployment_id",
        "requested_by_actor_id",
    ):
        op.create_index(f"ix_environment_deployments_{column}", "environment_deployments", [column])
    op.create_index(
        "ix_environment_deployments_current",
        "environment_deployments",
        ["environment", "instance_key", "status"],
    )

    op.create_table(
        "environment_actions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(length=30), nullable=False),
        sa.Column("environment", sa.String(length=30), nullable=False),
        sa.Column("requested_by_actor_id", sa.String(length=160), nullable=False),
        sa.Column("idempotency_key", sa.String(length=160), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("request_payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("deployment_id", sa.Uuid()),
        sa.Column("previous_deployment_id", sa.Uuid()),
        sa.Column("approval_actor_id", sa.String(length=160)),
        sa.Column("approval_reason", sa.Text()),
        sa.Column("error_code", sa.String(length=100)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["deployment_id"], ["environment_deployments.id"]),
        sa.ForeignKeyConstraint(["previous_deployment_id"], ["environment_deployments.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "action",
        "environment",
        "requested_by_actor_id",
        "status",
        "deployment_id",
        "previous_deployment_id",
        "error_code",
    ):
        op.create_index(f"ix_environment_actions_{column}", "environment_actions", [column])
    op.create_index(
        "ix_environment_actions_idempotency_key",
        "environment_actions",
        ["idempotency_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_table("environment_actions")
    op.drop_table("environment_deployments")
