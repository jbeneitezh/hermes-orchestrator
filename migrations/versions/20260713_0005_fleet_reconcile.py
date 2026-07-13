"""Añade solicitudes durables de reconciliación de flota.

Revision ID: 20260713_0005
Revises: 20260713_0004
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0005"
down_revision: str | None = "20260713_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "fleet_reconcile_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=160), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("requested_by_actor_id", sa.String(length=160), nullable=False),
        sa.Column("project_name", sa.String(length=120), nullable=False),
        sa.Column("compose_path", sa.String(length=500), nullable=False),
        sa.Column("mode", sa.String(length=20), nullable=False),
        sa.Column("request_payload", sa.JSON(), nullable=False),
        sa.Column("desired_hash", sa.String(length=64), nullable=False),
        sa.Column("diff_snapshot", sa.JSON(), nullable=False),
        sa.Column("risk", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("approval_actor_id", sa.String(length=160)),
        sa.Column("approval_reason", sa.Text()),
        sa.Column("runner_result", sa.JSON(), nullable=False),
        sa.Column("rollback_payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_fleet_reconcile_records_idempotency_key",
        "fleet_reconcile_records",
        ["idempotency_key"],
        unique=True,
    )
    op.create_index(
        "ix_fleet_reconcile_records_requested_by_actor_id",
        "fleet_reconcile_records",
        ["requested_by_actor_id"],
    )
    op.create_index(
        "ix_fleet_reconcile_records_project_name",
        "fleet_reconcile_records",
        ["project_name"],
    )
    op.create_index(
        "ix_fleet_reconcile_records_desired_hash",
        "fleet_reconcile_records",
        ["desired_hash"],
    )
    op.create_index(
        "ix_fleet_reconcile_records_status",
        "fleet_reconcile_records",
        ["status"],
    )


def downgrade() -> None:
    op.drop_table("fleet_reconcile_records")
