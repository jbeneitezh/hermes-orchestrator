"""Añade claim y lease durable para Runs pendientes.

Revision ID: 20260713_0008
Revises: 20260713_0007
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0008"
down_revision: str | None = "20260713_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runs",
        sa.Column("dispatch_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "runs",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.add_column("runs", sa.Column("lease_owner", sa.String(length=160)))
    op.add_column("runs", sa.Column("lease_acquired_at", sa.DateTime(timezone=True)))
    op.add_column("runs", sa.Column("lease_expires_at", sa.DateTime(timezone=True)))
    op.add_column("runs", sa.Column("heartbeat_at", sa.DateTime(timezone=True)))
    op.alter_column("runs", "dispatch_attempts", server_default=None)
    op.alter_column("runs", "next_attempt_at", server_default=None)
    op.create_index(
        "ix_runs_dispatch_claim",
        "runs",
        ["status", "next_attempt_at", "lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_runs_dispatch_claim", table_name="runs")
    op.drop_column("runs", "heartbeat_at")
    op.drop_column("runs", "lease_expires_at")
    op.drop_column("runs", "lease_acquired_at")
    op.drop_column("runs", "lease_owner")
    op.drop_column("runs", "next_attempt_at")
    op.drop_column("runs", "dispatch_attempts")
