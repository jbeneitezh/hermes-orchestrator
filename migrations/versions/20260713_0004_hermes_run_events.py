"""Añade enlace de Hermes y eventos normalizados de run.

Revision ID: 20260713_0004
Revises: 20260713_0003
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0004"
down_revision: str | None = "20260713_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("worker_run_id", sa.String(length=160)))
    op.add_column(
        "runs", sa.Column("usage_snapshot", sa.JSON(), nullable=False, server_default="{}")
    )
    op.add_column(
        "runs", sa.Column("error_details", sa.JSON(), nullable=False, server_default="{}")
    )
    op.alter_column("runs", "usage_snapshot", server_default=None)
    op.alter_column("runs", "error_details", server_default=None)
    op.create_index("ix_runs_worker_run_id", "runs", ["worker_run_id"])
    op.create_table(
        "run_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("worker_event_id", sa.String(length=160)),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("terminal", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "sequence", name="uq_run_events_sequence"),
    )
    op.create_index("ix_run_events_run_id", "run_events", ["run_id"])
    op.create_index("ix_run_events_event_type", "run_events", ["event_type"])
    op.create_index("ix_run_events_terminal", "run_events", ["terminal"])


def downgrade() -> None:
    op.drop_table("run_events")
    op.drop_index("ix_runs_worker_run_id", table_name="runs")
    op.drop_column("runs", "error_details")
    op.drop_column("runs", "usage_snapshot")
    op.drop_column("runs", "worker_run_id")
