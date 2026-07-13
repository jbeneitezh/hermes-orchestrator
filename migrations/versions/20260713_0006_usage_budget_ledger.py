"""Añade ledger de consumo, presupuestos y circuit breakers durables.

Revision ID: 20260713_0006
Revises: 20260713_0005
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0006"
down_revision: str | None = "20260713_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "usage_ledger",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("parent_run_id", sa.Uuid()),
        sa.Column("project_id", sa.String(length=120), nullable=False),
        sa.Column("category", sa.String(length=120), nullable=False),
        sa.Column("requesting_agent_id", sa.String(length=160), nullable=False),
        sa.Column("executing_agent_id", sa.String(length=160), nullable=False),
        sa.Column("requested_profile", sa.String(length=80), nullable=False),
        sa.Column("effective_profile", sa.String(length=80)),
        sa.Column("model", sa.String(length=120)),
        sa.Column("provider", sa.String(length=80)),
        sa.Column("reasoning_effort", sa.String(length=20)),
        sa.Column("input_tokens", sa.Integer()),
        sa.Column("output_tokens", sa.Integer()),
        sa.Column("reasoning_tokens", sa.Integer()),
        sa.Column("cache_read_tokens", sa.Integer()),
        sa.Column("cache_write_tokens", sa.Integer()),
        sa.Column("api_calls", sa.Integer()),
        sa.Column("estimated_cost", sa.Numeric(20, 6)),
        sa.Column("actual_cost", sa.Numeric(20, 6)),
        sa.Column("currency", sa.String(length=8)),
        sa.Column("cost_status", sa.String(length=20), nullable=False),
        sa.Column("cost_source", sa.String(length=120)),
        sa.Column("quota_status", sa.String(length=30), nullable=False),
        sa.Column("quota_reset_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("outcome", sa.String(length=40), nullable=False),
        sa.Column("retry_number", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "operation_id",
        "task_id",
        "parent_run_id",
        "project_id",
        "category",
        "requesting_agent_id",
        "executing_agent_id",
        "requested_profile",
        "effective_profile",
        "model",
        "provider",
        "cost_status",
        "quota_status",
        "quota_reset_at",
        "finished_at",
        "outcome",
    ):
        op.create_index(f"ix_usage_ledger_{column}", "usage_ledger", [column])
    op.create_index("ix_usage_ledger_run_id", "usage_ledger", ["run_id"], unique=True)

    op.create_table(
        "budgets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("scope_type", sa.String(length=30), nullable=False),
        sa.Column("scope_key", sa.String(length=160), nullable=False),
        sa.Column("window_seconds", sa.Integer(), nullable=False),
        sa.Column("soft_token_limit", sa.Integer()),
        sa.Column("hard_token_limit", sa.Integer()),
        sa.Column("max_concurrent_runs", sa.Integer()),
        sa.Column("max_fan_out", sa.Integer()),
        sa.Column("max_retries", sa.Integer()),
        sa.Column("circuit_failure_threshold", sa.Integer()),
        sa.Column("circuit_cooldown_seconds", sa.Integer()),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scope_type", "scope_key", name="uq_budget_scope"),
    )
    op.create_index("ix_budgets_scope_type", "budgets", ["scope_type"])
    op.create_index("ix_budgets_scope_key", "budgets", ["scope_key"])
    op.create_index("ix_budgets_enabled", "budgets", ["enabled"])

    op.create_table(
        "circuit_breakers",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("worker_actor_id", sa.String(length=160), nullable=False),
        sa.Column("profile_id", sa.String(length=80), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("last_error_code", sa.String(length=100)),
        sa.Column("opened_at", sa.DateTime(timezone=True)),
        sa.Column("reset_eligible_at", sa.DateTime(timezone=True)),
        sa.Column("reset_by_actor_id", sa.String(length=160)),
        sa.Column("reset_reason", sa.Text()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("worker_actor_id", "profile_id", name="uq_circuit_worker_profile"),
    )
    op.create_index("ix_circuit_breakers_worker_actor_id", "circuit_breakers", ["worker_actor_id"])
    op.create_index("ix_circuit_breakers_profile_id", "circuit_breakers", ["profile_id"])
    op.create_index("ix_circuit_breakers_state", "circuit_breakers", ["state"])


def downgrade() -> None:
    op.drop_table("circuit_breakers")
    op.drop_table("budgets")
    op.drop_table("usage_ledger")
