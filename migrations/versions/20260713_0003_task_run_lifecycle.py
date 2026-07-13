"""Añade el ciclo durable de task, run, approval y artifact.

Revision ID: 20260713_0003
Revises: 20260713_0002
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0003"
down_revision: str | None = "20260713_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("parent_task_id", sa.Uuid()),
        sa.Column("workflow_ref", sa.String(length=160)),
        sa.Column("requester_actor_id", sa.String(length=160), nullable=False),
        sa.Column("assignee_actor_id", sa.String(length=160)),
        sa.Column("reviewer_actor_id", sa.String(length=160)),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("idempotency_key", sa.String(length=160), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("objective", sa.Text(), nullable=False),
        sa.Column("acceptance_criteria", sa.JSON(), nullable=False),
        sa.Column("budget", sa.JSON(), nullable=False),
        sa.Column("references", sa.JSON(), nullable=False),
        sa.Column("independent_review", sa.Boolean(), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True)),
        sa.Column("cancel_idempotency_key", sa.String(length=160)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["parent_task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_tasks_operation_id", "tasks", ["operation_id"])
    op.create_index("ix_tasks_parent_task_id", "tasks", ["parent_task_id"])
    op.create_index("ix_tasks_requester_actor_id", "tasks", ["requester_actor_id"])
    op.create_index("ix_tasks_assignee_actor_id", "tasks", ["assignee_actor_id"])
    op.create_index("ix_tasks_reviewer_actor_id", "tasks", ["reviewer_actor_id"])
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_idempotency_key", "tasks", ["idempotency_key"], unique=True)
    op.create_index(
        "ux_tasks_cancel_idempotency_key",
        "tasks",
        ["cancel_idempotency_key"],
        unique=True,
    )

    op.create_table(
        "task_dependencies",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("depends_on_task_id", sa.Uuid(), nullable=False),
        sa.Column("required_status", sa.String(length=40), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["depends_on_task_id"], ["tasks.id"]),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("task_id", "depends_on_task_id"),
    )

    op.create_table(
        "task_comments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("actor_id", sa.String(length=160), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=160), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_task_comments_task_id", "task_comments", ["task_id"])
    op.create_index("ix_task_comments_actor_id", "task_comments", ["actor_id"])
    op.create_index(
        "ix_task_comments_idempotency_key",
        "task_comments",
        ["idempotency_key"],
        unique=True,
    )

    op.create_table(
        "runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_actor_id", sa.String(length=160), nullable=False),
        sa.Column("requested_profile_id", sa.String(length=80), nullable=False),
        sa.Column("effective_profile_id", sa.String(length=80)),
        sa.Column("dispatch_idempotency_key", sa.String(length=160), nullable=False),
        sa.Column("dispatch_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("timeout_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("error_code", sa.String(length=100)),
        sa.Column("summary", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id", "attempt_number", name="uq_runs_task_attempt"),
    )
    op.create_index("ix_runs_task_id", "runs", ["task_id"])
    op.create_index("ix_runs_operation_id", "runs", ["operation_id"])
    op.create_index("ix_runs_worker_actor_id", "runs", ["worker_actor_id"])
    op.create_index("ix_runs_status", "runs", ["status"])
    op.create_index("ix_runs_timeout_at", "runs", ["timeout_at"])
    op.create_index(
        "ix_runs_dispatch_idempotency_key",
        "runs",
        ["dispatch_idempotency_key"],
        unique=True,
    )

    op.create_table(
        "approvals",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(length=100), nullable=False),
        sa.Column("context_snapshot", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("requested_by_actor_id", sa.String(length=160), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_by_actor_id", sa.String(length=160)),
        sa.Column("decision_reason", sa.Text()),
        sa.Column("decision_idempotency_key", sa.String(length=160)),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_approvals_run_id", "approvals", ["run_id"])
    op.create_index("ix_approvals_status", "approvals", ["status"])
    op.create_index("ix_approvals_expires_at", "approvals", ["expires_at"])
    op.create_index(
        "ux_approvals_decision_idempotency_key",
        "approvals",
        ["decision_idempotency_key"],
        unique=True,
    )

    op.create_table(
        "artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid()),
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("uri", sa.String(length=1000), nullable=False),
        sa.Column("sha256", sa.String(length=64)),
        sa.Column("producer_actor_id", sa.String(length=160), nullable=False),
        sa.Column("sensitivity", sa.String(length=40), nullable=False),
        sa.Column("validation", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_artifacts_task_id", "artifacts", ["task_id"])
    op.create_index("ix_artifacts_run_id", "artifacts", ["run_id"])
    op.create_index("ix_artifacts_producer_actor_id", "artifacts", ["producer_actor_id"])


def downgrade() -> None:
    op.drop_table("artifacts")
    op.drop_table("approvals")
    op.drop_table("runs")
    op.drop_table("task_comments")
    op.drop_table("task_dependencies")
    op.drop_table("tasks")
