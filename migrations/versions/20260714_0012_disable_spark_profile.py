"""Deshabilita Spark para la policy Hermes Tradix v3.

Revision ID: 20260714_0012
Revises: 20260713_0011
Create Date: 2026-07-14
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260714_0012"
down_revision: str | None = "20260713_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("UPDATE execution_profiles SET enabled = FALSE WHERE id = 'spark-low'")


def downgrade() -> None:
    op.execute("UPDATE execution_profiles SET enabled = TRUE WHERE id = 'spark-low'")
