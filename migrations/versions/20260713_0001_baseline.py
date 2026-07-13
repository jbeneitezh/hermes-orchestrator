"""Establece la revisión baseline del plano de control.

Revision ID: 20260713_0001
Revises:
Create Date: 2026-07-13
"""

from collections.abc import Sequence

revision: str = "20260713_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Registra el baseline sin adelantar tablas de dominio de F6."""


def downgrade() -> None:
    """Retira el baseline; Alembic gestiona su propia tabla de versión."""
