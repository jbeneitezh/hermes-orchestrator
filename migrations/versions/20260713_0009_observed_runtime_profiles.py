"""Persiste runtime solicitado, observado y fallback por Run.

Revision ID: 20260713_0009
Revises: 20260713_0008
Create Date: 2026-07-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260713_0009"
down_revision: str | None = "20260713_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUNTIME_PROFILE_IDS = ("spark-low", "luna-low", "terra-medium", "sol-high")


def _set_runtime_profile_provider(provider: str, expected_current: str) -> None:
    profiles = sa.table(
        "execution_profiles",
        sa.column("id", sa.String()),
        sa.column("provider", sa.String()),
    )
    op.execute(
        profiles.update()
        .where(profiles.c.id.in_(RUNTIME_PROFILE_IDS))
        .where(profiles.c.provider == expected_current)
        .values(provider=provider)
    )


def upgrade() -> None:
    # Los workers consumen el OAuth compartido a través de la fachada
    # OpenAI-compatible del broker; Hermes observa `openai-api` en cada Run.
    _set_runtime_profile_provider("openai-api", "openai-codex")

    for name in ("requested_runtime", "observed_runtime", "runtime_fallback"):
        op.add_column(
            "runs",
            sa.Column(
                name,
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'{}'"),
            ),
        )
        op.alter_column("runs", name, server_default=None)

    op.add_column("usage_ledger", sa.Column("requested_model", sa.String(length=120)))
    op.add_column("usage_ledger", sa.Column("requested_provider", sa.String(length=80)))
    op.add_column(
        "usage_ledger",
        sa.Column("requested_reasoning_effort", sa.String(length=20)),
    )
    op.add_column(
        "usage_ledger",
        sa.Column(
            "runtime_fallback",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    )
    op.alter_column("usage_ledger", "runtime_fallback", server_default=None)


def downgrade() -> None:
    op.drop_column("usage_ledger", "runtime_fallback")
    op.drop_column("usage_ledger", "requested_reasoning_effort")
    op.drop_column("usage_ledger", "requested_provider")
    op.drop_column("usage_ledger", "requested_model")
    op.drop_column("runs", "runtime_fallback")
    op.drop_column("runs", "observed_runtime")
    op.drop_column("runs", "requested_runtime")
    _set_runtime_profile_provider("openai-codex", "openai-api")
