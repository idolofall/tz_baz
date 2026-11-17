"""add processing status and webhook delivery columns

Revision ID: 002
Revises: 001
Create Date: 2026-11-17 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE payment_status_enum ADD VALUE IF NOT EXISTS 'processing'")
    op.add_column(
        "payments",
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("payments", "webhook_delivered_at")
    op.drop_column("payments", "processing_started_at")
