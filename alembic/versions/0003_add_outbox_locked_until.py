"""Lease outbox rows so publishing happens outside the claiming transaction."""

import sqlalchemy as sa
from alembic import op

revision = "0003_add_outbox_locked_until"
down_revision = "0002_add_webhook_sent_at"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("outbox", sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("outbox", "locked_until")
