"""Track webhook delivery separately from payment status."""

import sqlalchemy as sa
from alembic import op

revision = "0002_add_webhook_sent_at"
down_revision = "0001_create_payments_and_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("webhook_sent_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("payments", "webhook_sent_at")
