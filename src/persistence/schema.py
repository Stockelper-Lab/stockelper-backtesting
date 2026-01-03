from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()


backtest_jobs = Table(
    "backtest_jobs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer, nullable=False, index=True),
    Column("stock_ticker", String(32), nullable=True, index=True),
    Column("strategy_type", String(64), nullable=True),
    Column("status", String(16), nullable=False, index=True),  # pending|in_progress|completed|failed
    Column("input_json", JSONB, nullable=True),
    Column("error_message", Text, nullable=True),
    Column("retry_count", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
)


backtest_results = Table(
    "backtest_results",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("job_id", Integer, ForeignKey("backtest_jobs.id", ondelete="CASCADE"), nullable=False, index=True),
    Column("user_id", Integer, nullable=False, index=True),
    Column("stock_ticker", String(32), nullable=True, index=True),
    Column("strategy_type", String(64), nullable=True),
    Column("results_json", JSONB, nullable=False),
    Column("generated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)


notifications = Table(
    "notifications",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer, nullable=False, index=True),
    Column("type", String(32), nullable=False, index=True),  # portfolio_recommendation|backtesting_complete|event_alert|...
    Column("title", Text, nullable=False),
    Column("message", Text, nullable=False),
    Column("data", JSONB, nullable=True),
    Column("is_read", Boolean, nullable=False, server_default="false", index=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("read_at", DateTime(timezone=True), nullable=True),
)


