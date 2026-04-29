"""
Dagster resources — shared infra (DB connection, etc).

We expose a `mysql` resource that asset functions can request via:

    @asset(...)
    def my_asset(mysql: MySQLResource):
        with mysql.connection() as conn:
            ...
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

from dagster import ConfigurableResource
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class MySQLResource(ConfigurableResource):
    """SQLAlchemy engine wrapper that reads creds from .env."""

    user: str = "analyst"
    password: str = "analystpw"
    host: str = "localhost"
    port: int = 3306
    database: str = "portfolio"

    def _engine(self) -> Engine:
        url = (
            f"mysql+mysqlconnector://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}?charset=utf8mb4"
        )
        return create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"allow_local_infile": True},
        )

    @contextmanager
    def connection(self):
        """Yield a SQLAlchemy connection (begins a transaction, commits on exit)."""
        engine = self._engine()
        with engine.begin() as conn:
            yield conn
        engine.dispose()

    def execute(self, sql: str, params: dict | None = None):
        """Run a single statement and return the result (for one-shot calls)."""
        with self.connection() as conn:
            return conn.execute(text(sql), params or {})


def make_mysql_resource() -> MySQLResource:
    """Factory that loads .env and returns a configured MySQLResource."""
    load_dotenv(PROJECT_ROOT / ".env")
    return MySQLResource(
        user=os.environ.get("MYSQL_USER", "analyst"),
        password=os.environ.get("MYSQL_PASSWORD", "analystpw"),
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        database=os.environ.get("MYSQL_DATABASE", "portfolio"),
    )
