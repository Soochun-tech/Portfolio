"""
Helper: log every asset materialization to `pipeline_run_log` so we can build
a monitoring dashboard later (Day 7 — pipeline health view).

Usage:
    with run_logged(mysql, asset_name="mart_taxi_daily_zone", partition="2024-01-15") as ctx:
        rows = ctx.run_query("...")
        ctx.set_rows_written(len(rows))
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import text


class RunContext:
    def __init__(self, conn, run_id: int):
        self.conn = conn
        self.run_id = run_id
        self.rows_written = 0

    def set_rows_written(self, n: int) -> None:
        self.rows_written = n


@contextmanager
def run_logged(mysql, asset_name: str, partition: str | None = None):
    """Insert a 'running' row, yield, then update with success/failure."""
    started_at = datetime.now()

    # 1. Mark start
    with mysql.connection() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO pipeline_run_log
                    (asset_name, partition_key, started_at, status)
                VALUES (:asset, :pkey, :started, 'running')
                """
            ),
            {"asset": asset_name, "pkey": partition, "started": started_at},
        )
        run_id = result.lastrowid

    # 2. Run user code with a fresh connection
    try:
        with mysql.connection() as conn:
            ctx = RunContext(conn=conn, run_id=run_id)
            yield ctx
    except Exception as e:
        # 3a. Mark failure
        with mysql.connection() as fconn:
            fconn.execute(
                text(
                    """
                    UPDATE pipeline_run_log
                       SET status = 'failed',
                           finished_at = :finished,
                           error_message = :err
                     WHERE run_id = :rid
                    """
                ),
                {
                    "finished": datetime.now(),
                    "err": str(e)[:1000],
                    "rid": run_id,
                },
            )
        raise
    else:
        # 3b. Mark success
        with mysql.connection() as fconn:
            fconn.execute(
                text(
                    """
                    UPDATE pipeline_run_log
                       SET status = 'success',
                           finished_at = :finished,
                           rows_written = :rows
                     WHERE run_id = :rid
                    """
                ),
                {
                    "finished": datetime.now(),
                    "rows": ctx.rows_written,
                    "rid": run_id,
                },
            )
