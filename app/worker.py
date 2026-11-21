"""Sandboxed query worker executed in a dedicated thread per round.

Isolation model
---------------
* Every round gets its own thread, its own in-memory DuckDB connection and
  its own copy of the selected station's rows. The only relation visible to
  user SQL is the ``observations`` table of that connection; connections of
  other rounds are unreachable, so cross-round data access is impossible.
* Every connection is created with ``enable_external_access=false`` which
  disables file system, network and extension access inside DuckDB.
* The static SQL guard (``app.sql_guard``) rejects anything but a single
  read-only SELECT/WITH statement before execution.

Cancellation / timeout
----------------------
DuckDB's native ``connection.interrupt()`` is used to abort a running query
immediately; the worker surfaces it as an ordinary failure and no partial
output is ever published. A watchdog additionally interrupts at the 5 s
deadline so a slow query cannot block anything else.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Optional

import duckdb

from .config import (
    BATCH_SIZE,
    MAX_RESULT_BYTES,
    TABLE_COLUMNS,
    TABLE_NAME,
)
from .sql_guard import SqlRejected, validate_sql

# Outcome markers used by the supervisor.
MSG_DONE = "done"
MSG_ERROR = "error"
MSG_TOO_LARGE = "too_large"


@dataclass
class WorkerResult:
    kind: str
    csv_bytes: bytes = b""
    row_count: int = 0
    error: str = ""
    result_bytes: int = 0


def _locked_connection() -> duckdb.DuckDBPyConnection:
    # enable_external_access=false blocks file system, network and extension
    # access inside DuckDB itself (second layer behind the static SQL guard).
    return duckdb.connect(
        ":memory:",
        config={
            "enable_external_access": False,
            "allow_unsigned_extensions": False,
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        },
    )


def _build_relation(con: duckdb.DuckDBPyConnection, columns: dict[str, list]) -> None:
    con.execute(
        f"""
        CREATE TABLE {TABLE_NAME} (
            {TABLE_COLUMNS[0]} VARCHAR,
            {TABLE_COLUMNS[1]} VARCHAR,
            {TABLE_COLUMNS[2]} DOUBLE
        )
        """
    )
    rows = list(
        zip(
            columns[TABLE_COLUMNS[0]],
            columns[TABLE_COLUMNS[1]],
            columns[TABLE_COLUMNS[2]],
        )
    )
    con.executemany(
        f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?)", rows
    )


def _csv_quote(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if any(ch in text for ch in (",", '"', "\n", "\r")):
        text = '"' + text.replace('"', '""') + '"'
    return text


def _csv_header(columns: list[str]) -> bytes:
    return (",".join(_csv_quote(c) for c in columns) + "\n").encode("utf-8")


def _format_csv(columns: list[str], rows: list[tuple]) -> bytes:
    out = []
    for row in rows:
        out.append(",".join(_csv_quote(v) for v in row))
    return ("\n".join(out) + ("\n" if rows else "")).encode("utf-8")


class QueryWorker(threading.Thread):
    """Runs one validated query to completion or interruption."""

    def __init__(self, round_id: str, sql: str, columns: dict[str, list]) -> None:
        super().__init__(name=f"query-{round_id[:8]}", daemon=True)
        self.round_id = round_id
        self._sql = sql
        self._columns = columns
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._interrupted = threading.Event()
        self._lock = threading.Lock()
        self.result: Optional[WorkerResult] = None

    # ---------------------------------------------------------- cancellation
    def interrupt(self, reason: str) -> None:
        """Ask DuckDB to abort the running query (safe from another thread)."""
        self._interrupted.set()
        with self._lock:
            if self._con is not None:
                try:
                    self._con.interrupt()
                except Exception:
                    # The connection may already be closed; nothing to do.
                    pass

    def is_interrupted(self) -> bool:
        return self._interrupted.is_set()

    # ------------------------------------------------------------------ main
    def run(self) -> None:
        try:
            try:
                validated = validate_sql(self._sql)
            except SqlRejected as exc:
                self.result = WorkerResult(
                    MSG_ERROR, error=f"SQL rejected: {exc}"
                )
                return

            con = _locked_connection()
            with self._lock:
                # If cancellation arrived before the connection existed, stop now.
                if self._interrupted.is_set():
                    con.close()
                    self.result = WorkerResult(
                        MSG_ERROR, error="cancelled before execution started"
                    )
                    return
                self._con = con

            try:
                self._execute(validated.text, con)
            finally:
                with self._lock:
                    self._con = None
                try:
                    con.close()
                except Exception:
                    pass
        except Exception as exc:  # noqa: BLE001 - report every failure textually
            if self._interrupted.is_set():
                self.result = WorkerResult(
                    MSG_ERROR, error="query was interrupted"
                )
            else:
                self.result = WorkerResult(
                    MSG_ERROR,
                    error=f"{type(exc).__name__}: {str(exc)[:300]}",
                )

    def _execute(self, sql: str, con: duckdb.DuckDBPyConnection) -> None:
        _build_relation(con, self._columns)

        cur = con.execute(sql)
        description = cur.description or []
        result_columns = [d[0] for d in description]
        if not result_columns:
            self.result = WorkerResult(
                MSG_ERROR, error="query did not return a result set"
            )
            return

        chunks: list[bytes] = [_csv_header(result_columns)]
        total_bytes = len(chunks[0])
        total_rows = 0

        while True:
            if self._interrupted.is_set():
                self.result = WorkerResult(
                    MSG_ERROR, error="query was interrupted; no partial result"
                )
                return
            batch = cur.fetchmany(BATCH_SIZE)
            if not batch:
                break
            chunk = _format_csv(result_columns, batch)
            total_bytes += len(chunk)
            total_rows += len(batch)
            if total_bytes > MAX_RESULT_BYTES:
                # Whole round fails; accumulated chunks are discarded.
                self.result = WorkerResult(
                    MSG_TOO_LARGE,
                    error=(
                        f"result CSV exceeds {MAX_RESULT_BYTES} bytes "
                        "(8 MiB); the whole round failed and no result is kept"
                    ),
                    result_bytes=total_bytes,
                )
                return
            chunks.append(chunk)

        self.result = WorkerResult(
            MSG_DONE,
            csv_bytes=b"".join(chunks),
            row_count=total_rows,
            result_bytes=total_bytes,
        )
