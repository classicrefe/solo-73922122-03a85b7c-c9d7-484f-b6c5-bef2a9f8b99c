"""Round registry: concurrency slots, state machine and worker supervision."""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from .config import (
    MAX_CONCURRENT_ROUNDS,
    MAX_FINISHED_ROUNDS,
    QUERY_TIMEOUT_SECONDS,
)
from .worker import (
    MSG_DONE,
    MSG_ERROR,
    MSG_TOO_LARGE,
    QueryWorker,
    WorkerResult,
)

# Public round states.
PENDING = "pending"        # accepted, worker thread starting
RUNNING = "running"        # query executing
SUCCEEDED = "succeeded"    # result published, downloadable
FAILED = "failed"          # SQL/runtime error, timeout or oversized result
CANCELLED = "cancelled"    # cancellation took effect after the worker exited

_TERMINAL_STATES = {SUCCEEDED, FAILED, CANCELLED}


class SlotsFullError(RuntimeError):
    """Raised when MAX_CONCURRENT_ROUNDS are already active."""


class RoundNotFound(KeyError):
    pass


@dataclass
class Round:
    round_id: str
    station_id: str
    sql: str
    state: str = PENDING
    error: Optional[str] = None
    result_csv: Optional[bytes] = None
    result_bytes: int = 0
    row_count: int = 0
    created_at: float = field(default_factory=time.monotonic)
    finished_at: Optional[float] = None
    cancel_requested: bool = False
    # Worker plumbing (not exposed in the API).
    _worker: Optional[QueryWorker] = None
    _supervisor: Optional[threading.Thread] = None
    _watchdog: Optional[threading.Timer] = None
    _timeout_happened: bool = False

    def public_dict(self) -> dict:
        data = {
            "round_id": self.round_id,
            "station_id": self.station_id,
            "state": self.state,
            "cancel_requested": self.cancel_requested,
            "elapsed_ms": int(
                ((self.finished_at or time.monotonic()) - self.created_at) * 1000
            ),
        }
        if self.state == SUCCEEDED:
            data.update(
                result_bytes=self.result_bytes,
                row_count=self.row_count,
                download_url=f"/api/rounds/{self.round_id}/result",
            )
        if self.error:
            data["error"] = self.error
        return data


class RoundRegistry:
    def __init__(self, max_active: int = MAX_CONCURRENT_ROUNDS) -> None:
        self._lock = threading.RLock()
        self._rounds: dict[str, Round] = {}
        self._max_active = max_active

    # ------------------------------------------------------------------ slots
    def active_count(self) -> int:
        with self._lock:
            return sum(
                1 for r in self._rounds.values() if r.state not in _TERMINAL_STATES
            )

    def submit(self, station_id: str, sql: str, columns: dict[str, list]) -> Round:
        with self._lock:
            if self.active_count() >= self._max_active:
                raise SlotsFullError(
                    f"already {self._max_active} rounds running; retry after one finishes"
                )
            round_id = uuid.uuid4().hex
            round_obj = Round(
                round_id=round_id,
                station_id=station_id,
                sql=sql,
            )
            worker = QueryWorker(round_id, sql, columns)
            watchdog = threading.Timer(
                QUERY_TIMEOUT_SECONDS, self._on_timeout, args=(round_id,)
            )
            watchdog.daemon = True
            supervisor = threading.Thread(
                target=self._supervise,
                args=(round_id,),
                name=f"supervisor-{round_id[:8]}",
                daemon=True,
            )
            round_obj._worker = worker
            round_obj._watchdog = watchdog
            round_obj._supervisor = supervisor
            self._rounds[round_id] = round_obj

            worker.start()
            watchdog.start()
            supervisor.start()
            round_obj.state = RUNNING
            self._evict_finished_locked()
            return round_obj

    # -------------------------------------------------------------- supervision
    def _on_timeout(self, round_id: str) -> None:
        with self._lock:
            round_obj = self._rounds.get(round_id)
            if round_obj is None or round_obj.state in _TERMINAL_STATES:
                return
            round_obj._timeout_happened = True
            worker = round_obj._worker
        # DuckDB interrupt aborts the in-flight query immediately.
        if worker is not None:
            worker.interrupt("timeout")

    def _supervise(self, round_id: str) -> None:
        with self._lock:
            round_obj = self._rounds[round_id]
            worker = round_obj._worker
            watchdog = round_obj._watchdog

        # Wait for the worker thread to actually exit. This is what makes the
        # "cancelled" state truthful: it is only published after real exit,
        # which also releases the concurrency slot.
        worker.join()
        watchdog.cancel()
        worker_result: Optional[WorkerResult] = worker.result

        with self._lock:
            self._finalize(round_obj, worker_result)

    def _finalize(self, round_obj: Round, worker_result: Optional[WorkerResult]) -> None:
        with self._lock:
            # Already finalised (should not happen, guard anyway).
            if round_obj.state in _TERMINAL_STATES:
                return

            # Cancellation requested while active always wins once the worker
            # has really exited, even if a result was produced at the same
            # instant: a late result must never be opened.
            if round_obj.cancel_requested:
                round_obj.state = CANCELLED
                round_obj.error = "round cancelled; result discarded"
                round_obj.result_csv = None
            elif round_obj._timeout_happened:
                round_obj.state = FAILED
                round_obj.error = (
                    f"query exceeded {QUERY_TIMEOUT_SECONDS:.0f} s and was terminated; "
                    "no partial results are available"
                )
                round_obj.result_csv = None
            elif worker_result is None:
                round_obj.state = FAILED
                round_obj.error = "query worker exited without producing a result"
                round_obj.result_csv = None
            elif worker_result.kind == MSG_DONE:
                round_obj.state = SUCCEEDED
                round_obj.result_csv = worker_result.csv_bytes
                round_obj.result_bytes = worker_result.result_bytes
                round_obj.row_count = worker_result.row_count
                round_obj.error = None
            elif worker_result.kind == MSG_TOO_LARGE:
                round_obj.state = FAILED
                round_obj.error = worker_result.error
                round_obj.result_csv = None
            else:
                round_obj.state = FAILED
                round_obj.error = worker_result.error or "query failed"
                round_obj.result_csv = None

            round_obj.finished_at = time.monotonic()
            round_obj._worker = None
            round_obj._watchdog = None

    # ------------------------------------------------------------------ cancel
    def request_cancel(self, round_id: str) -> tuple[bool, str]:
        """Request cancellation.

        Acceptance does not wait for the worker to exit; the round becomes
        ``cancelled`` (and releases its slot) only after the worker really
        terminates. A round that already published a result cannot be cancelled.
        """
        with self._lock:
            round_obj = self._get_locked(round_id)
            if round_obj.state == SUCCEEDED:
                return False, "round already succeeded and published a result; cannot cancel"
            if round_obj.state in _TERMINAL_STATES:
                return False, f"round is already in terminal state '{round_obj.state}'"
            if round_obj.cancel_requested:
                return True, "cancellation already requested; waiting for worker exit"
            round_obj.cancel_requested = True
            worker = round_obj._worker
        if worker is not None:
            worker.interrupt("cancel")
        return True, (
            "cancellation accepted; round will be marked cancelled once the worker exits"
        )

    # ------------------------------------------------------------------- reads
    def get(self, round_id: str) -> Round:
        with self._lock:
            return self._get_locked(round_id)

    def _get_locked(self, round_id: str) -> Round:
        try:
            return self._rounds[round_id]
        except KeyError:
            raise RoundNotFound(round_id) from None

    def list_rounds(self) -> list[dict]:
        with self._lock:
            return [r.public_dict() for r in self._rounds.values()]

    def snapshot(self, round_id: str) -> dict:
        return self.get(round_id).public_dict()

    # ----------------------------------------------------------------- helpers
    def _evict_finished_locked(self) -> None:
        finished = [
            r for r in self._rounds.values() if r.state in _TERMINAL_STATES
        ]
        if len(finished) <= MAX_FINISHED_ROUNDS:
            return
        finished.sort(key=lambda r: r.finished_at or 0.0)
        for round_obj in finished[: len(finished) - MAX_FINISHED_ROUNDS]:
            self._rounds.pop(round_obj.round_id, None)

    def shutdown(self) -> None:
        with self._lock:
            items = [
                (r._worker, r._watchdog)
                for r in self._rounds.values()
                if r.state not in _TERMINAL_STATES
            ]
        for worker, watchdog in items:
            try:
                if watchdog is not None:
                    watchdog.cancel()
            except Exception:
                pass
            try:
                if worker is not None:
                    worker.interrupt("shutdown")
            except Exception:
                pass
        for worker, _ in items:
            try:
                if worker is not None:
                    worker.join(timeout=3.0)
            except Exception:
                pass
