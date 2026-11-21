"""Asynchronous upload pipeline: validation and station splitting off-loop.

Why this module exists
----------------------
CSV parsing and per-station splitting are CPU-bound and can take a while once
a file covers many stations. Running them inside the async upload endpoint
would block the event loop, so round status polling and downloads of already
published results would stop responding for the whole validation window.

Instead, every upload:

1. Gets a strictly increasing **sequence number** at the moment it enters the
   pipeline, i.e. in validation arrival order ("按文件进入校验的先后判定新旧").
2. Is parsed, validated and split into per-station copies inside a bounded
   thread-pool worker, keeping the event loop (and therefore every HTTP
   endpoint) responsive.
3. On success, is published via :meth:`DataStore.swap_if_newer`: only a batch
   whose sequence is newer than the currently published one can be swapped in,
   and stations/rows are replaced together atomically.

Out-of-order completion therefore has the exact required semantics:

* a newer file that fails validation does not touch the current batch;
* an older valid file still swaps in if no newer valid file has been
  published yet (``swap_if_newer`` compares against the live batch);
* a stale file that finishes after a newer batch was already published is
  reported as "already-updated batch" and discarded.

Rounds that were already accepted hold their own copy of the station columns,
so a swap never changes the input of in-flight or finished SQL.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

from .config import MAX_UPLOAD_WORKERS
from .csv_io import CsvError, ParsedCsv, parse_observations_csv
from .store import DataStore, Dataset, build_dataset

# Public upload outcomes, surfaced verbatim by the API and web UI.
SWAPPED = "swapped"          # 本次换批成功
STALE = "stale"              # 已有更新批次，本文件未换入
REJECTED = "rejected"        # 校验失败，当前批次保持不变


@dataclass
class UploadResult:
    outcome: str
    filename: str
    sequence: int
    # Present only for outcome == SWAPPED.
    dataset: Optional[Dataset] = None
    # Present only for outcome == STALE.
    current_sequence: Optional[int] = None
    # Present only for outcome == REJECTED.
    error: Optional[str] = None
    message: str = ""

    def public_dict(self) -> dict:
        data = {
            "outcome": self.outcome,
            "filename": self.filename,
            "sequence": self.sequence,
            "message": self.message,
        }
        if self.dataset is not None:
            data.update(
                total_rows=self.dataset.total_rows,
                stations=self.dataset.stations,
                station_counts=self.dataset.station_counts,
            )
        if self.current_sequence is not None:
            data["current_sequence"] = self.current_sequence
        if self.error is not None:
            data["error"] = self.error
        return data


@dataclass
class _Job:
    sequence: int
    filename: str
    raw: bytes
    validate_event: Optional[threading.Event] = field(default=None)
    release_event: Optional[threading.Event] = field(default=None)


class UploadPipeline:
    """Accepts uploads and validates them on worker threads."""

    def __init__(self, store: DataStore, max_workers: int = MAX_UPLOAD_WORKERS) -> None:
        self._store = store
        self._max_workers = max_workers
        self._executor: Optional[ThreadPoolExecutor] = None
        self._executor_lock = threading.Lock()
        self._seq_lock = threading.Lock()
        self._next_sequence = 1

    def _ensure_executor(self) -> ThreadPoolExecutor:
        # Lazily (re)created so a lifespan shutdown at process teardown, or a
        # test that restarts the app, leaves a working pipeline behind.
        with self._executor_lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix="upload-validate",
                )
            return self._executor

    # ------------------------------------------------------------- submission
    def submit(self, raw: bytes, filename: str):
        """Reserve arrival order and return a future resolving to a result."""
        with self._seq_lock:
            sequence = self._next_sequence
            self._next_sequence += 1
        job = _Job(sequence=sequence, filename=filename, raw=raw)
        return self._ensure_executor().submit(self._process, job)

    # Test/barrier hook: validate only after ``validate_event`` is set, and
    # hold the finished job until ``release_event`` is set. Used to create
    # deterministic out-of-order validation scenarios.
    def submit_controlled(
        self,
        raw: bytes,
        filename: str,
        validate_event: threading.Event,
        release_event: threading.Event,
    ):
        with self._seq_lock:
            sequence = self._next_sequence
            self._next_sequence += 1
        job = _Job(
            sequence=sequence,
            filename=filename,
            raw=raw,
            validate_event=validate_event,
            release_event=release_event,
        )
        return self._ensure_executor().submit(self._process, job)

    # --------------------------------------------------------------- worker
    def _process(self, job: _Job) -> UploadResult:
        if job.validate_event is not None:
            job.validate_event.wait()
        try:
            parsed: ParsedCsv = parse_observations_csv(job.raw)
        except CsvError as exc:
            result = UploadResult(
                outcome=REJECTED,
                filename=job.filename,
                sequence=job.sequence,
                error=str(exc),
                message=f"校验失败：{exc}；当前批次保持不变",
            )
        else:
            # Splitting happens on the worker thread as well, so the event
            # loop never pays the per-station copy cost.
            dataset = build_dataset(parsed, job.filename, job.sequence)
            if self._store.swap_if_newer(dataset):
                result = UploadResult(
                    outcome=SWAPPED,
                    filename=job.filename,
                    sequence=job.sequence,
                    dataset=dataset,
                    message=(
                        f"本次换批成功：{job.filename}（批次 #{job.sequence}），"
                        f"共 {dataset.total_rows} 行、{len(dataset.stations)} 个站点；"
                        "已受理的 SQL 继续使用其原始输入"
                    ),
                )
            else:
                current = self._store.current()
                current_sequence = current.sequence if current is not None else None
                result = UploadResult(
                    outcome=STALE,
                    filename=job.filename,
                    sequence=job.sequence,
                    current_sequence=current_sequence,
                    message=(
                        f"已有更新批次（当前 #{current_sequence}），"
                        f"{job.filename}（批次 #{job.sequence}）未换入，当前数据保持不变"
                    ),
                )
        if job.release_event is not None:
            job.release_event.wait()
        return result

    # ------------------------------------------------------------- lifecycle
    def shutdown(self) -> None:
        # Pending validations are cancelled; running ones finish quickly
        # (uploads are capped at 1 MiB). No event-loop thread is involved.
        with self._executor_lock:
            executor = self._executor
            self._executor = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
