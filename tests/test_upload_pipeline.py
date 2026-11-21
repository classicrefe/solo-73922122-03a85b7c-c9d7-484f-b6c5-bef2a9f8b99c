"""Unit tests for the off-loop upload pipeline and batch swap semantics.

Covers the ordering rule "新旧按进入校验的先后判定":

* every validated file gets a sequence in arrival order;
* only a valid file newer than the current batch swaps in;
* a newer-but-invalid file leaves the current batch untouched;
* an older valid file still swaps in when no newer valid batch is live;
* a file that finishes after a newer batch was published is reported stale;
* validation runs on worker threads (the calling thread is never the worker).
"""
from __future__ import annotations

import threading

import pytest

from app.store import DataStore, build_dataset
from app.upload_pipeline import (
    REJECTED,
    STALE,
    SWAPPED,
    UploadPipeline,
)

VALID_A = (
    "站号,UTC时刻,温度\n"
    "A,2026-01-01T00:00Z,1.0\n"
    "A,2026-01-01T01:00Z,2.0\n"
    "B,2026-01-01T00:00Z,3.0\n"
).encode()

VALID_B = (
    "站号,UTC时刻,温度\n"
    "C,2026-01-01T00:00Z,4.0\n"
    "C,2026-01-01T01:00Z,5.0\n"
    "D,2026-01-01T00:00Z,6.0\n"
    "D,2026-01-01T01:00Z,7.0\n"
).encode()

INVALID = "站号,UTC时刻,温度\nA,t,not-a-number\n".encode()


@pytest.fixture()
def pipeline():
    store = DataStore()
    pipe = UploadPipeline(store, max_workers=4)
    yield pipe, store
    pipe.shutdown()


def test_first_valid_upload_swaps_and_carries_stations_and_rows(pipeline):
    pipe, store = pipeline
    result = pipe.submit(VALID_A, "a.csv").result(timeout=5)
    assert result.outcome == SWAPPED
    assert result.sequence == 1
    current = store.current()
    assert current is not None
    assert current.sequence == 1
    assert current.filename == "a.csv"
    assert current.total_rows == 3
    assert current.stations == ["A", "B"]
    assert current.station_counts == {"A": 2, "B": 1}


def test_newer_valid_batch_replaces_older_one(pipeline):
    pipe, store = pipeline
    first = pipe.submit(VALID_A, "a.csv").result(timeout=5)
    second = pipe.submit(VALID_B, "b.csv").result(timeout=5)
    assert first.outcome == SWAPPED and second.outcome == SWAPPED
    assert second.sequence == 2
    current = store.current()
    assert current.sequence == 2
    assert current.stations == ["C", "D"]
    assert current.total_rows == 4


def test_invalid_upload_is_rejected_and_keeps_current_batch(pipeline):
    pipe, store = pipeline
    pipe.submit(VALID_A, "a.csv").result(timeout=5)
    result = pipe.submit(INVALID, "bad.csv").result(timeout=5)
    assert result.outcome == REJECTED
    assert result.sequence == 2
    assert "温度" in (result.error or "")
    current = store.current()
    assert current.sequence == 1
    assert current.filename == "a.csv"


def test_newer_invalid_then_older_valid_older_still_swaps_in(pipeline):
    """Newer file (#2) fails validation; the older valid one (#1) still swaps."""
    pipe, store = pipeline
    # #1 is held before validation, #2 runs first and fails.
    gate1 = threading.Event()
    release1 = threading.Event()
    future1 = pipe.submit_controlled(VALID_A, "a.csv", gate1, release1)
    invalid = pipe.submit(INVALID, "bad.csv").result(timeout=5)
    assert invalid.outcome == REJECTED
    assert store.current() is None  # nothing published yet

    gate1.set()
    release1.set()
    first = future1.result(timeout=5)
    assert first.outcome == SWAPPED
    assert store.current().sequence == 1
    assert store.current().stations == ["A", "B"]


def test_older_valid_file_finishing_after_newer_batch_is_stale(pipeline):
    """#1 valid but slow; #2 valid publishes first; #1 must not overwrite it."""
    pipe, store = pipeline
    gate1 = threading.Event()
    release1 = threading.Event()
    future1 = pipe.submit_controlled(VALID_A, "a.csv", gate1, release1)

    second = pipe.submit(VALID_B, "b.csv").result(timeout=5)
    assert second.outcome == SWAPPED
    assert store.current().sequence == 2

    gate1.set()
    release1.set()
    first = future1.result(timeout=5)
    assert first.outcome == STALE
    assert first.sequence == 1
    assert first.current_sequence == 2
    # The newer batch survives; stations/rows were not rolled back.
    current = store.current()
    assert current.sequence == 2
    assert current.stations == ["C", "D"]
    assert current.total_rows == 4


def test_validation_runs_off_the_calling_thread(pipeline, monkeypatch):
    pipe, store = pipeline
    seen = []
    import app.upload_pipeline as mod

    real_parse = mod.parse_observations_csv

    def recording_parse(raw):
        seen.append(threading.current_thread().name)
        return real_parse(raw)

    monkeypatch.setattr(mod, "parse_observations_csv", recording_parse)
    pipe.submit(VALID_A, "a.csv").result(timeout=5)
    assert seen and seen[0].startswith("upload-validate")
    assert seen[0] != threading.current_thread().name


def test_swap_is_atomic_for_stations_and_rows(pipeline):
    pipe, store = pipeline
    pipe.submit(VALID_A, "a.csv").result(timeout=5)
    pipe.submit(VALID_B, "b.csv").result(timeout=5)
    dataset = store.current()
    # Every station of the published batch belongs to the same sequence.
    assert set(dataset.by_station) == {"C", "D"}
    assert sum(dataset.station_counts.values()) == dataset.total_rows
    assert all(
        len(cols["station_id"]) == dataset.station_counts[station]
        for station, cols in dataset.by_station.items()
    )


def test_store_direct_compare_and_swap():
    from app.csv_io import parse_observations_csv

    store = DataStore()
    d1 = build_dataset(parse_observations_csv(VALID_A), "a.csv", 1)
    d2 = build_dataset(parse_observations_csv(VALID_B), "b.csv", 2)
    assert store.swap_if_newer(d1) is True
    assert store.swap_if_newer(d1) is False  # equal sequence rejected
    assert store.swap_if_newer(d2) is True
    assert store.swap_if_newer(d1) is False  # older sequence rejected
    assert store.current().sequence == 2
