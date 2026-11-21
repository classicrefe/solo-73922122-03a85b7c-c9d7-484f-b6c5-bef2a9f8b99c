"""End-to-end API tests covering concurrency, timeout, cancel and isolation."""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app, registry, store, upload_pipeline
from app.registry import CANCELLED, FAILED, RUNNING, SUCCEEDED

SAMPLE = Path(__file__).resolve().parent.parent / "sample_data" / "observations.csv"


@pytest.fixture()
def client():
    # Each test starts from an empty registry and no uploaded dataset.
    registry._rounds.clear()
    store._dataset = None
    upload_pipeline.shutdown()
    with upload_pipeline._seq_lock:
        upload_pipeline._next_sequence = 1
    with TestClient(app) as c:
        yield c
    registry.shutdown()
    registry._rounds.clear()
    store._dataset = None
    upload_pipeline.shutdown()


def _upload(client):
    with open(SAMPLE, "rb") as fh:
        resp = client.post(
            "/api/upload",
            files={"file": ("observations.csv", fh, "text/csv")},
        )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _wait_terminal(client, round_id, timeout=15.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.get(f"/api/rounds/{round_id}").json()
        if last["state"] in (SUCCEEDED, FAILED, CANCELLED):
            return last
        time.sleep(0.05)
    raise AssertionError(f"round did not finish: {last}")


def test_health_and_upload(client):
    health = client.get("/api/health").json()
    assert health["max_concurrent_rounds"] == 2
    assert health["dataset_loaded"] is False
    data = _upload(client)
    assert data["total_rows"] == 9
    assert data["stations"] == ["ST001", "ST002", "ST003"]
    assert data["station_counts"]["ST001"] == 5


def test_successful_select_and_download(client):
    _upload(client)
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST001",
              "sql": "SELECT station_id, COUNT(*) AS n, ROUND(AVG(temperature_c),2) AS avg_t "
                     "FROM observations GROUP BY station_id ORDER BY station_id"},
    )
    assert resp.status_code == 201, resp.text
    round_id = resp.json()["round_id"]
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED
    assert final["row_count"] == 1

    dl = client.get(f"/api/rounds/{round_id}/result")
    assert dl.status_code == 200
    assert dl.headers["content-type"].startswith("text/csv")
    body = dl.text
    assert "station_id" in body and "avg_t" in body and "ST001" in body
    # Header attachment is present.
    assert "attachment" in dl.headers["content-disposition"]


def test_data_is_scoped_to_selected_station(client):
    _upload(client)
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST003",
              "sql": "SELECT COUNT(*) AS n, COUNT(DISTINCT station_id) AS stations "
                     "FROM observations"},
    )
    round_id = resp.json()["round_id"]
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED
    dl = client.get(f"/api/rounds/{round_id}/result").text
    lines = [ln for ln in dl.strip().splitlines() if ln]
    assert lines[0] == "n,stations", lines
    assert lines[1].startswith("1,1"), lines  # one row, one distinct station


def test_rejected_sql_returns_400(client):
    _upload(client)
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "DROP TABLE observations"},
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"].lower()
    assert "select" in detail or "rejected" in detail or "drop" in detail


def test_file_access_blocked_at_duckdb_layer(client):
    _upload(client)
    # Static guard catches this first.
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST001",
              "sql": "SELECT * FROM read_csv_auto('/etc/passwd')"},
    )
    assert resp.status_code == 400


def test_unknown_station_404_and_no_upload_409(client):
    resp = client.post(
        "/api/rounds",
        json={"station_id": "NOPE", "sql": "SELECT 1"},
    )
    assert resp.status_code == 409
    assert "尚未上传" in resp.json()["detail"]
    _upload(client)
    resp = client.post(
        "/api/rounds",
        json={"station_id": "NOPE", "sql": "SELECT 1"},
    )
    assert resp.status_code == 404


def test_two_parallel_rounds_and_third_rejected_503(client):
    _upload(client)
    slow_sql = ("WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 5000000) "
                "SELECT count(*) FROM seq")
    ids = []
    for _ in range(2):
        resp = client.post("/api/rounds", json={"station_id": "ST001", "sql": slow_sql})
        assert resp.status_code == 201, resp.text
        ids.append(resp.json()["round_id"])
    # Slots full.
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "SELECT * FROM observations"},
    )
    assert resp.status_code == 503
    assert "rounds running" in resp.json()["detail"]

    # Status queries remain responsive while queries run.
    status = client.get("/api/rounds").json()
    assert status["active"] == 2

    for rid in ids:
        final = _wait_terminal(client, rid)
        assert final["state"] in (SUCCEEDED, FAILED)
    # Slot released -> new submission accepted again.
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "SELECT count(*) FROM observations"},
    )
    assert resp.status_code == 201
    final = _wait_terminal(client, resp.json()["round_id"])
    assert final["state"] == SUCCEEDED


def test_timeout_terminates_within_about_five_seconds_no_partial(client):
    _upload(client)
    infinite_sql = ("WITH RECURSIVE seq(x) AS ("
                    "SELECT 1 UNION ALL SELECT x+1 FROM seq) "
                    "SELECT count(*) FROM seq")
    start = time.monotonic()
    resp = client.post("/api/rounds", json={"station_id": "ST001", "sql": infinite_sql})
    round_id = resp.json()["round_id"]
    final = _wait_terminal(client, round_id, timeout=20.0)
    elapsed = time.monotonic() - start
    assert final["state"] == FAILED
    assert "5" in final["error"] and "terminated" in final["error"]
    assert elapsed < 8.0, f"timeout took too long: {elapsed:.1f}s"
    # No partial result is downloadable.
    dl = client.get(f"/api/rounds/{round_id}/result")
    assert dl.status_code == 409
    # Slot is released.
    assert client.get("/api/rounds").json()["active"] == 0


def test_cancel_running_round(client):
    _upload(client)
    infinite_sql = ("WITH RECURSIVE seq(x) AS ("
                    "SELECT 1 UNION ALL SELECT x+1 FROM seq) "
                    "SELECT count(*) FROM seq")
    resp = client.post("/api/rounds", json={"station_id": "ST001", "sql": infinite_sql})
    round_id = resp.json()["round_id"]
    # Ensure it is actually running before cancelling.
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        snap = client.get(f"/api/rounds/{round_id}").json()
        if snap["state"] == RUNNING:
            break
        time.sleep(0.02)
    cancel = client.post(f"/api/rounds/{round_id}/cancel")
    assert cancel.status_code == 200
    assert cancel.json()["accepted"] is True
    final = _wait_terminal(client, round_id)
    assert final["state"] == CANCELLED
    # A cancelled round exposes no result.
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    assert client.get("/api/rounds").json()["active"] == 0


def test_cancel_after_success_is_refused(client):
    _upload(client)
    resp = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "SELECT * FROM observations"},
    )
    round_id = resp.json()["round_id"]
    final = _wait_terminal(client, round_id)
    assert final["state"] == SUCCEEDED
    cancel = client.post(f"/api/rounds/{round_id}/cancel")
    assert cancel.status_code == 409
    assert "cannot cancel" in cancel.json()["detail"]
    # Result still downloadable.
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 200


def test_slow_query_does_not_block_other_round(client):
    _upload(client)
    infinite_sql = ("WITH RECURSIVE seq(x) AS ("
                    "SELECT 1 UNION ALL SELECT x+1 FROM seq) "
                    "SELECT count(*) FROM seq")
    slow = client.post("/api/rounds", json={"station_id": "ST001", "sql": infinite_sql})
    slow_id = slow.json()["round_id"]
    # Second slot still available for a fast query.
    fast = client.post(
        "/api/rounds",
        json={"station_id": "ST002", "sql": "SELECT count(*) FROM observations"},
    )
    fast_id = fast.json()["round_id"]
    fast_final = _wait_terminal(client, fast_id)
    assert fast_final["state"] == SUCCEEDED
    # The slow one eventually times out; status polling worked throughout.
    slow_final = _wait_terminal(client, slow_id, timeout=20)
    assert slow_final["state"] == FAILED


def test_result_too_large_fails_whole_round(client):
    _upload(client)
    # Generate a wide result: repeated string concatenation creates big rows.
    huge_sql = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq "
        "WHERE x < 200000) SELECT x, repeat('0123456789ABCDEF', 40) AS pad FROM seq"
    )
    resp = client.post("/api/rounds", json={"station_id": "ST001", "sql": huge_sql})
    round_id = resp.json()["round_id"]
    final = _wait_terminal(client, round_id, timeout=20)
    assert final["state"] == FAILED
    assert "8 MiB" in final["error"]
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409


def test_round_not_found(client):
    assert client.get("/api/rounds/deadbeef").status_code == 404
    assert client.post("/api/rounds/deadbeef/cancel").status_code == 404


def test_index_page_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "观测数据 SQL 清洗沙箱" in resp.text


# --------------------------------------------------------------------- uploads

VALID_CSV = (
    "站号,UTC时刻,温度\n"
    "X1,2026-01-01T00:00Z,1.0\n"
    "X1,2026-01-01T01:00Z,2.0\n"
    "X2,2026-01-01T00:00Z,3.0\n"
).encode()

INVALID_CSV = "站号,UTC时刻,温度\nX1,t,not-a-number\n".encode()


def test_upload_returns_swapped_outcome_with_batch_metadata(client):
    resp = client.post(
        "/api/upload", files={"file": ("batch.csv", VALID_CSV, "text/csv")}
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["outcome"] == "swapped"
    assert data["sequence"] == 1
    assert data["total_rows"] == 3
    assert data["stations"] == ["X1", "X2"]
    assert data["station_counts"] == {"X1": 2, "X2": 1}
    assert "换批成功" in data["message"]
    health = client.get("/api/health").json()
    assert health["dataset_sequence"] == 1
    assert health["stations"] == ["X1", "X2"]


def test_invalid_upload_returns_400_and_keeps_current_batch(client):
    _upload(client)
    before = client.get("/api/health").json()
    resp = client.post(
        "/api/upload", files={"file": ("bad.csv", INVALID_CSV, "text/csv")}
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["outcome"] == "rejected"
    assert detail["sequence"] == 2
    assert "校验失败" in detail["message"]
    after = client.get("/api/health").json()
    assert after["dataset_sequence"] == before["dataset_sequence"] == 1
    assert after["stations"] == before["stations"]


def test_slow_upload_validation_does_not_block_rounds_or_downloads(client, monkeypatch):
    """The regression this change fixes: endpoints stay live during validation."""
    import app.upload_pipeline as pipeline_mod

    _upload(client)
    # A succeeded round with a downloadable result, created before the upload.
    rid = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "SELECT count(*) AS n FROM observations"},
    ).json()["round_id"]
    _wait_terminal(client, rid)
    assert client.get(f"/api/rounds/{rid}").json()["state"] == SUCCEEDED

    real_parse = pipeline_mod.parse_observations_csv

    def slow_parse(raw):
        time.sleep(2.0)
        return real_parse(raw)

    monkeypatch.setattr(pipeline_mod, "parse_observations_csv", slow_parse)

    upload_result = {}

    def do_upload():
        upload_result["resp"] = client.post(
            "/api/upload", files={"file": ("slow.csv", VALID_CSV, "text/csv")}
        )

    th = threading.Thread(target=do_upload)
    th.start()
    time.sleep(0.4)  # let the request enter the (now slow) validation stage
    try:
        t0 = time.monotonic()
        health = client.get("/api/health")
        health_ms = (time.monotonic() - t0) * 1000
        t0 = time.monotonic()
        rounds = client.get("/api/rounds")
        rounds_ms = (time.monotonic() - t0) * 1000
        t0 = time.monotonic()
        download = client.get(f"/api/rounds/{rid}/result")
        download_ms = (time.monotonic() - t0) * 1000
    finally:
        th.join()

    assert upload_result["resp"].status_code == 200
    assert health.status_code == 200
    assert rounds.status_code == 200 and rounds.json()["active"] == 0
    assert download.status_code == 200 and b"n\n5\n" in download.content
    # None of the short endpoints waited for the 2 s validation.
    assert max(health_ms, rounds_ms, download_ms) < 500, (
        health_ms, rounds_ms, download_ms
    )


def test_concurrent_uploads_newer_valid_wins_and_older_reported_stale(client, monkeypatch):
    """Out-of-order completion: older file finishes after newer one -> stale."""
    import app.upload_pipeline as pipeline_mod

    real_parse = pipeline_mod.parse_observations_csv
    block_first = threading.Event()
    first_seen = threading.Event()

    def ordered_parse(raw):
        # The first upload (a.csv) blocks until the second one has published.
        if not first_seen.is_set():
            first_seen.set()
            block_first.wait()
        return real_parse(raw)

    monkeypatch.setattr(pipeline_mod, "parse_observations_csv", ordered_parse)

    outcomes = {}

    def upload(name, payload, key):
        outcomes[key] = client.post(
            "/api/upload", files={"file": (name, payload, "text/csv")}
        )

    t1 = threading.Thread(target=upload, args=("a.csv", VALID_CSV, "older"))
    t2 = threading.Thread(target=upload, args=("b.csv", VALID_CSV, "newer"))
    t1.start()
    # Make sure a.csv enters validation (and blocks) before b.csv is submitted.
    first_seen.wait(5)
    t2.start()
    t2.join(timeout=10)
    block_first.set()
    t1.join(timeout=10)

    older = outcomes["older"].json()
    newer = outcomes["newer"].json()
    assert newer["outcome"] == "swapped" and newer["sequence"] == 2
    assert older["outcome"] == "stale" and older["sequence"] == 1
    assert older["current_sequence"] == 2
    assert "已有更新批次" in older["message"]
    # The live batch is the newer one.
    health = client.get("/api/health").json()
    assert health["dataset_sequence"] == 2


def test_newer_invalid_upload_does_not_block_older_valid_swap(client):
    """Newer file fails validation; the earlier valid file still swaps in."""
    # Hold upload #1 inside its worker (valid, but not finished yet) and send
    # upload #2, which is invalid and finishes first.
    validate1 = threading.Event()
    release1 = threading.Event()
    future1 = upload_pipeline.submit_controlled(
        VALID_CSV, "a.csv", validate1, release1
    )
    validate1.set()  # let #1 validate; it still holds at the release barrier

    bad = client.post(
        "/api/upload", files={"file": ("bad.csv", INVALID_CSV, "text/csv")}
    )
    assert bad.status_code == 400
    detail = bad.json()["detail"]
    assert detail["outcome"] == "rejected"
    assert detail["sequence"] == 2

    release1.set()
    older = future1.result(timeout=10)
    assert older.outcome == "swapped"
    assert older.sequence == 1

    # The older valid file (sequence 1) is the live batch; the failed newer
    # file (sequence 2) never replaced it.
    health = client.get("/api/health").json()
    assert health["dataset_sequence"] == 1
    assert health["stations"] == ["X1", "X2"]


def test_accepted_round_keeps_original_input_after_batch_swap(client):
    """Rounds snapshot their station columns; later uploads cannot change them."""
    _upload(client)
    rid = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "SELECT count(*) AS n FROM observations"},
    ).json()["round_id"]
    final = _wait_terminal(client, rid)
    assert final["state"] == SUCCEEDED
    first_csv = client.get(f"/api/rounds/{rid}/result").content

    # Swap in a different batch (ST001 no longer exists there).
    resp = client.post(
        "/api/upload", files={"file": ("other.csv", VALID_CSV, "text/csv")}
    )
    assert resp.json()["outcome"] == "swapped"
    assert "ST001" not in client.get("/api/health").json()["stations"]

    # The old round's result is still the original input's output.
    assert client.get(f"/api/rounds/{rid}/result").content == first_csv
    # New rounds cannot target the vanished station.
    refused = client.post(
        "/api/rounds",
        json={"station_id": "ST001", "sql": "SELECT 1"},
    )
    assert refused.status_code == 404
