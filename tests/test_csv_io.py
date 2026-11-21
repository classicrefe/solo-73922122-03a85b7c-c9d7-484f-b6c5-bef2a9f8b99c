"""Unit tests for CSV parsing and size validation."""
import pytest

from app.config import MAX_UPLOAD_BYTES
from app.csv_io import CsvError, parse_observations_csv


def test_parse_basic_csv():
    raw = "站号,UTC时刻,温度\nA,2026-01-01T00:00Z,1.5\nA,2026-01-01T01:00Z,2.5\nB,2026-01-01T00:00Z,-3\n".encode()
    parsed = parse_observations_csv(raw)
    assert parsed.total_rows == 3
    assert parsed.stations == ["A", "B"]
    assert parsed.rows[0].temperature_c == 1.5


def test_header_order_independent():
    raw = "温度,站号,UTC时刻\n1.5,A,2026-01-01T00:00Z\n".encode()
    parsed = parse_observations_csv(raw)
    assert parsed.rows[0].station_id == "A"
    assert parsed.rows[0].temperature_c == 1.5


def test_missing_header_rejected():
    with pytest.raises(CsvError, match="header"):
        parse_observations_csv(b"station,time,temp\nA,t,1\n")


def test_bad_number_rejected():
    with pytest.raises(CsvError, match="温度"):
        parse_observations_csv("站号,UTC时刻,温度\nA,t,not-a-number\n".encode())


def test_empty_field_rejected():
    with pytest.raises(CsvError):
        parse_observations_csv("站号,UTC时刻,温度\nA,t,\n".encode())


def test_oversized_upload_rejected():
    payload = b"a" * (MAX_UPLOAD_BYTES + 10)
    with pytest.raises(CsvError, match="1 MiB"):
        parse_observations_csv(payload)


def test_binary_nul_rejected():
    with pytest.raises(CsvError):
        parse_observations_csv(b"station\x00,time,temp\n")


def test_no_data_rows_rejected():
    with pytest.raises(CsvError, match="no data"):
        parse_observations_csv("站号,UTC时刻,温度\n".encode())
