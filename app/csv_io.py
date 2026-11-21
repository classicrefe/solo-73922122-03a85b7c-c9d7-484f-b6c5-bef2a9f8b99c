"""Parsing and validation of uploaded observation CSV files.

Expected columns (Chinese headers, order independent):
    站号, UTC时刻, 温度
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass

from .config import MAX_CSV_ROWS, MAX_UPLOAD_BYTES, TABLE_COLUMNS

HEADER_MAP = {
    "站号": "station_id",
    "utc时刻": "ts_utc",
    "温度": "temperature_c",
}


class CsvError(ValueError):
    """Raised when an uploaded CSV is malformed or violates limits."""


@dataclass
class ObservationRow:
    station_id: str
    ts_utc: str
    temperature_c: float


@dataclass
class ParsedCsv:
    rows: list[ObservationRow]
    stations: list[str]
    total_rows: int


def parse_observations_csv(raw: bytes) -> ParsedCsv:
    if raw is None or len(raw) == 0:
        raise CsvError("uploaded file is empty")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise CsvError(f"upload exceeds maximum size of {MAX_UPLOAD_BYTES} bytes (1 MiB)")

    # Reject UTF-16 / binary payloads early; CSV must be UTF-8 text.
    try:
        text = raw.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as exc:
        raise CsvError(f"file is not valid UTF-8 text: {exc}") from exc

    if "\x00" in text:
        raise CsvError("file contains NUL bytes and is not a valid CSV")

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise CsvError("CSV has no header row")

    header_norm = [h.strip().lower() for h in header]
    try:
        idx = {name: header_norm.index(alias) for alias, name in HEADER_MAP.items()}
    except ValueError as exc:
        raise CsvError(
            "CSV header must contain the columns: 站号, UTC时刻, 温度"
        ) from exc
    if len(header_norm) != len(set(header_norm)):
        raise CsvError("CSV header contains duplicate columns")

    rows: list[ObservationRow] = []
    stations: set[str] = set()
    line_no = 1
    for record in reader:
        line_no += 1
        if not record or all((c is None or c.strip() == "") for c in record):
            continue  # tolerate blank lines
        if len(record) != len(header):
            raise CsvError(f"line {line_no}: expected {len(header)} columns, got {len(record)}")
        station = record[idx["station_id"]].strip()
        ts_raw = record[idx["ts_utc"]].strip()
        temp_raw = record[idx["temperature_c"]].strip()
        if not station:
            raise CsvError(f"line {line_no}: 站号 must not be empty")
        if not ts_raw:
            raise CsvError(f"line {line_no}: UTC时刻 must not be empty")
        if not temp_raw:
            raise CsvError(f"line {line_no}: 温度 must not be empty")
        try:
            temperature = float(temp_raw)
        except ValueError as exc:
            raise CsvError(f"line {line_no}: 温度 is not a number: {temp_raw!r}") from exc
        if temperature != temperature or temperature in (float("inf"), float("-inf")):
            raise CsvError(f"line {line_no}: 温度 must be a finite number")
        rows.append(ObservationRow(station, ts_raw, temperature))
        stations.add(station)
        if len(rows) > MAX_CSV_ROWS:
            raise CsvError(f"CSV exceeds maximum of {MAX_CSV_ROWS} data rows")

    if not rows:
        raise CsvError("CSV contains a header but no data rows")

    return ParsedCsv(
        rows=rows,
        stations=sorted(stations),
        total_rows=len(rows),
    )


def rows_to_columns(rows: list[ObservationRow]) -> dict[str, list]:
    return {
        TABLE_COLUMNS[0]: [r.station_id for r in rows],
        TABLE_COLUMNS[1]: [r.ts_utc for r in rows],
        TABLE_COLUMNS[2]: [r.temperature_c for r in rows],
    }
