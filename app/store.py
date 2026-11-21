"""In-memory store of the latest uploaded observation data."""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

from .csv_io import ParsedCsv, rows_to_columns


@dataclass
class Dataset:
    stations: list[str]
    total_rows: int
    # station_id -> column dict ready for the worker
    by_station: dict[str, dict[str, list]]
    filename: str


class DataStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._dataset: Optional[Dataset] = None

    def replace(self, parsed: ParsedCsv, filename: str) -> Dataset:
        by_station: dict[str, dict[str, list]] = {}
        for station in parsed.stations:
            station_rows = [r for r in parsed.rows if r.station_id == station]
            by_station[station] = rows_to_columns(station_rows)
        dataset = Dataset(
            stations=parsed.stations,
            total_rows=parsed.total_rows,
            by_station=by_station,
            filename=filename,
        )
        with self._lock:
            self._dataset = dataset
        return dataset

    def current(self) -> Optional[Dataset]:
        with self._lock:
            return self._dataset

    def station_columns(self, station_id: str) -> Optional[dict[str, list]]:
        with self._lock:
            if self._dataset is None:
                return None
            return self._dataset.by_station.get(station_id)
