"""In-memory store of the latest uploaded observation data.

A monotonically increasing ``sequence`` is stamped on every validated batch
in the order files enter validation. ``swap_if_newer`` only publishes a batch
when it is newer than the one currently published, which makes out-of-order
validation completion safe (see :mod:`app.upload_pipeline`).
"""
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
    # 1-based order in which the file entered validation; higher means newer.
    sequence: int

    @property
    def station_counts(self) -> dict[str, int]:
        return {
            station: len(cols["station_id"])
            for station, cols in self.by_station.items()
        }


def build_dataset(parsed: ParsedCsv, filename: str, sequence: int) -> Dataset:
    """Split a parsed CSV into per-station columnar copies.

    Rows are bucketed in a single pass (O(rows)) instead of scanning all
    rows once per station, so a file with many stations stays cheap.
    """
    buckets: dict[str, list] = {}
    for row in parsed.rows:
        buckets.setdefault(row.station_id, []).append(row)
    by_station = {
        station: rows_to_columns(buckets[station]) for station in parsed.stations
    }
    return Dataset(
        stations=parsed.stations,
        total_rows=parsed.total_rows,
        by_station=by_station,
        filename=filename,
        sequence=sequence,
    )


class DataStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._dataset: Optional[Dataset] = None

    def swap_if_newer(self, dataset: Dataset) -> bool:
        """Publish ``dataset`` only when it is newer than the current batch.

        Returns ``True`` when the swap happened. A batch never replaces an
        equally-new or newer batch, so a slow validation cannot overwrite a
        batch that was published later.
        """
        with self._lock:
            current = self._dataset
            if current is not None and dataset.sequence <= current.sequence:
                return False
            self._dataset = dataset
            return True

    def current(self) -> Optional[Dataset]:
        with self._lock:
            return self._dataset

    def station_columns(self, station_id: str) -> Optional[dict[str, list]]:
        with self._lock:
            if self._dataset is None:
                return None
            return self._dataset.by_station.get(station_id)
