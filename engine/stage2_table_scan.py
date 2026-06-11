from typing import Any
import pyarrow.parquet as pq


def scan_table(filename: str) -> list[dict[str, Any]]:
    data = pq.read_table(filename)
    return data.to_pylist()


# we don't want to read the whole file at once and then select, filter, aggregate, etc.

# what if we could read one row at a time?

def scan_table_2(filename: str) -> list[dict[str, Any]]:
    file = pq.ParquetFile(filename)

    results = []
    for batch in file.iter_batches(1):
        results.extend(batch.to_pylist())

    return results


class TableScan:
    def __init__(self, filename: str):
        self._file = pq.ParquetFile(filename)
        self._iter = self._file.iter_batches(1)

    def next(self) -> dict[str, Any] | None:
        maybe_rows = next(self._iter)
        if not maybe_rows:
            return None
        return maybe_rows.to_pylist()[0]

    def close(self):
        self._file.close()
