from typing import Any
import pyarrow.parquet as pq

FILE_NAME = "data/sample_1.parquet"


def scan_table_full(filename: str) -> list[dict[str, Any]]:
    data = pq.read_table(filename)
    return data.to_pylist()


# we don't want to read the whole file at once and then select, filter, aggregate, etc.
# what if we could read one row at a time?
def scan_table() -> list[dict[str, Any]]:
    # TODO
    ...


class TableScan:
    def __init__(self):
        # TODO
        ...

    def next(self) -> dict[str, Any] | None:
        # TODO
        ...

    def close(self):
        # TODO
        ...
