from typing import Any
import pyarrow.parquet as pq

FILE_NAME = "data/sample_1.parquet"


class Operator:
    def next(self) -> dict[str, Any] | None:
        raise NotImplementedError(f"next not implemented for {self}")

    def close(self):
        raise NotImplementedError(f"close not implemented for {self}")


class TableScan(Operator):
    def __init__(self):
        self._file = pq.ParquetFile(FILE_NAME)
        self._iter = self._file.iter_batches(1)

    def next(self) -> dict[str, Any] | None:
        maybe_rows = next(self._iter, None)
        if not maybe_rows:
            return None
        return maybe_rows.to_pylist()[0]

    def close(self):
        self._file.close()


class Projection(Operator):
    def __init__(self, child: Operator) -> None:
        self._child = child

    def next(self) -> dict[str, Any] | None:
        # TODO
        ...


# select name, age + 1 as age from users;
# projection: name, age + 1
if __name__ == "__main__":
    # TODO: create plan and run
    ...
