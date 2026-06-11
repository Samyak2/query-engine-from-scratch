from typing import Any
import pyarrow.parquet as pq


class Operator:
    def next(self) -> dict[str, Any] | None:
        raise NotImplementedError(f"next not implemented for {self}")

    def close(self):
        raise NotImplementedError(f"close not implemented for {self}")


class TableScan(Operator):
    def __init__(self, filename: str):
        super().__init__()
        self._file = pq.ParquetFile(filename)
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
        super().__init__()
        self._child = child

    def next(self) -> dict[str, Any] | None:
        maybe_row = self._child.next()
        if not maybe_row:
            return None
        return {"a": maybe_row["a"], "b": maybe_row["b"] + 1}

if __name__ == "__main__":
    plan = Projection(TableScan("data/sample_1.parquet"))

    row = plan.next()
    while row is not None:
        print(row)
        row = plan.next()
