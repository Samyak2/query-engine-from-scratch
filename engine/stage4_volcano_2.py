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
        maybe_row = self._child.next()
        if not maybe_row:
            return None
        return {"name": maybe_row["name"], "age": maybe_row["age"] + 1}


class Filter(Operator):
    def __init__(self, child: Operator) -> None:
        super().__init__()
        self._child = child

    def next(self) -> dict[str, Any] | None:
        while True:
            maybe_row = self._child.next()
            if not maybe_row:
                return None

            if maybe_row["age"] > 25:
                return maybe_row

    def close(self):
        self._child.close()


# select name, age + 1 as age from users where age > 25;
# filter: age > 25 (on the raw rows, before projection)
# projection: name, age + 1
if __name__ == "__main__":
    plan = Projection(Filter(TableScan()))

    num_rows = 0
    row = plan.next()
    while row is not None:
        print(row)
        num_rows += 1
        row = plan.next()

    print(num_rows)
