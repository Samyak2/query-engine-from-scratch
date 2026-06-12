import time
from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

from engine.stage7_aggregate import build_plan as build_row_plan

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"

# How many rows each vectorized next() call processes at once. The row engine
# in stage 7 used a batch size of 1 (one dict per row). Here we work on whole
# chunks of columns.
BATCH_SIZE = 8192


# ---------------------------------------------------------------------------
# Why a different shape?
#
# Stage 7's engine moves one ROW at a time, each row a Python dict:
#     {"id": 1, "name": "...", "age": 63, "country": "..."}
# To sum ages it touches every column of every row, chases a dict lookup per
# row, and runs the Python interpreter once per row. That's a lot of overhead
# for one integer add.
#
# Real data is laid out by COLUMN, not by row. If we keep it that way -- one
# contiguous array per column -- summing age means walking a single tight
# array of ints. No dict lookups, no touching name/country, friendly to the
# CPU cache (the next age you need is the next slot in memory). We process a
# whole BATCH of values per call instead of one row. This is "vectorized" /
# columnar execution.
#
# Here a batch is a dict of columns: {col_name: [values...]}. The operators
# loop over those lists in Python (no pyarrow.compute magic) so you can see
# exactly where the work happens -- and still measure the win from the layout.
# ---------------------------------------------------------------------------

Batch = dict[str, list]


def execute_expr_vectorized(batch: Batch, expr: "Expr") -> list:
    """Evaluate an expression over a whole column batch, returning a column."""
    n = _batch_len(batch)

    if "Identifier" in expr:
        return batch[expr["Identifier"]["value"]]

    if "Value" in expr:
        value = expr["Value"]["value"]
        if "Number" in value:
            text = value["Number"][0]
            scalar = float(text) if "." in text else int(text)
        elif "SingleQuotedString" in value:
            scalar = value["SingleQuotedString"]
        elif "Boolean" in value:
            scalar = value["Boolean"]
        elif value == "Null":
            scalar = None
        else:
            raise Exception("unknown value: ", value)
        # A literal is the same for every row -> broadcast to a full column.
        return [scalar] * n

    if "Nested" in expr:
        return execute_expr_vectorized(batch, expr["Nested"])

    if "BinaryOp" in expr:
        binary = expr["BinaryOp"]
        left = execute_expr_vectorized(batch, binary["left"])
        right = execute_expr_vectorized(batch, binary["right"])
        op = binary["op"]
        # Same operators as the row evaluator -- only the shape differs: we
        # apply each one element-wise down the two columns.
        if op == "Plus":
            return [left[i] + right[i] for i in range(n)]
        if op == "Minus":
            return [left[i] - right[i] for i in range(n)]
        if op == "Multiply":
            return [left[i] * right[i] for i in range(n)]
        if op == "Divide":
            return [left[i] / right[i] for i in range(n)]
        if op == "Gt":
            return [left[i] > right[i] for i in range(n)]
        if op == "Lt":
            return [left[i] < right[i] for i in range(n)]
        if op == "GtEq":
            return [left[i] >= right[i] for i in range(n)]
        if op == "LtEq":
            return [left[i] <= right[i] for i in range(n)]
        if op == "Eq":
            return [left[i] == right[i] for i in range(n)]
        if op == "NotEq":
            return [left[i] != right[i] for i in range(n)]
        raise Exception("unknown binary op: ", op)

    raise Exception("unknown expr: ", expr)


def _batch_len(batch: Batch) -> int:
    return len(next(iter(batch.values())))


class Operator:
    def next(self) -> Batch | None:
        raise NotImplementedError(f"next not implemented for {self}")

    def close(self):
        raise NotImplementedError(f"close not implemented for {self}")


class TableScan(Operator):
    def __init__(self, filename: str):
        super().__init__()
        self._file = pq.ParquetFile(filename)
        # Read BATCH_SIZE rows at a time, as columns.
        self._iter = self._file.iter_batches(BATCH_SIZE)

    def next(self) -> Batch | None:
        record_batch = next(self._iter, None)
        if record_batch is None:
            return None
        # to_pydict() hands us {col_name: [values...]} -- columnar already.
        return record_batch.to_pydict()

    def close(self):
        self._file.close()


class Filter(Operator):
    def __init__(self, expr: "Expr", child: Operator) -> None:
        super().__init__()
        self._child = child
        self._expr = expr

    def next(self) -> Batch | None:
        batch = self._child.next()
        if batch is None:
            return None
        # Evaluate the predicate as a column of booleans, then keep only the
        # rows where it's true -- column by column.
        mask = execute_expr_vectorized(batch, self._expr)
        return {
            col: [v for v, keep in zip(values, mask) if keep]
            for col, values in batch.items()
        }

    def close(self):
        self._child.close()


class SumAggregate(Operator):
    def __init__(self, alias: str, arg: "Expr", child: Operator) -> None:
        super().__init__()
        self._alias = alias
        self._arg = arg
        self._child = child
        self._done = False

    def next(self) -> Batch | None:
        if self._done:
            return None
        self._done = True

        # Blocking, like stage 7 -- but we fold a whole column per batch
        # instead of one row at a time.
        total = 0
        while (batch := self._child.next()) is not None:
            if _batch_len(batch) == 0:
                continue
            total += sum(execute_expr_vectorized(batch, self._arg))
        return {self._alias: [total]}

    def close(self):
        self._child.close()


def build_plan(sql: str) -> Operator:
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    plan: Operator = TableScan(FILE_NAME)

    if tree["selection"] is not None:
        plan = Filter(tree["selection"], plan)

    item = tree["projection"][0]
    if "ExprWithAlias" in item:
        func = item["ExprWithAlias"]["expr"]["Function"]
        alias = item["ExprWithAlias"]["alias"]["value"]
    else:
        func = item["UnnamedExpr"]["Function"]
        alias = None

    name = func["name"][0]["Identifier"]["value"].lower()
    if name != "sum":
        raise Exception(f"only sum() is supported in this stage, got {name!r}")

    arg = func["args"]["List"]["args"][0]["Unnamed"]["Expr"]
    return SumAggregate(alias or "sum", arg, plan)


def run_vectorized(sql: str):
    plan = build_plan(sql)
    try:
        result = None
        while (batch := plan.next()) is not None:
            result = {k: v[0] for k, v in batch.items()}
        return result
    finally:
        plan.close()


def run_row(sql: str):
    plan = build_row_plan(sql)
    try:
        result = None
        while (row := plan.next()) is not None:
            result = row
        return result
    finally:
        plan.close()


def _time(fn, sql: str, repeats: int) -> float:
    start = time.perf_counter()
    for _ in range(repeats):
        fn(sql)
    return (time.perf_counter() - start) / repeats


def benchmark(sql: str, repeats: int = 20) -> None:
    row_result = run_row(sql)
    vec_result = run_vectorized(sql)
    assert row_result == vec_result, (row_result, vec_result)

    row_time = _time(run_row, sql, repeats)
    vec_time = _time(run_vectorized, sql, repeats)

    print(f"=== {sql}")
    print(f"  result          : {vec_result}")
    print(f"  row    (batch=1) : {row_time * 1000:8.3f} ms")
    print(f"  vector (batch={BATCH_SIZE}): {vec_time * 1000:8.3f} ms")
    print(f"  speedup          : {row_time / vec_time:6.2f}x\n")


if __name__ == "__main__":
    benchmark("select sum(age) from users")
    benchmark("select sum(age) from users where age > 35")
    benchmark("select sum(age + 1) from users")
