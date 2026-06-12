import time
from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"
BATCH_SIZE = 8192


# ---------------------------------------------------------------------------
# Re-write the engine: every operator is now columnar.
#
# Up to stage 6 the whole engine was row-at-a-time: each operator passed a
# single dict {"id": 1, "name": "...", "age": 63} to the next. Stage 8 made
# the SUM path columnar, but only the aggregate path.
#
# Here we rewrite ALL the operators to speak the same columnar language: each
# next() passes a *batch of columns*,
#       {"name": [...], "age": [...]}        (a dict of columns)
# instead of one row. TableScan, Filter, Projection and SumAggregate all take
# a column-batch in and hand a column-batch out. The data is columnar from the
# scan all the way to the result -- no row dicts anywhere.
#
# We're still using plain Python lists for the columns (no Arrow, no NumPy) --
# the point of this stage is the *shape*: operators that think in columns, not
# rows. Working on whole columns is what later lets a real engine drop in
# compiled kernels; here we just establish the column-at-a-time model.
# ---------------------------------------------------------------------------

Batch = dict[str, list]


def _batch_len(batch: Batch) -> int:
    return len(next(iter(batch.values())))


def execute_expr_vectorized(batch: Batch, expr: "Expr") -> list:
    """Evaluate an expression over a column-batch, returning a whole column."""
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
        return [scalar] * n

    if "Nested" in expr:
        return execute_expr_vectorized(batch, expr["Nested"])

    if "BinaryOp" in expr:
        binary = expr["BinaryOp"]
        left = execute_expr_vectorized(batch, binary["left"])
        right = execute_expr_vectorized(batch, binary["right"])
        op = binary["op"]
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


class Operator:
    def next(self) -> Batch | None:
        raise NotImplementedError(f"next not implemented for {self}")

    def close(self):
        raise NotImplementedError(f"close not implemented for {self}")


class TableScan(Operator):
    def __init__(self, filename: str, batch_size: int = BATCH_SIZE):
        super().__init__()
        self._file = pq.ParquetFile(filename)
        # batch_size is the only knob that matters for this stage: 1 means
        # row-at-a-time (a one-row column dict per call), large means columnar.
        self._iter = self._file.iter_batches(batch_size)

    def next(self) -> Batch | None:
        record_batch = next(self._iter, None)
        if record_batch is None:
            return None
        # to_pydict() hands us {col_name: [values...]} -- a dict of columns.
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
        # Evaluate the predicate into a boolean column, then keep, for every
        # column, only the positions where the mask is true.
        mask = execute_expr_vectorized(batch, self._expr)
        return {
            col: [v for v, keep in zip(values, mask) if keep]
            for col, values in batch.items()
        }

    def close(self):
        self._child.close()


class Projection(Operator):
    def __init__(self, exprs: dict[str, "Expr"], child: Operator) -> None:
        super().__init__()
        self._exprs = exprs
        self._child = child

    def next(self) -> Batch | None:
        batch = self._child.next()
        if batch is None:
            return None
        # Each output column is one expression evaluated over the whole batch.
        return {
            alias: execute_expr_vectorized(batch, expr)
            for alias, expr in self._exprs.items()
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

        # Blocking: drain the child, summing each column-batch as we go.
        total = 0
        while (batch := self._child.next()) is not None:
            if _batch_len(batch) == 0:
                continue
            total += sum(execute_expr_vectorized(batch, self._arg))
        return {self._alias: [total]}

    def close(self):
        self._child.close()


def _unwrap_projection_item(item: dict) -> tuple[str, "Expr"]:
    if "ExprWithAlias" in item:
        return item["ExprWithAlias"]["alias"]["value"], item["ExprWithAlias"]["expr"]
    expr = item["UnnamedExpr"]
    alias = expr.get("Identifier", {}).get("value", "?column?")
    return alias, expr


def build_plan(sql: str, batch_size: int = BATCH_SIZE) -> Operator:
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    plan: Operator = TableScan(FILE_NAME, batch_size)

    if tree["selection"] is not None:
        plan = Filter(tree["selection"], plan)

    # If the SELECT is a single aggregate (sum), build the aggregate operator;
    # otherwise it's a plain projection. Both consume column-batches.
    first = tree["projection"][0]
    inner = first.get("ExprWithAlias", {}).get("expr", first.get("UnnamedExpr", {}))
    if "Function" in inner:
        name = inner["Function"]["name"][0]["Identifier"]["value"].lower()
        if name != "sum":
            raise Exception(f"only sum() is supported in this stage, got {name!r}")
        alias = (
            first["ExprWithAlias"]["alias"]["value"]
            if "ExprWithAlias" in first
            else "sum"
        )
        arg = inner["Function"]["args"]["List"]["args"][0]["Unnamed"]["Expr"]
        return SumAggregate(alias, arg, plan)

    exprs = dict(_unwrap_projection_item(item) for item in tree["projection"])
    return Projection(exprs, plan)


def run(sql: str, batch_size: int = BATCH_SIZE):
    """Run a query and collect the column-batches back into rows."""
    plan = build_plan(sql, batch_size)
    try:
        rows = []
        while (batch := plan.next()) is not None:
            cols = list(batch.keys())
            for i in range(_batch_len(batch)):
                rows.append({col: batch[col][i] for col in cols})
        return rows
    finally:
        plan.close()


def _time(sql: str, batch_size: int, repeats: int) -> float:
    start = time.perf_counter()
    for _ in range(repeats):
        run(sql, batch_size)
    return (time.perf_counter() - start) / repeats


def benchmark(sql: str, repeats: int = 20) -> None:
    # Same engine, same operators -- the ONLY difference is how many rows each
    # next() carries: 1 (row-at-a-time) vs a full columnar batch.
    row_rows = run(sql, batch_size=1)
    col_rows = run(sql, batch_size=BATCH_SIZE)
    assert row_rows == col_rows, (row_rows[:3], col_rows[:3])

    row_time = _time(sql, 1, repeats)
    col_time = _time(sql, BATCH_SIZE, repeats)

    print(f"=== {sql}")
    print(f"  rows out         : {len(col_rows)}")
    print(f"  row    (batch=1) : {row_time * 1000:8.3f} ms")
    print(f"  columnar (b={BATCH_SIZE}): {col_time * 1000:8.3f} ms")
    print(f"  speedup          : {row_time / col_time:6.2f}x\n")


if __name__ == "__main__":
    benchmark("select sum(age) from users")
    benchmark("select name, age + 1 as age from users")
    benchmark("select name, age from users where age > 35")
