from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"


# Same per-row evaluator as stage 6 -- it walks a parsed expression tree and
# returns a value for one row. Aggregates like sum() are NOT handled here:
# they need every row at once, so they're a separate operator (below).
def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    if "Identifier" in expr:
        return row[expr["Identifier"]["value"]]

    if "Value" in expr:
        value = expr["Value"]["value"]
        if "Number" in value:
            text = value["Number"][0]
            return float(text) if "." in text else int(text)
        if "SingleQuotedString" in value:
            return value["SingleQuotedString"]
        if "Boolean" in value:
            return value["Boolean"]
        if value == "Null":
            return None
        raise Exception("unknown value: ", value)

    if "Nested" in expr:
        return execute_expr(row, expr["Nested"])

    if "BinaryOp" in expr:
        binary = expr["BinaryOp"]
        left = execute_expr(row, binary["left"])
        right = execute_expr(row, binary["right"])
        op = binary["op"]
        if op == "Plus":
            return left + right
        if op == "Minus":
            return left - right
        if op == "Multiply":
            return left * right
        if op == "Divide":
            return left / right
        if op == "Gt":
            return left > right
        if op == "Lt":
            return left < right
        if op == "GtEq":
            return left >= right
        if op == "LtEq":
            return left <= right
        if op == "Eq":
            return left == right
        if op == "NotEq":
            return left != right
        raise Exception("unknown binary op: ", op)

    raise Exception("unknown expr: ", expr)


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


class Filter(Operator):
    def __init__(self, expr: "Expr", child: Operator) -> None:
        super().__init__()
        self._child = child
        self._expr = expr

    def next(self) -> dict[str, Any] | None:
        while True:
            maybe_row = self._child.next()
            if not maybe_row:
                return None
            if execute_expr(maybe_row, self._expr):
                return maybe_row

    def close(self):
        self._child.close()


# SUM is our first *aggregate*: it folds many rows into a single value.
#
# Every operator so far has been streaming -- one row in, (maybe) one row out.
# An aggregate can't do that: to know SUM(age) it must see EVERY row first.
# So it's a *blocking* operator. On the first next() call it drains its child
# completely, accumulating the running total, then emits one result row. Every
# call after that returns None (there's only one row of output).
#
# We evaluate the inner expression per row with the same execute_expr, so
# SUM(age + 1) or SUM(age * 2) work too, not just SUM(age).
class SumAggregate(Operator):
    def __init__(self, alias: str, arg: "Expr", child: Operator) -> None:
        super().__init__()
        self._alias = alias
        self._arg = arg
        self._child = child
        self._done = False

    def next(self) -> dict[str, Any] | None:
        if self._done:
            return None
        self._done = True

        total = 0
        while (row := self._child.next()) is not None:
            total += execute_expr(row, self._arg)
        return {self._alias: total}

    def close(self):
        self._child.close()


def build_plan(sql: str) -> Operator:
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    plan: Operator = TableScan(FILE_NAME)

    if tree["selection"] is not None:
        plan = Filter(tree["selection"], plan)

    # No GROUP BY yet: we assume the SELECT is a single aggregate like
    # `sum(age)`. Pull the function name, its argument expression, and an
    # alias for the output column.
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

    # Unwrap the single argument: sum(<arg>) -> <arg>.
    arg = func["args"]["List"]["args"][0]["Unnamed"]["Expr"]
    alias = alias or "sum"

    return SumAggregate(alias, arg, plan)


def run(sql: str) -> None:
    plan = build_plan(sql)
    print(f"=== {sql}")
    try:
        while (row := plan.next()) is not None:
            print(row)
    finally:
        plan.close()


if __name__ == "__main__":
    run("select sum(age) from users")
    run("select sum(age) from users where age > 35")
    run("select sum(age + 1) from users")
