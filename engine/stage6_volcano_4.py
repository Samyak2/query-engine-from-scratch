from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr


def execute_expr(row: dict[str, Any], expr: "Expr"):
    if "Identifier" in expr:
        ident_name = expr["Identifier"]["value"]
        return row[ident_name]
    if "Value" in expr:
        value = expr["Value"]["value"]["Number"][0]
        return int(value)
    if "BinaryOp" in expr:
        binary = expr["BinaryOp"]
        left = execute_expr(row, binary["left"])
        right = execute_expr(row, binary["right"])
        if binary["op"] == "Plus":
            return left + right
        raise Exception("unknown binary op: ", binary["op"])
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


class Projection(Operator):
    def __init__(self, exprs: dict[str, "Expr"], child: Operator) -> None:
        super().__init__()
        self._child = child
        self._exprs = exprs

    def next(self) -> dict[str, Any] | None:
        maybe_row = self._child.next()
        if not maybe_row:
            return None
        output_row = {}
        for alias, expr in self._exprs.items():
            output_row[alias] = execute_expr(maybe_row, expr)
        return output_row


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


def build_plan(sql: str):
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    plan = Filter(
        tree["selection"],
        Projection(tree["projection"], TableScan("data/sample_1.parquet")),
    )

    num_rows = 0
    row = plan.next()
    while row is not None:
        print(row)
        num_rows += 1
        row = plan.next()
    print(num_rows)


if __name__ == "__main__":
    # build_plan("select a, b from X")

    # build_plan("select a, b from X")

    # build_plan("select a, b from X where b > 9000000")

    build_plan("select sum(a) from X where b > 9000000")
