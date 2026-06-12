from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr


def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    # TODO: copy from previous
    ...


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
        # TODO
        ...


class Filter(Operator):
    def __init__(self, expr: "Expr", child: Operator) -> None:
        super().__init__()
        self._child = child
        self._expr = expr

    def next(self) -> dict[str, Any] | None:
        # TODO
        ...


def build_plan(sql: str):
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    # Bottom of the plan: read every row from the table.
    plan: Operator = TableScan("data/sample_1.parquet")

    # WHERE goes *below* the projection, on the raw rows -- the predicate
    # `age > 35` references columns before projection may rename or drop them.
    # `selection` is None when there's no WHERE clause, so we skip Filter then.
    if tree["selection"] is not None:
        plan = Filter(tree["selection"], plan)

    # SELECT list: unwrap each item into an (alias -> expr) pair, the shape
    # Projection wants. Two shapes come out of the parser:
    #   `name`            -> {"UnnamedExpr": <expr>}
    #   `age + 1 as age`  -> {"ExprWithAlias": {"expr": <expr>, "alias": ...}}
    exprs: dict[str, "Expr"] = {}
    for item in tree["projection"]:
        if "UnnamedExpr" in item:
            expr = item["UnnamedExpr"]
            alias = expr.get("Identifier", {}).get("value", "?column?")
        else:
            expr = item["ExprWithAlias"]["expr"]
            alias = item["ExprWithAlias"]["alias"]["value"]
        exprs[alias] = expr
    plan = Projection(exprs, plan)

    num_rows = 0
    row = plan.next()
    while row is not None:
        print(row)
        num_rows += 1
        row = plan.next()
    print(num_rows)


if __name__ == "__main__":
    # We're now running actual SQL: parse it, build a plan of operators, and
    # the operators lean entirely on execute_expr -- nothing is hardcoded.
    build_plan("select name, age + 1 as age from users")

    build_plan("select name, age from users where age > 35")
