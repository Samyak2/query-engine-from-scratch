from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"


def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    # TODO: copy from previous
    ...


class Operator:
    def next(self) -> dict[str, Any] | None:
        raise NotImplementedError(f"next not implemented for {self}")

    def close(self):
        raise NotImplementedError(f"close not implemented for {self}")


class TableScan(Operator):
    # TODO: copy
    ...


class Filter(Operator):
    # TODO: copy
    ...


class SumAggregate(Operator):
    def __init__(self, alias: str, arg: "Expr", child: Operator) -> None:
        super().__init__()
        self._alias = alias
        self._arg = arg
        self._child = child

    def next(self) -> dict[str, Any] | None:
        # TODO
        ...

    def close(self):
        self._child.close()


def build_plan(sql: str) -> Operator:
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    plan: Operator = TableScan()

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
