from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"


def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    # TODO
    ...


def run(sql: str) -> None:
    select = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]

    # Each item in the SELECT list comes in one of two shapes:
    #   `name`            -> {"UnnamedExpr": <expr>}
    #   `age + 1 as age`  -> {"ExprWithAlias": {"expr": <expr>, "alias": ...}}
    # We unwrap both into (alias, expr) pairs and hand each expr to our
    # interpreter, once per row.
    output_exprs: dict[str, "Expr"] = {}
    for item in select["projection"]:
        if "UnnamedExpr" in item:
            expr = item["UnnamedExpr"]
            alias = expr.get("Identifier", {}).get("value", "?column?")
        else:
            expr = item["ExprWithAlias"]["expr"]
            alias = item["ExprWithAlias"]["alias"]["value"]
        output_exprs[alias] = expr

    print(f"=== {sql}")
    for row in pq.read_table(FILE_NAME).to_pylist():
        output_row = {
            alias: execute_expr(row, expr) for alias, expr in output_exprs.items()
        }
        print(output_row)


if __name__ == "__main__":
    run("select name, age + 1 as age from users")
    run("select name, age * 2 - 1 as score from users")  # precedence: (age*2) - 1
