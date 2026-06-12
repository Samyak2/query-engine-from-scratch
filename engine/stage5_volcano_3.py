from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr

FILE_NAME = "data/sample_1.parquet"


# Until stage 4 we evaluated expressions like `age + 1` by hand, e.g.
#   output_row["age"] = row["age"] + 1
# That only works because *we* read the SQL and wrote the matching Python.
#
# Now we let the SQL parser (sqloxide) turn the text into a tree, and we
# write an interpreter that walks that tree. The interpreter is recursive
# because expressions are recursive: `age + 1` is a `+` whose left side is
# an `age` column reference and whose right side is the literal `1`.
def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    # A column reference, e.g. the `age` in `select age + 1`.
    # This is the case that makes expressions interesting: we look the
    # name up in the current row. It's the parsed-tree version of the
    # hand-written `row["age"]` from stage 4.
    if "Identifier" in expr:
        column_name = expr["Identifier"]["value"]
        return row[column_name]

    # A literal number, e.g. the `1` in `age + 1`.
    # sqloxide stores it as a string, so we parse it back to an int.
    if "Value" in expr:
        value = expr["Value"]["value"]["Number"][0]
        return int(value)

    # A parenthesised expression, e.g. the `(3 + 4)` in `2 * (3 + 4)`.
    # The parser keeps the parens as a `Nested` wrapper so it can faithfully
    # round-trip the SQL; for evaluation we just unwrap and recurse.
    if "Nested" in expr:
        return execute_expr(row, expr["Nested"])

    # A binary operation: something `op` something, e.g. `age + 1`.
    # We recurse into each side first, then apply the operator. Operator
    # precedence (`*` before `+`) is already baked into the tree shape by
    # the parser, so we don't have to think about it here.
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
        raise Exception("unknown binary op: ", op)

    raise Exception("unknown expr: ", expr)


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
