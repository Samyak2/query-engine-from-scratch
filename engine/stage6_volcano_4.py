from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from sqloxide import Expr


# The full expression evaluator. In stage 5 this was a small calculator that
# only handled column refs, number literals and the four arithmetic ops.
# Here it grows the arms we need to actually run queries: comparisons (for
# WHERE), string/bool/null literals, parentheses, and a clear error for
# aggregates (which aren't per-row expressions -- they're a separate operator).
#
# It's still one recursive function: execute_expr(row, expr) -> value.
def execute_expr(row: dict[str, Any], expr: "Expr") -> Any:
    # A column reference, e.g. the `age` in `age + 1`: look it up in the row.
    if "Identifier" in expr:
        return row[expr["Identifier"]["value"]]

    # A literal value: a number, string, boolean or null.
    if "Value" in expr:
        value = expr["Value"]["value"]
        if "Number" in value:
            # sqloxide keeps the original text; int vs float by the dot.
            text = value["Number"][0]
            return float(text) if "." in text else int(text)
        if "SingleQuotedString" in value:
            return value["SingleQuotedString"]
        if "Boolean" in value:
            return value["Boolean"]
        if value == "Null":
            return None
        raise Exception("unknown value: ", value)

    # A parenthesised expression, e.g. `(3 + 4)` in `2 * (3 + 4)`. The parser
    # keeps the parens as a `Nested` wrapper; we just unwrap and recurse.
    if "Nested" in expr:
        return execute_expr(row, expr["Nested"])

    # A binary operation: `left op right`. Recurse into both sides, then
    # apply the operator. Precedence is already baked into the tree shape.
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

    # A function call. `avg(age)`, `sum(age)`, `count(*)` are aggregates: they
    # fold many rows into one value, so they can't be computed from a single
    # row. That's a whole operator of its own -- see the aggregation stage.
    if "Function" in expr:
        name = expr["Function"]["name"][0]["Identifier"]["value"]
        raise Exception(
            f"aggregate {name!r} is not a per-row expression; "
            "it's handled by the aggregation stage"
        )

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
