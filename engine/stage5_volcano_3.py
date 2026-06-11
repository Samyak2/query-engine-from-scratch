from typing import Any, TYPE_CHECKING

from sqloxide import parse_sql

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


def build_plan(sql: str):
    tree = parse_sql(sql, dialect="ansi")[0]["Query"]["body"]["Select"]
    # import pprint
    import json

    # pprint.pprint(tree)
    print(json.dumps(tree, indent=4))


if __name__ == "__main__":
    # build_plan("select a, b from X")

    # build_plan("select a, b + 1 from X")

    # build_plan("select a, b from X where b > 9000000")

    build_plan("select sum(a) from X where b > 9000000")
