import pytest
from sqloxide import parse_sql

from engine.stage5_volcano_3 import execute_expr


# Helper: parse a single SELECT-list expression out of `select <sql_expr>`
# so the tests can talk in SQL, the same way the stage does.
def _parse_one_expr(sql_expr: str):
    select = parse_sql(f"select {sql_expr}", dialect="ansi")[0]["Query"]["body"][
        "Select"
    ]
    item = select["projection"][0]
    if "UnnamedExpr" in item:
        return item["UnnamedExpr"]
    return item["ExprWithAlias"]["expr"]


def _eval(sql_expr: str, row: dict | None = None):
    return execute_expr(row or {}, _parse_one_expr(sql_expr))


# Value: a bare literal number parses back to an int
def test_literal_number():
    assert _eval("42") == 42


# Identifier: a column reference looks the name up in the current row
def test_identifier_reads_column_from_row():
    assert _eval("age", {"age": 30}) == 30


def test_identifier_missing_column_raises():
    with pytest.raises(KeyError):
        _eval("age", {"name": "x"})


# BinaryOp: each operator over two literals
@pytest.mark.parametrize(
    "sql_expr, expected",
    [
        ("1 + 1", 2),
        ("10 - 4", 6),
        ("6 * 7", 42),
        ("20 / 5", 4),
    ],
)
def test_binary_ops(sql_expr, expected):
    assert _eval(sql_expr) == expected


# BinaryOp over a column: the stage-4 `age + 1` expressed as a parsed tree
def test_binary_op_over_column():
    assert _eval("age + 1", {"age": 63}) == 64


# Precedence is baked into the tree shape by the parser, not by execute_expr
def test_precedence_multiply_before_add():
    # parses as (2 * 3) + 4, not 2 * (3 + 4)
    assert _eval("2 * 3 + 4") == 10


def test_parentheses_override_precedence():
    assert _eval("2 * (3 + 4)") == 14


def test_nested_expression_over_column():
    # (age * 2) - 1, with age = 63 -> 125
    assert _eval("age * 2 - 1", {"age": 63}) == 125


# An unknown operator should fail loudly rather than silently.
# Stage 5's evaluator only knows the four arithmetic ops, so a comparison
# like `<` trips the guard (comparisons arrive in the next stage).
def test_unknown_binary_op_raises():
    with pytest.raises(Exception, match="unknown binary op"):
        _eval("1 < 2")


def test_unknown_expr_raises():
    # a function call isn't a Value / Identifier / BinaryOp / Nested,
    # so it falls through to the catch-all
    with pytest.raises(Exception, match="unknown expr"):
        _eval("count(age)")
