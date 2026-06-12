import pytest
import pyarrow.parquet as pq
from sqloxide import parse_sql

from engine.stage6_volcano_4 import (
    Filter,
    Projection,
    TableScan,
    build_plan,
    execute_expr,
)

FILE_NAME = "data/sample_1.parquet"


def _raw_rows():
    return pq.read_table(FILE_NAME).to_pylist()


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


def _drain(plan, scan):
    try:
        rows = []
        while (row := plan.next()) is not None:
            rows.append(row)
        return rows
    finally:
        scan.close()


# --- execute_expr: the arms stage 6 grew beyond stage 5 ---


# Stage 5 only had arithmetic; stage 6 adds the literal kinds.
@pytest.mark.parametrize(
    "sql_expr, expected",
    [
        ("42", 42),
        ("3.5", 3.5),
        ("'hello'", "hello"),
        ("true", True),
        ("null", None),
    ],
)
def test_value_literals(sql_expr, expected):
    assert _eval(sql_expr) == expected


@pytest.mark.parametrize(
    "sql_expr, expected",
    [
        ("1 + 1", 2),
        ("10 - 4", 6),
        ("6 * 7", 42),
        ("20 / 5", 4),
    ],
)
def test_arithmetic_ops(sql_expr, expected):
    assert _eval(sql_expr) == expected


# Comparisons are the new arms that make WHERE work; they return bools.
@pytest.mark.parametrize(
    "sql_expr, row, expected",
    [
        ("age > 35", {"age": 36}, True),
        ("age > 35", {"age": 35}, False),  # strict at the boundary
        ("age < 35", {"age": 18}, True),
        ("age >= 35", {"age": 35}, True),
        ("age <= 35", {"age": 35}, True),
        ("age = 35", {"age": 35}, True),
        ("age != 35", {"age": 35}, False),
    ],
)
def test_comparison_ops(sql_expr, row, expected):
    assert _eval(sql_expr, row) is expected


def test_identifier_reads_column():
    assert _eval("age + 1", {"age": 63}) == 64


def test_nested_parentheses():
    assert _eval("2 * (3 + 4)") == 14


# Function calls are not per-row expressions in this stage.
def test_aggregate_function_raises():
    with pytest.raises(Exception):
        _eval("avg(age)", {"age": 30})


def test_unknown_binary_op_raises():
    # string concat `||` isn't in the table of ops
    with pytest.raises(Exception, match="unknown binary op"):
        _eval("name || 'x'", {"name": "a"})


# --- Projection / Filter operators, driven by execute_expr ---


# select name, age + 1 as age from users
def test_projection_evaluates_exprs_per_row():
    raw = _raw_rows()
    exprs = parse_sql(
        "select name, age + 1 as age from users", dialect="ansi"
    )[0]["Query"]["body"]["Select"]
    # reuse build_plan's unwrapping by going through the operator directly
    scan = TableScan(FILE_NAME)
    plan = Projection(
        {
            "name": exprs["projection"][0]["UnnamedExpr"],
            "age": exprs["projection"][1]["ExprWithAlias"]["expr"],
        },
        scan,
    )
    rows = _drain(plan, scan)

    expected = [{"name": r["name"], "age": r["age"] + 1} for r in raw]
    assert rows == expected
    assert all(set(r.keys()) == {"name", "age"} for r in rows)


# where age > 35
def test_filter_keeps_rows_matching_predicate():
    predicate = parse_sql("select * from users where age > 35", dialect="ansi")[0][
        "Query"
    ]["body"]["Select"]["selection"]
    scan = TableScan(FILE_NAME)
    plan = Filter(predicate, scan)
    rows = _drain(plan, scan)

    expected = [r for r in _raw_rows() if r["age"] > 35]
    assert rows == expected


# --- build_plan: actual SQL, end to end ---


def test_build_plan_projection_only(capsys):
    build_plan("select name, age + 1 as age from users")
    out = capsys.readouterr().out.splitlines()
    raw = _raw_rows()
    assert out[0] == str({"name": raw[0]["name"], "age": raw[0]["age"] + 1})
    assert out[-1] == str(len(raw))  # trailing row count


def test_build_plan_filter_then_project(capsys):
    build_plan("select name, age from users where age > 35")
    out = capsys.readouterr().out.splitlines()
    expected_count = sum(1 for r in _raw_rows() if r["age"] > 35)
    assert out[-1] == str(expected_count)


def test_build_plan_no_where_keeps_all_rows(capsys):
    # selection is None -> no Filter operator -> every row survives
    build_plan("select name from users")
    out = capsys.readouterr().out.splitlines()
    assert out[-1] == str(len(_raw_rows()))
