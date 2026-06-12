import pyarrow.parquet as pq
import pytest
from sqloxide import parse_sql

from engine.stage7_aggregate import (
    FILE_NAME,
    Filter,
    SumAggregate,
    TableScan,
    build_plan,
)


def _raw_rows():
    return pq.read_table(FILE_NAME).to_pylist()


def _arg_expr(sql_expr: str):
    # pull the inner argument of sum(<expr>)
    func = parse_sql(f"select sum({sql_expr}) from users", dialect="ansi")[0]["Query"][
        "body"
    ]["Select"]["projection"][0]["UnnamedExpr"]["Function"]
    return func["args"]["List"]["args"][0]["Unnamed"]["Expr"]


def _run(sql: str):
    plan = build_plan(sql)
    try:
        rows = []
        while (row := plan.next()) is not None:
            rows.append(row)
        return rows
    finally:
        plan.close()


def _scan():
    return TableScan()


# SumAggregate folds every row into a single total
def test_sum_over_all_rows():
    scan = _scan()
    plan = SumAggregate("sum", _arg_expr("age"), scan)
    try:
        first = plan.next()
    finally:
        scan.close()
    assert first == {"sum": sum(r["age"] for r in _raw_rows())}


# It's a blocking operator: exactly one output row, then None
def test_sum_emits_single_row_then_none():
    scan = _scan()
    plan = SumAggregate("sum", _arg_expr("age"), scan)
    try:
        assert plan.next() is not None
        assert plan.next() is None
        assert plan.next() is None
    finally:
        scan.close()


# The inner expression is evaluated per row before summing
def test_sum_of_expression():
    scan = _scan()
    plan = SumAggregate("total", _arg_expr("age + 1"), scan)
    try:
        result = plan.next()
    finally:
        scan.close()
    assert result == {"total": sum(r["age"] + 1 for r in _raw_rows())}


# Sum over a filtered child only counts surviving rows
def test_sum_over_filter():
    predicate = parse_sql("select * from users where age > 35", dialect="ansi")[0][
        "Query"
    ]["body"]["Select"]["selection"]
    scan = _scan()
    plan = SumAggregate("sum", _arg_expr("age"), Filter(predicate, scan))
    try:
        result = plan.next()
    finally:
        scan.close()
    assert result == {"sum": sum(r["age"] for r in _raw_rows() if r["age"] > 35)}


# build_plan: actual SQL end to end
def test_build_plan_sum():
    assert _run("select sum(age) from users") == [
        {"sum": sum(r["age"] for r in _raw_rows())}
    ]


def test_build_plan_sum_with_alias():
    rows = _run("select sum(age) as total_age from users")
    assert rows == [{"total_age": sum(r["age"] for r in _raw_rows())}]


def test_build_plan_sum_with_where():
    rows = _run("select sum(age) from users where age > 35")
    assert rows == [{"sum": sum(r["age"] for r in _raw_rows() if r["age"] > 35)}]


# No GROUP BY yet, and only sum() in this stage
def test_non_sum_aggregate_rejected():
    with pytest.raises(Exception, match="only sum"):
        build_plan("select avg(age) from users")
