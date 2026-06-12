import pyarrow.parquet as pq
import pytest
from sqloxide import parse_sql

from engine.stage9_columnar import (
    FILE_NAME,
    Filter,
    Projection,
    SumAggregate,
    TableScan,
    execute_expr_vectorized,
    run,
)


def _raw_rows():
    return pq.read_table(FILE_NAME).to_pylist()


def _expr(sql_expr: str):
    return parse_sql(f"select {sql_expr}", dialect="ansi")[0]["Query"]["body"][
        "Select"
    ]["projection"][0]["UnnamedExpr"]


def _arg_expr(sql_expr: str):
    func = parse_sql(f"select sum({sql_expr}) from users", dialect="ansi")[0]["Query"][
        "body"
    ]["Select"]["projection"][0]["UnnamedExpr"]["Function"]
    return func["args"]["List"]["args"][0]["Unnamed"]["Expr"]


# --- columnar evaluator: a whole column in, a whole column out ---


def test_identifier_returns_whole_column():
    assert execute_expr_vectorized({"age": [10, 20, 30]}, _expr("age")) == [10, 20, 30]


def test_literal_broadcasts_to_column_length():
    assert execute_expr_vectorized({"age": [10, 20, 30]}, _expr("1")) == [1, 1, 1]


def test_binary_op_is_elementwise():
    assert execute_expr_vectorized({"age": [10, 20, 30]}, _expr("age + 1")) == [
        11,
        21,
        31,
    ]


def test_comparison_returns_boolean_column():
    assert execute_expr_vectorized({"age": [10, 40, 35]}, _expr("age > 35")) == [
        False,
        True,
        False,
    ]


# --- every operator now speaks column-batches ---


def test_projection_evaluates_each_output_column():
    batch = {"name": ["a", "b"], "age": [10, 20]}
    out = Projection(
        {"name": _expr("name"), "age": _expr("age + 1")}, _StubChild([batch])
    ).next()
    assert out == {"name": ["a", "b"], "age": [11, 21]}


def test_filter_keeps_matching_rows_columnar():
    batch = {"age": [10, 40, 35, 50], "id": [1, 2, 3, 4]}
    predicate = parse_sql("select * from users where age > 35", dialect="ansi")[0][
        "Query"
    ]["body"]["Select"]["selection"]
    out = Filter(predicate, _StubChild([batch])).next()
    assert out == {"age": [40, 50], "id": [2, 4]}


def test_sum_aggregate_folds_batches():
    child = _StubChild([{"age": [1, 2, 3]}, {"age": [4, 5]}])
    assert SumAggregate("sum", _arg_expr("age"), child).next() == {"sum": [15]}


def test_tablescan_yields_dict_of_columns():
    scan = TableScan(FILE_NAME)
    try:
        batch = scan.next()
    finally:
        scan.close()
    assert isinstance(batch, dict)
    assert set(batch.keys()) == {"id", "name", "age", "country"}
    assert isinstance(batch["age"], list)


# --- the payoff: row-at-a-time and columnar give identical results ---


@pytest.mark.parametrize(
    "sql",
    [
        "select sum(age) from users",
        "select sum(age) from users where age > 35",
        "select name, age + 1 as age from users",
        "select name, age from users where age > 35",
    ],
)
def test_columnar_matches_row_at_a_time(sql):
    assert run(sql, batch_size=8192) == run(sql, batch_size=1)


def test_projection_result_matches_raw_data():
    raw = _raw_rows()
    expected = [{"name": r["name"], "age": r["age"] + 1} for r in raw]
    assert run("select name, age + 1 as age from users") == expected


def test_sum_result_matches_raw_data():
    expected = sum(r["age"] for r in _raw_rows())
    assert run("select sum(age) from users") == [{"sum": expected}]


class _StubChild:
    def __init__(self, batches):
        self._batches = iter(batches)

    def next(self):
        return next(self._batches, None)

    def close(self):
        pass
