import pyarrow.parquet as pq
import pytest
from sqloxide import parse_sql

from engine.stage8_vectorized import (
    FILE_NAME,
    SumAggregate,
    TableScan,
    execute_expr_vectorized,
    run_row,
    run_vectorized,
)


def _raw_rows():
    return pq.read_table(FILE_NAME).to_pylist()


def _arg_expr(sql_expr: str):
    func = parse_sql(f"select sum({sql_expr}) from users", dialect="ansi")[0]["Query"][
        "body"
    ]["Select"]["projection"][0]["UnnamedExpr"]["Function"]
    return func["args"]["List"]["args"][0]["Unnamed"]["Expr"]


def _expr(sql_expr: str):
    return parse_sql(f"select {sql_expr}", dialect="ansi")[0]["Query"]["body"][
        "Select"
    ]["projection"][0]["UnnamedExpr"]


# --- vectorized expression evaluator: column in, column out ---


def test_identifier_returns_whole_column():
    batch = {"age": [10, 20, 30]}
    assert execute_expr_vectorized(batch, _expr("age")) == [10, 20, 30]


def test_literal_broadcasts_to_column_length():
    batch = {"age": [10, 20, 30]}
    assert execute_expr_vectorized(batch, _expr("1")) == [1, 1, 1]


def test_binary_op_is_elementwise():
    batch = {"age": [10, 20, 30]}
    assert execute_expr_vectorized(batch, _expr("age + 1")) == [11, 21, 31]


def test_comparison_returns_boolean_column():
    batch = {"age": [10, 40, 35]}
    assert execute_expr_vectorized(batch, _expr("age > 35")) == [False, True, False]


# --- operators over column batches ---


def test_tablescan_yields_column_batches():
    raw = _raw_rows()
    scan = TableScan(FILE_NAME)
    try:
        batch = scan.next()
    finally:
        scan.close()

    assert set(batch.keys()) == {"id", "name", "age", "country"}
    assert len({len(column) for column in batch.values()}) == 1
    assert {name: values[0] for name, values in batch.items()} == raw[0]


def test_sum_aggregate_folds_batches():
    child = _StubChild([{"age": [1, 2, 3]}, {"age": [4, 5]}])
    result = SumAggregate("sum", _arg_expr("age"), child).next()
    assert result == {"sum": [15]}


def test_sum_aggregate_blocks_after_first_call():
    agg = SumAggregate("sum", _arg_expr("age"), _StubChild([{"age": [1, 2]}]))
    assert agg.next() == {"sum": [3]}
    assert agg.next() is None


# --- the whole point: vectorized matches the row engine, exactly ---


@pytest.mark.parametrize(
    "sql",
    [
        "select sum(age) from users",
        "select sum(age + 1) from users",
    ],
)
def test_vectorized_matches_row_engine(sql):
    assert run_vectorized(sql) == run_row(sql)


def test_vectorized_result_matches_raw_data():
    expected = sum(r["age"] for r in _raw_rows())
    assert run_vectorized("select sum(age) from users") == {"sum": expected}


# A tiny child operator that yields pre-built batches, for unit isolation.
class _StubChild:
    def __init__(self, batches):
        self._batches = iter(batches)

    def next(self):
        return next(self._batches, None)

    def close(self):
        pass
