import pyarrow.parquet as pq

from engine.stage4_volcano_2 import (
    FILE_NAME,
    Filter,
    Projection,
    TableScan,
)

EXPECTED_ROWS = 10_000


def _raw_rows():
    return pq.read_table(FILE_NAME).to_pylist()


# Filter: where age > 25, applied to the raw scan rows
def test_filter_keeps_only_age_over_25():
    scan = TableScan()
    plan = Filter(scan)
    try:
        rows = []
        while (row := plan.next()) is not None:
            rows.append(row)
    finally:
        scan.close()

    expected = [r for r in _raw_rows() if r["age"] > 25]
    assert rows == expected


def test_filter_emits_full_rows_unchanged():
    scan = TableScan()
    plan = Filter(scan)
    try:
        row = plan.next()
    finally:
        scan.close()
    # filter does not project; it passes rows through untouched
    assert set(row.keys()) == {"id", "name", "age", "country"}
    assert row["age"] > 25


def test_filter_boundary_age_25_excluded():
    # age > 25 is strict, so every surviving row is strictly greater
    scan = TableScan()
    plan = Filter(scan)
    try:
        while (row := plan.next()) is not None:
            assert row["age"] > 25
    finally:
        scan.close()


def test_filter_returns_none_when_exhausted():
    scan = TableScan()
    plan = Filter(scan)
    try:
        while plan.next() is not None:
            pass
        assert plan.next() is None
    finally:
        scan.close()


def test_filter_close_closes_child():
    scan = TableScan()
    plan = Filter(scan)
    plan.close()
    # the underlying parquet file handle is now closed
    assert scan._file.closed


# Full plan: select name, age + 1 as age from users where age > 25
def test_projection_over_filter_matches_query():
    scan = TableScan()
    plan = Projection(Filter(scan))
    try:
        rows = []
        while (row := plan.next()) is not None:
            rows.append(row)
    finally:
        scan.close()

    expected = [
        {"name": r["name"], "age": r["age"] + 1}
        for r in _raw_rows()
        if r["age"] > 25
    ]
    assert rows == expected
    assert all(set(r.keys()) == {"name", "age"} for r in rows)
