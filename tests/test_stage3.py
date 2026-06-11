import pyarrow.parquet as pq

from engine.stage3_volcano import FILE_NAME, Projection, TableScan

EXPECTED_ROWS = 10_000


def _raw_rows():
    return pq.read_table(FILE_NAME).to_pylist()


# TableScan: volcano-style leaf operator (select * from users)
def test_tablescan_yields_rows_in_order():
    expected = _raw_rows()
    scan = TableScan()
    try:
        for i in range(5):
            assert scan.next() == expected[i]
    finally:
        scan.close()


def test_tablescan_drains_and_terminates():
    scan = TableScan()
    rows = []
    try:
        while (row := scan.next()) is not None:
            rows.append(row)
        # once drained, it keeps signalling end-of-data
        assert scan.next() is None
    finally:
        scan.close()

    assert len(rows) == EXPECTED_ROWS
    assert rows == _raw_rows()


# Projection: select name, age + 1 as age from users
def test_projection_projects_name_and_increments_age():
    raw = _raw_rows()
    scan = TableScan()
    plan = Projection(scan)
    try:
        for i in range(5):
            expected = {"name": raw[i]["name"], "age": raw[i]["age"] + 1}
            assert plan.next() == expected
    finally:
        scan.close()


def test_projection_only_emits_name_and_age():
    scan = TableScan()
    plan = Projection(scan)
    try:
        row = plan.next()
        assert set(row.keys()) == {"name", "age"}
    finally:
        scan.close()


def test_projection_drains_all_rows():
    raw = _raw_rows()
    expected = [{"name": r["name"], "age": r["age"] + 1} for r in raw]

    scan = TableScan()
    plan = Projection(scan)
    rows = []
    try:
        while (row := plan.next()) is not None:
            rows.append(row)
    finally:
        scan.close()

    assert len(rows) == EXPECTED_ROWS
    assert rows == expected


def test_projection_returns_none_when_child_exhausted():
    raw = _raw_rows()
    scan = TableScan()
    plan = Projection(scan)
    try:
        for _ in range(len(raw)):
            assert plan.next() is not None
        assert plan.next() is None
    finally:
        scan.close()
