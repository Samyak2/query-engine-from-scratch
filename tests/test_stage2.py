from engine.stage2_table_scan import (
    FILE_NAME,
    TableScan,
    scan_table,
    scan_table_full,
)

EXPECTED_ROWS = 10_000


def _check_row(row):
    # schema + value ranges from data/sample_1.parquet
    assert set(row.keys()) == {"id", "name", "age", "country"}
    assert 1 <= row["id"] <= EXPECTED_ROWS
    assert 18 <= row["age"] <= 90
    assert isinstance(row["name"], str) and row["name"]
    assert row["country"] in {"France", "Germany", "India", "Italy", "Japan"}


# scan_table: reads the file one batch at a time, returns the full list
def test_sample_1():
    result = scan_table()
    assert len(result) == EXPECTED_ROWS
    assert 18 <= result[0]["age"] <= 90


def test_scan_table_row_count():
    assert len(scan_table()) == EXPECTED_ROWS


def test_scan_table_schema_and_ranges():
    for row in scan_table():
        _check_row(row)


def test_scan_table_matches_full_read():
    # reading row-by-row must yield the same data, in the same order,
    # as reading the whole table at once
    assert scan_table() == scan_table_full(FILE_NAME)


# TableScan: volcano-style operator (next() / close())
def test_tablescan_next_returns_rows_in_order():
    expected = scan_table()
    ts = TableScan()
    try:
        for i in range(5):
            assert ts.next() == expected[i]
    finally:
        ts.close()


def test_tablescan_row_schema():
    ts = TableScan()
    try:
        _check_row(ts.next())
    finally:
        ts.close()


def test_tablescan_drains_all_rows():
    expected = scan_table()
    ts = TableScan()
    rows = []
    try:
        while (row := ts.next()) is not None:
            rows.append(row)
    finally:
        ts.close()

    assert len(rows) == EXPECTED_ROWS
    assert rows == expected


def test_tablescan_returns_none_at_end():
    # next() signals end-of-data by returning None
    ts = TableScan()
    try:
        for _ in range(EXPECTED_ROWS):
            assert ts.next() is not None
        assert ts.next() is None
    finally:
        ts.close()
