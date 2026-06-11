from engine.stage2_table_scan import scan_table, scan_table_full, TableScan

SAMPLE = "data/sample_1.parquet"
EXPECTED_ROWS = 10_000


def _check_row(row):
    # schema + value ranges from data/sample_1.py
    assert set(row.keys()) == {"a", "b", "c"}
    assert 0 <= row["a"] <= 1000
    assert -1_000_000 <= row["b"] <= 1_000_000
    assert isinstance(row["c"], str) and len(row["c"]) == 10


# scan_table: reads the file one batch at a time, returns the full list
def test_sample_1():
    result = scan_table(SAMPLE)
    assert len(result) == EXPECTED_ROWS
    assert result[0]["a"] > 0 and result[0]["a"] < 1000


def test_scan_table_row_count():
    assert len(scan_table(SAMPLE)) == EXPECTED_ROWS


def test_scan_table_schema_and_ranges():
    for row in scan_table(SAMPLE):
        _check_row(row)


def test_scan_table_matches_full_read():
    # reading row-by-row must yield the same data, in the same order,
    # as reading the whole table at once
    assert scan_table(SAMPLE) == scan_table_full(SAMPLE)


# TableScan: volcano-style operator (next() / close())
def test_tablescan_next_returns_rows_in_order():
    expected = scan_table(SAMPLE)
    ts = TableScan(SAMPLE)
    try:
        for i in range(5):
            assert ts.next() == expected[i]
    finally:
        ts.close()


def test_tablescan_row_schema():
    ts = TableScan(SAMPLE)
    try:
        _check_row(ts.next())
    finally:
        ts.close()


def test_tablescan_drains_all_rows():
    expected = scan_table(SAMPLE)
    ts = TableScan(SAMPLE)
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
    ts = TableScan(SAMPLE)
    try:
        for _ in range(EXPECTED_ROWS):
            assert ts.next() is not None
        assert ts.next() is None
    finally:
        ts.close()
