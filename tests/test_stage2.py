from engine.table_scan import scan_table_2

def test_sample_1():
    result = scan_table_2("data/sample_1.parquet")
    assert len(result) == 10_000
    assert result[0]["a"] > 0 and result[0]["a"] < 1000
