def test_pyarrow():
    import pyarrow.parquet as pq

    parquet_file = pq.read_table("data/sample_1.parquet")

    print(parquet_file)


def test_sql_parser():
    from sqloxide import parse_sql

    parse_sql(sql="select * from A", dialect="ansi")
