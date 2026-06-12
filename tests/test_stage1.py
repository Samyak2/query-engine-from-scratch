import pyarrow.parquet as pq

from engine.stage1_handwritten import (
    FILE_NAME,
    query1,
    query2,
    query3,
    query4,
)


def load_data():
    return pq.read_table(FILE_NAME).to_pylist()


# query1: select * from users
def test_query1_returns_all_rows():
    data = load_data()
    result = query1()
    assert result == data


def test_query1_preserves_all_columns():
    result = query1()
    assert set(result[0].keys()) == {"id", "name", "age", "country"}


# query2: select name, age + 1 as age from users
def test_query2_projects_name_and_increments_age():
    data = load_data()
    result = query2()
    expected = [{"name": row["name"], "age": row["age"] + 1} for row in data]
    assert result == expected


def test_query2_drops_other_columns():
    result = query2()
    assert set(result[0].keys()) == {"name", "age"}


# query3: select name, age + 1 as age from users where age > 25
def test_query3_filters_and_projects():
    data = load_data()
    expected = [
        {"name": row["name"], "age": row["age"] + 1}
        for row in data
        if row["age"] > 25
    ]
    result = query3()
    assert result == expected


def test_query3_excludes_age_at_or_below_25():
    result = query3()
    # age + 1 means every surviving row had age > 25, so reported age > 26
    assert all(row["age"] > 26 for row in result)


# query4: select avg(age) as avg_age from users
def test_query4_computes_average_age():
    data = load_data()
    expected = sum(row["age"] for row in data) / len(data)
    result = query4()
    assert result == [{"avg_age": expected}]
