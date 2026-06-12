from typing import Any
import pyarrow.parquet as pq

FILE_NAME = "data/sample_1.parquet"

# result should be a list of dictionaries, e.g.:
# [
#     {"col1": "a", "col2": 1, "col3": True},
#     {"col1": "b", "col2": 2, "col3": False},
#     {"col1": "c", "col2": 3, "col3": True},
# ]


# select * from users
def query1() -> list[dict[str, Any]]:
    ...


# select name, age + 1 as age from users
def query2() -> list[dict[str, Any]]:
    data = pq.read_table(FILE_NAME).to_pylist()
    result = []
    for row in data:
        result.append({"name": row["name"], "age": row["age"] + 1})

    return result


# select name, age + 1 as age from users where age > 25
def query3() -> list[dict[str, Any]]:
    ...


# select avg(age) as avg_age from users
def query4() -> list[dict[str, Any]]:
    ...
