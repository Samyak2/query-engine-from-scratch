from typing import Any

# select * from X
def query1(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return data

# select a, b + 1 from X
def query2(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in data:
        new_row = {"a": row["a"], "b": row["b"] + 1}
        result.append(new_row)

    return result

# select a, b from X where y > 0
def query3(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in data:
        if row["y"] > 0:
            new_row = {"a": row["a"], "b": row["b"] + 1}
            result.append(new_row)

    return result

# select sum(a) as total from X
def query4(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = 0
    for row in data:
        total += row["a"]

    return [{"total": total}]
