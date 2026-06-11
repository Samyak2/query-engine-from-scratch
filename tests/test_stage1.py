from engine.stage1_handwritten import query1, query2, query3, query4


# query1: select * from X
def test_query1():
    result = query1([{"a": 1}])
    assert result == [{"a": 1}]


def test_query1_multiple_rows():
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    assert query1(data) == data


def test_query1_empty():
    assert query1([]) == []


def test_query1_preserves_all_columns():
    data = [{"a": 1, "b": 2, "c": "x", "y": -5}]
    assert query1(data) == data


# query2: select a, b + 1 from X
def test_query2_projects_and_computes():
    result = query2([{"a": 1, "b": 2}])
    assert result == [{"a": 1, "b": 3}]


def test_query2_drops_other_columns():
    result = query2([{"a": 1, "b": 2, "c": "keep_me_out", "y": 9}])
    assert result == [{"a": 1, "b": 3}]


def test_query2_multiple_rows():
    data = [{"a": 1, "b": 0}, {"a": 5, "b": -1}]
    assert query2(data) == [{"a": 1, "b": 1}, {"a": 5, "b": 0}]


def test_query2_empty():
    assert query2([]) == []


# query3: select a, b + 1 from X where y > 0
def test_query3_keeps_matching_and_projects():
    result = query3([{"a": 1, "b": 2, "y": 5}])
    assert result == [{"a": 1, "b": 3}]


def test_query3_filters_out_non_positive_y():
    data = [
        {"a": 1, "b": 2, "y": -3},
        {"a": 2, "b": 3, "y": 4},
    ]
    assert query3(data) == [{"a": 2, "b": 4}]


def test_query3_boundary_y_zero_excluded():
    # y > 0 is strict, so y == 0 must be filtered out
    assert query3([{"a": 1, "b": 2, "y": 0}]) == []


def test_query3_all_filtered():
    data = [{"a": 1, "b": 2, "y": 0}, {"a": 2, "b": 3, "y": -1}]
    assert query3(data) == []


def test_query3_empty():
    assert query3([]) == []


# query4: select sum(a) as total from X
def test_query4_sums_rows():
    result = query4([{"a": 1}, {"a": 2}, {"a": 3}])
    assert result == [{"total": 6}]


def test_query4_single_row():
    assert query4([{"a": 7}]) == [{"total": 7}]


def test_query4_empty_sums_to_zero():
    assert query4([]) == [{"total": 0}]


def test_query4_negative_values():
    assert query4([{"a": 5}, {"a": -2}, {"a": -3}]) == [{"total": 0}]
