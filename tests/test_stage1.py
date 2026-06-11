from engine.stage1_handwritten import query1, query2, query3, query4

def test_query1():
    result = query1([{"a": 1}])
    assert result == [{"a": 1}]
