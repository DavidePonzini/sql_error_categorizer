import pytest
from sql_error_categorizer.query import Query

@pytest.mark.parametrize('sql, expected_ctes, expected_main_query', [
    ("WITH cte1 AS (SELECT * FROM table1), cte2 AS (SELECT * FROM table2) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id;",
     ['SELECT * FROM table1', 'SELECT * FROM table2'], 'SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id;'),
    ("SELECT * FROM table;", [], 'SELECT * FROM table;'),
    ("WITH cte AS (SELECT a FROM b) SELECT * FROM cte;", ['SELECT a FROM b'], 'SELECT * FROM cte;')
])
def test_query_cte_extraction(sql, expected_ctes, expected_main_query):
    query = Query(sql)
    assert [cte.sql.strip() for cte in query.ctes] == expected_ctes
    assert query.main_query.sql.strip() == expected_main_query

@pytest.mark.parametrize('sql, expected_selects', [
    ("SELECT a,b FROM table1 WHERE a > (SELECT MAX(a) FROM table2);", 2),
    ("WITH cte AS (SELECT a FROM b) SELECT * FROM cte WHERE a > (SELECT AVG(a) FROM b);", 3),
    ("SELECT * FROM table;", 1),
    ("WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM (SELECT * FROM t2)) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id;", 4)
])
def test_query_selects(sql, expected_selects):
    query = Query(sql)
    assert len(query.selects) == expected_selects, f"Expected {expected_selects} selects, got {len(query.selects)}"

# TODO: Implement tests for set operations properties
@pytest.mark.skip(reason="Not yet implemented")
def test_set_operation_properties():
    pass