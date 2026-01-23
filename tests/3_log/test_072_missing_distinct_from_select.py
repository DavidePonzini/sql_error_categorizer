from tests import *
import pytest

ERROR = SqlErrors.LOG_72_MISSING_DISTINCT_FROM_SELECT

@pytest.mark.parametrize("query,solutions,schema", [
    (
        'SELECT a, b FROM table1;',
        ['SELECT DISTINCT a, b FROM table1;'],
        None
    ),
    (
        'SELECT cid FROM customer UNION ALL SELECT cid FROM customer;',
        ['SELECT DISTINCT cid FROM customer;'],
        'miedema'
    ),
    # Subqueries
    (
        'SELECT a, (SELECT b FROM table2) as sub_col FROM table1;',
        ['SELECT DISTINCT a, (SELECT b FROM table2) as sub_col FROM table1;'],
        None
    ),
    # CTEs
    (
        'WITH cte AS (SELECT x, y FROM table2) SELECT x, y FROM cte;',
        ['WITH cte AS (SELECT DISTINCT x, y FROM table2) SELECT x, y FROM cte;'],
        None
    ),
])
def test_wrong(query, solutions, schema):
    detected_errors = run_test(
        query,
        solutions=solutions,
        detectors=[LogicalErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == 1

@pytest.mark.parametrize("query,solutions,schema", [
    (
        'SELECT DISTINCT a, b FROM table1;',
        ['SELECT DISTINCT a, b FROM table1;'],
        None
    ),
    (
        'SELECT cid FROM customer;',
        ['SELECT DISTINCT cid FROM customer;'],
        'miedema'
    ),
    (
        'SELECT cid FROM customer UNION SELECT cid FROM customer;',
        ['SELECT DISTINCT cid FROM customer;'],
        'miedema'
    ),
    (
        'SELECT street FROM customer GROUP BY street;',
        ['SELECT DISTINCT street FROM customer;'],
        'miedema'
    ),
    (
        # no solutions (return no errors)
        'SELECT a, b, c FROM table1;',
        [],
        None,
    ),    
    # Subqueries
    (
        'SELECT DISTINCT a, (SELECT b FROM table2) as sub_col FROM table1;',
        ['SELECT DISTINCT a, (SELECT b FROM table2) as sub_col FROM table1;'],
        None
    ),
    # CTEs
    (
        'WITH cte AS (SELECT DISTINCT x, y FROM table2) SELECT x, y FROM cte;',
        ['WITH cte AS (SELECT x, y FROM table2) SELECT DISTINCT x, y FROM cte;'],
        None
    ),
])
def test_correct(query, solutions, schema):
    detected_errors = run_test(
        query,
        solutions=solutions,
        detectors=[LogicalErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == 0