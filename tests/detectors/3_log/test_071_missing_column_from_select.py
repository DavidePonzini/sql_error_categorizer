from tests import *
import pytest

ERROR = SqlErrors.LOG_71_MISSING_COLUMN_FROM_SELECT

@pytest.mark.parametrize("query,solutions,schema,search_path,expected_len,expected_columns", [
    (
        # non-existing columns in solution
        'SELECT a, b FROM table1;',
        ['SELECT a, b, c FROM table1;'],
        None,
        'public',
        (2, 3),
        [('public', None, 'c')],
    ),
    (
        # non-existing colums in solution + existing columns in attempted query
        'SELECT cid, age FROM customer;',
        ['SELECT cid, cname, address FROM customer;'],
        'miedema',
        'miedema',
        (2, 3),
        [('miedema', 'customer', 'cname'), ('miedema', None, 'address')],
    ),
    (
        # correct amount of columns, but some extraneous ones
        'SELECT cid, street AS cname FROM customer;',
        ['SELECT cid, cname FROM customer;'],
        'miedema',
        'miedema',
        (),
        [('miedema', 'customer', 'cname')],
    ),
    # subqueries
    (
        'SELECT a, (SELECT b FROM table2) as sub_col FROM table1;',
        ['SELECT a, d, (SELECT b, c FROM table2) as sub_col FROM table1;'],
        None,
        'public',
        (2, 3),
        [('public', None, 'd'), ('public', None, 'c')],
    ),
    # CTEs
    (
        'WITH cte1 AS (SELECT cid, cname FROM customer) SELECT sid, cid, cname FROM store, cte1;',
        ['WITH cte1 AS (SELECT cid, street as cname FROM customer) SELECT cname FROM store, cte1;'],
        'miedema',
        'miedema',
        (),
        [('miedema', 'customer', 'street')],
    ),
])
def test_wrong(query, solutions, schema, search_path, expected_len, expected_columns):
    detected_errors = run_test(
        query,
        solutions=solutions,
        detectors=[LogicalErrorDetector],
        catalog_filename=schema,
        search_path=search_path,
    )

    assert count_errors(detected_errors, ERROR) == len(expected_columns) + (1 if expected_len else 0)
    if expected_len:
        assert has_error(detected_errors, ERROR, expected_len)
    for schema_name, table_name, column_name in expected_columns:
        assert has_error(detected_errors, ERROR, (schema_name, table_name, column_name))

@pytest.mark.parametrize("query,solutions,schema", [
    (
        # non-existing columns in solution
        'SELECT a, b FROM table1;',
        ['SELECT a, b FROM table1;'],
        None,
    ),
    (
        # existing colums in solution
        'SELECT cid, cname AS street FROM customer;',
        ['SELECT cid, cname FROM customer;'],
        'miedema',
    ),
    (
        # too many columns in attempted query
        'SELECT cid, cname FROM customer;',
        ['SELECT cid FROM customer;'],
        'miedema',
    ),
    (
        # no solutions (return no errors)
        'SELECT a, b, c FROM table1;',
        [],
        None,
    ),
    # subqueries
    (
        'SELECT a, (SELECT b FROM table2) as sub_col FROM table1;',
        ['SELECT a, (SELECT b FROM table2) as sub_col FROM table1;'],
        None,
    ),
    # CTEs
    (
        'WITH cte1 AS (SELECT cid, cname FROM customer) SELECT sid, cid, cname FROM store, cte1;',
        ['WITH cte1 AS (SELECT cid, cname FROM customer) SELECT sid, cid, cname FROM store, cte1;'],
        'miedema',
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