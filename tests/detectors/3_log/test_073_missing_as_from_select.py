from tests import *
import pytest

ERROR = SqlErrors.LOG_73_MISSING_AS_FROM_SELECT

@pytest.mark.parametrize("query,solutions,schema,expected", [
    (
        'SELECT a FROM table1;',
        ['SELECT a AS b FROM table1;'],
        None,
        ['b']
    ),
    (
        'SELECT cid, cname AS street FROM customer;',
        ['SELECT cid AS id, cname FROM customer;'],
        'miedema',
        ['id', 'cname']
    )
    # subqueries -- Not applicable
    # CTEs -- Not applicable
])
def test_wrong(query, solutions, schema, expected):
    detected_errors = run_test(
        query,
        solutions=solutions,
        detectors=[LogicalErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == len(expected)
    for col in expected:
        assert has_error(detected_errors, ERROR, (col,))

@pytest.mark.parametrize("query,solutions,schema", [
    (
        'SELECT a AS b FROM table1;',
        ['SELECT a AS b FROM table1;'],
        None,
    ),
    (
        'SELECT cid AS id, cname AS street FROM customer;',
        ['SELECT cid AS id, cname AS street FROM customer;'],
        'miedema',
    ),
    (
        'SELECT cid AS cname, cname AS cid FROM customer;',
        ['SELECT cid AS cname, cname AS cid FROM customer;'],
        'miedema',
    ),
    (
        # no solutions (return no errors)
        'SELECT a, b, c FROM table1;',
        [],
        None,
    ),
    # subqueries -- Not applicable
    # CTEs -- Not applicable
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