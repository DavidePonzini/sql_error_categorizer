from tests import *
import pytest

@pytest.mark.parametrize('query,func,clause,schema', [
    ('SELECT notafunction() FROM store;', 'notafunction', 'SELECT', None),
    ('SELECT anotherfunc(col1, col2) FROM store;', 'anotherfunc', 'SELECT', None),
    ('SELECT * FROM store WHERE invalid_func(col1) > 10;', 'invalid_func', 'WHERE', None),
    # subqueries
    ('''SELECT * FROM store WHERE col1 IN (SELECT unknown_func(col2) FROM other_table);''', 'unknown_func', 'SELECT', None),
    # CTEs
    ('''WITH temp AS (SELECT invalid_func(col) FROM store) SELECT * FROM temp;''', 'invalid_func', 'SELECT', None),
])
def test_wrong(query, func, clause, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, (func, clause))

@pytest.mark.parametrize('query,schema', [
    ('SELECT SUM(col1) FROM store;', None),
    ('SELECT AVG(col2) FROM customer;', None),
    ('SELECT COUNT(*) FROM orders;', None),
    ('SELECT cid FROM customer WHERE LENGTH(cname) > 5;', None),
    ('SELECT cid FROM customer GROUP BY cid HAVING COUNT(order_id) > 2;', None),
    ('SELECT NOW();', None),
    # subqueries
    ('SELECT * FROM store WHERE sid >= (SELECT MAX(col1) FROM store);', None),
    # CTEs
    ('''WITH temp AS (SELECT MAX(col1) FROM store) SELECT * FROM temp;''', None),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 0
