from tests import *
import pytest

@pytest.mark.parametrize('query,column,schema', [
    ('SELECT id FROM store;', 'id', 'miedema'),
    ('SELECT sid FROM store WHERE id > 5;', 'id', 'miedema'),
    # subqueries
    ('SELECT sid FROM store WHRE sid > ALL(SELECT customer_id FROM customer);', 'customer_id', 'miedema'),
    ('SELECT sid FROM store WHERE sid IN (SELECT customer_id FROM customer);', 'customer_id', 'miedema'),
    ('SELECT sid FROM store WHERE sid > ALL(SELECT customer_id FROM customer);', 'customer_id', 'miedema'),
    # CTEs
    ('WITH temp AS (SELECT store_id FROM store) SELECT * FROM temp;', 'store_id', 'miedema'),
    ('WITH temp AS (SELECT sid FROM store) SELECT store_id FROM temp;', 'store_id', 'miedema'),
])
def test_wrong(query, column, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN) == 1
    assert has_error(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN, (column,))

@pytest.mark.parametrize('query,schema', [
    ('SELECT sid FROM store;', 'miedema'),
    ('SELECT sid FROM store WHERE street = \'Eindhoven\';', 'miedema'),
    ('SELECT sid FROM store WHERE street = \'Eindhoven\';', 'miedema'),
    # subqueries
    ('SELECT sid FROM store WHERE sid > ALL(SELECT cid FROM customer);', 'miedema'),
    # CTEs
    ('WITH temp AS (SELECT sname as name FROM store) SELECT name FROM temp;', 'miedema'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema
    )

    assert count_errors(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN) == 0
