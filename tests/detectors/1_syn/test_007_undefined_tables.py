from tests import *
import pytest

@pytest.mark.parametrize('query,objects,schema', [
    ('SELECT * FROM store;', ('store',), None),
    ('SELECT * FROM customer c JOIN orders o ON c.cid = o.cid;', ['orders AS o'], 'miedema'),
    # subqueries
    ('SELECT * FROM products p WHERE p.pid IN (SELECT pid FROM my_table);', ['products AS p', 'my_table'], 'miedema'),
    # CTEs
    ('WITH temp AS (SELECT * FROM employees) SELECT * FROM temp;', ['employees'], None),
    ('WITH temp AS (SELECT * FROM unknown_table) SELECT * FROM temp2 WHERE id > 5;', ['unknown_table', 'temp2'], None),
])
def test_wrong(query, objects, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT) == len(objects)
    for obj in objects:
        assert has_error(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT, (obj,))

@pytest.mark.parametrize('query,schema', [
    ('SELECT * FROM store;', 'miedema'),
    ('SELECT * FROM customer c JOIN store s ON c.cid = s.sid;', 'miedema'),
    # subqueries
    ('SELECT * FROM customer c WHERE c.sid IN (SELECT sid FROM store);', 'miedema'),
    # CTEs
    ('WITH temp AS (SELECT * FROM store) SELECT * FROM temp;', 'miedema'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT) == 0
