from tests import *
import pytest

@pytest.mark.parametrize('query,value,schema,search_path', [
    ('SELECT * FROM notaschema.store;', 'notaschema.store', None, 'public'),
    ('SELECT * FROM miedema.store;', 'miedema.store', None, 'public'),
    ('SELECT * FROM miedema.store;', 'miedema.store', None, 'miedema'),
    # subqueries
    ('SELECT * FROM (SELECT 1 AS id) AS subquery WHERE id IN (SELECT id FROM unknownschema.table);', 'unknownschema.table', None, 'public'),
    # CTEs
    ('WITH temp AS (SELECT * FROM notaschema.customer) SELECT * FROM temp', 'notaschema.customer', 'miedema', 'public'),
])
def test_wrong(query, value, schema, search_path):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=search_path,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_8_INVALID_SCHEMA_NAME) == 1
    assert has_error(detected_errors, SqlErrors.SYN_8_INVALID_SCHEMA_NAME, (value,))

@pytest.mark.parametrize('query,schema,search_path', [
    ('SELECT * FROM miedema.store;', 'miedema', 'public'),
    ('SELECT * FROM miedema.store;', 'miedema', 'miedema'),
    # subqueries
    ('SELECT * FROM (SELECT 1 AS id) AS subquery WHERE id IN (SELECT cid FROM miedema.customer);', 'miedema', 'public'),
    # CTEs
    ('WITH temp AS (SELECT * FROM miedema.customer) SELECT * FROM temp', 'miedema', 'public'),
    ('WITH temp AS (SELECT * FROM miedema.employees) SELECT * FROM temp', 'miedema', 'public'),
])
def test_correct(query, schema, search_path):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=search_path,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_8_INVALID_SCHEMA_NAME) == 0