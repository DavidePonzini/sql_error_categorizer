from tests import *
import pytest
import itertools

@pytest.mark.parametrize('query,column,table_aliases,schema', [
    ('SELECT street FROM store s, customer c;', 'street', ['s.street', 'c.street'], 'miedema'),
    ('SELECT s.street FROM store s, customer c WHERE street = c.street;', 'street', ['s.street', 'c.street'], 'miedema'),
    # subqueries
    ('SELECT * FROM store s, customer c WHERE cid IN (SELECT street FROM store s2, customer c2);', 'street', ['s.street', 'c.street', 's2.street', 'c2.street'], 'miedema'),
    # CTEs
    ('WITH temp AS (SELECT street FROM store s, customer c) SELECT street FROM temp;', 'street', ['s.street', 'c.street'], 'miedema'),
])
def test_wrong(query, column, table_aliases, schema):
    detected_errors = run_test(
        query=query, 
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN) == 1
    assert any([ has_error(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN, (column, list(perm))) for perm in itertools.permutations(table_aliases) ])

@pytest.mark.parametrize('query,schema', [
    ('SELECT s.street FROM store s, customer c;', 'miedema'),
    # subqueries
    ('SELECT * FROM store s, customer c WHERE cid IN (SELECT s2.street FROM store s2, customer c2);', 'miedema'),
    # CTEs
    ('WITH temp AS (SELECT s.street FROM store s, customer c) SELECT street FROM temp;', 'miedema'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema
    )

    assert count_errors(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN) == 0
