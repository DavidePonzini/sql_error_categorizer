from tests import *
import pytest

@pytest.mark.parametrize('query,value,schema', [
    ('SELECT * FROM table WHERE id = :id;', ':id', None),
    ('SELECT * FROM table WHERE name = @name;', '@name', None),
    ('SELECT * FROM table WHERE age = ?;', '?', None),
    # subqueries
    ('SELECT * FROM table WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept = :dept);', ':dept', None),
    # CTEs
    ('WITH temp AS (SELECT * FROM employees WHERE position = @position) SELECT * FROM temp;', '@position', None),
])
def test_wrong(query, value, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER) == 1
    assert has_error(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER, (value,))

@pytest.mark.parametrize('query,schema', [
    ('SELECT * FROM table WHERE id = 5;', None),
    ('SELECT * FROM table WHERE name = \'John\';', None),
    # subqueries
    ('SELECT * FROM table WHERE age > (SELECT MAX(age) FROM table);', None),
    ('SELECT * FROM employees WHERE dept = \'Sales\';', None),
    # CTEs
    ('WITH temp AS (SELECT * FROM employees WHERE position = \'Manager\') SELECT * FROM temp;', None),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER) == 0