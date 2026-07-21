import pytest
from tests import *

ERROR = SqlErrors.NONSTANDARD_OPERATORS

@pytest.mark.parametrize('query,schema,op,expected,dialect', [
    ('SELECT * FROM users WHERE age == 30', None, '==', '=', None),
    ('SELECT * FROM users WHERE age === 30', None, '===', '=', None),
    ('SELECT * FROM users WHERE age !== 30', None, '!==', '<>', None),
    ('SELECT * FROM users WHERE age && 30', None, '&&', ' AND ', None),
    ('SELECT * FROM users WHERE age || 30', None, '||', ' OR ', None),
    ('SELECT * FROM users WHERE age ! 30', None, '!', ' NOT ', None),
    ('SELECT * FROM users WHERE age >> 30', None, '>>', '>', None),
    ('SELECT * FROM users WHERE age << 30', None, '<<', '<', None),
    ('SELECT * FROM users WHERE age ≠ 30', None, '≠', '<>', None),
    ('SELECT * FROM users WHERE age ≥ 30', None, '≥', '>=', None),
    ('SELECT * FROM users WHERE age ≤ 30', None, '≤', '<=', None),
])
def test_wrong(query: str, schema: str, op: str, expected: str, dialect: Dialect | None):

    detected_errors = run_test(
        query,
        catalog_filename=schema,
        search_path=schema,
        detectors=[SyntaxErrorDetector],
        dialect=dialect
    )

    assert count_errors(detected_errors, ERROR) == 1
    assert has_error(detected_errors, ERROR, (op, expected))

@pytest.mark.parametrize('query,schema,dialect', [
    ('SELECT sname || street FROM store', 'miedema', Dialect.POSTGRES),
])
def test_correct(query: str, schema: str, dialect: Dialect | None):
    detected_errors = run_test(
        query,
        catalog_filename=schema,
        search_path=schema,
        detectors=[SyntaxErrorDetector],
        dialect=dialect
    )
    assert count_errors(detected_errors, ERROR) == 0