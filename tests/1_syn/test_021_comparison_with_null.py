import pytest
from tests import *

operators = ['=', '<>', '<', '<=', '>', '>=']

@pytest.mark.parametrize('operator', operators)
def test_equals_null(operator):
    detected_errors = run_test(
        query=f'SELECT * FROM table WHERE column {operator} NULL;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_21_COMPARISON_WITH_NULL) == 1
    assert has_error(detected_errors, SqlErrors.SYN_21_COMPARISON_WITH_NULL, (f'column {operator} NULL',))

def test_is_null():
    detected_errors = run_test(
        query='SELECT * FROM table WHERE column IS NULL;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_21_COMPARISON_WITH_NULL) == 0

def test_is_not_null():
    detected_errors = run_test(
        query='SELECT * FROM table WHERE column IS NOT NULL;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_21_COMPARISON_WITH_NULL) == 0

def test_comparison_in_subquery():
    detected_errors = run_test(
        query='''
        SELECT * FROM table1 WHERE column1 = (
            SELECT column2 FROM table2 WHERE column3 <> NULL
        );
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_21_COMPARISON_WITH_NULL) == 1
    assert has_error(detected_errors, SqlErrors.SYN_21_COMPARISON_WITH_NULL, ('column3 <> NULL',))