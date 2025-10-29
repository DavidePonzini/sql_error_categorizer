import pytest
from tests import *

def test_no_from_simple():
    query = 'SELECT col1'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE) == 1
    assert has_error(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE, (query,))

@pytest.mark.skip(reason="is_constant not yet implemented")
def test_no_from_with_constant_expression():
    query = 'SELECT 1 + 2'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE) == 0

def test_no_from_with_cte():
    query = '''
    WITH cte AS (
        SELECT no_col
    )
    SELECT col1 FROM cte
    '''

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE) == 1
    assert has_error(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE, ('SELECT no_col',))

def test_no_from_with_subquery_both():
    subquery = 'SELECT no_col'
    query = f'SELECT col1 AS sub_col WHERE col2 IN ({subquery})'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE) == 2
    assert has_error(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE, (query,))
    assert has_error(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE, (subquery,))

def test_no_from_with_subquery_only_sub():
    subquery = 'SELECT no_col'
    query = f'SELECT col1 AS sub_col FROM table1 WHERE col2 IN ({subquery})'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE) == 1
    assert has_error(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE, (subquery,))

def test_no_from_with_subquery_only_main():
    subquery = 'SELECT col3 FROM table2'
    query = f'SELECT col1 AS sub_col WHERE col2 IN ({subquery})'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE) == 1
    assert has_error(detected_errors, SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE, (query,))