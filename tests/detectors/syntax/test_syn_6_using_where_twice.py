import pytest
from tests import *

def test_single_where():
    query = 'SELECT col1 WHERE col2 = 1'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE) == 0

def test_double_where_main():
    query = 'SELECT col1 WHERE col2 = 1 WHERE col3 = 2'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE) == 1
    assert has_error(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE, (query, 2))

def test_double_where_subquery():
    subquery = 'SELECT col2 WHERE col3 = 1 WHERE col4 = 2'

    query = f'''
    SELECT col1, ({subquery}) as subquery_col
    FROM table1
    WHERE col5 = 3
    '''

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE) == 1
    assert has_error(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE, (subquery, 2))

def test_multiple_double_where():
    subquery1 = 'SELECT col2 WHERE col3 = 1 WHERE col4 = 2'
    subquery2 = 'SELECT col5 WHERE col6 = 3 WHERE col7 = 4 WHERE col8 = 5'

    query = f'''
    SELECT col1, ({subquery1}) as subquery_col1, ({subquery2}) as subquery_col2
    FROM table1
    WHERE col8 = 5 WHERE col9 = 6
    '''

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE) == 3
    assert has_error(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE, (subquery1, 2))
    assert has_error(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE, (subquery2, 3))
    assert has_error(detected_errors, SqlErrors.SYN_19_USING_WHERE_TWICE, (query.strip(), 2))