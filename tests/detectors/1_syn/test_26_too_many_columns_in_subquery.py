from tests import *

def test_in():
    subquery = 'SELECT col4, col5 FROM table2'
    query = f'SELECT col1, col2 FROM table1 WHERE col3 IN ({subquery})'

    detected_errors = run_test(
        debug=True,
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY) == 1
    assert has_error(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY, (subquery, 1))
    
def test_exists():
    subquery = 'SELECT col2, col3 FROM table2'
    query = f'SELECT col1 FROM table1 WHERE EXISTS ({subquery})'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY) == 0

def test_comparison():
    subquery = 'SELECT col3, col4 FROM table2'
    query = f'SELECT col1 FROM table1 WHERE col2 = ({subquery})'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY) == 1
    assert has_error(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY, (subquery, 1))

def test_comparison_no_error():
    subquery = 'SELECT col3 FROM table2'
    query = f'SELECT col1 FROM table1 WHERE col2 = ({subquery})'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY) == 0

def test_multiple_columns_select():
    subquery = 'SELECT col1, col2 FROM table2'
    query = f'SELECT ({subquery}) AS subquery_result FROM table1'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY) == 1
    assert has_error(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY, (subquery, 1))

def test_multiple_columns_from():
    subquery = 'SELECT col2, col3 FROM table2'
    query = f'SELECT col1 FROM ({subquery}) AS subquery_alias'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY) == 0