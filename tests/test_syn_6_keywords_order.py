import pytest
from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error, has_any_error

def test_correct_order():
    query = 'SELECT col1 FROM table1 WHERE col2 = 10 GROUP BY col1 HAVING COUNT(col2) > 5 ORDER BY col1 LIMIT 10 OFFSET 5'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert not has_any_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_ORDER_OF_KEYWORDS)


def test_incorrect_order():
    query = 'SELECT col1 WHERE col2 = 10 FROM table1'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_ORDER_OF_KEYWORDS, (['SELECT', 'WHERE', 'FROM'],))


def test_incorrect_order_with_subquery():
    subquery = 'SELECT col2 GROUP BY col3 FROM table2 ORDER BY col2 LIMIT 5 OFFSET 2 WHERE col3 = 20'
    query = f'SELECT col1 FROM table1 WHERE col2 IN ({subquery})'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector], debug=True
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_ORDER_OF_KEYWORDS, (['SELECT', 'GROUP BY', 'FROM', 'ORDER BY', 'LIMIT', 'OFFSET', 'WHERE'],))