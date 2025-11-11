from tests import *

def test_aggregate_in_select():
    detected_errors = run_test(
        'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 0

def test_aggregate_in_where():
    detected_errors = run_test(
        'SELECT * FROM orders WHERE SUM(amount) > 100;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 1
    assert has_error(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, ('SUM', 'WHERE'))

def test_no_aggregate_in_where():
    detected_errors = run_test(
        'SELECT * FROM orders WHERE amount > 100;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 0

def test_aggregate_in_group_by():
    detected_errors = run_test(
        'SELECT customer_id, SUM(amount) FROM orders GROUP BY SUM(amount);',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 1
    assert has_error(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, ('SUM', 'GROUP BY'))

def test_aggregate_in_having():
    detected_errors = run_test(
        'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id HAVING SUM(amount) > 100;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 0

def test_aggregate_in_order_by():
    detected_errors = run_test(
        'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id ORDER BY SUM(amount) DESC;',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 1
    assert has_error(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, ('SUM', 'ORDER BY'))

def test_multiple_aggregates_in_where_and_order_by():
    detected_errors = run_test(
        'SELECT customer_id, SUM(amount), AVG(amount) FROM orders WHERE SUM(amount) > 100 GROUP BY customer_id ORDER BY AVG(amount);',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 2
    assert has_error(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, ('SUM', 'WHERE'))
    assert has_error(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, ('AVG', 'ORDER BY'))