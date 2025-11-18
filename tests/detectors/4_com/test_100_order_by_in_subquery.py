from tests import *

def test_order_by_in_subquery():
    query = 'SELECT * FROM employees WHERE id IN (SELECT employee_id FROM orders ORDER BY order_date)'

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY) == 1
    assert has_error(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY, ('SELECT employee_id FROM orders ORDER BY order_date',))

def test_no_order_by_in_subquery():
    query = 'SELECT * FROM employees WHERE id IN (SELECT employee_id FROM orders WHERE amount > 100)'

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY) == 0

def test_order_by_in_nested_subquery():
    query = 'SELECT * FROM employees WHERE id IN (SELECT employee_id FROM orders WHERE product_id IN (SELECT id FROM products ORDER BY created_at))'

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY) == 1
    assert has_error(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY, ('SELECT id FROM products ORDER BY created_at',))

def test_multiple_order_by_in_subqueries():
    query = 'SELECT * FROM employees WHERE id IN (SELECT employee_id FROM orders ORDER BY order_date) AND department_id IN (SELECT id FROM departments ORDER BY name)'

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY) == 2
    assert has_error(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY, ('SELECT employee_id FROM orders ORDER BY order_date',))
    assert has_error(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY, ('SELECT id FROM departments ORDER BY name',))

def test_order_by_in_subquery_with_limit():
    query = 'SELECT * FROM employees WHERE id IN (SELECT employee_id FROM orders ORDER BY order_date LIMIT 5)'

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_100_ORDER_BY_IN_SUBQUERY) == 0