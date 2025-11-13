from tests import *
import pytest

@pytest.mark.parametrize('query,errors', [
    ('SELECT * FROM orders WHERE SUM(amount) > 100;', [('SUM', 'WHERE')]),
    ('SELECT customer_id, SUM(amount) FROM orders GROUP BY SUM(amount);', [('SUM', 'GROUP BY')]),
    ('SELECT customer_id, SUM(amount) FROM orders ORDER BY SUM(amount) DESC;', [('SUM', 'ORDER BY')]),
    # subqueries
    ('SELECT customer_id, SUM(amount), AVG(amount) FROM orders WHERE SUM(amount) > 100 GROUP BY customer_id ORDER BY AVG(amount);', [('SUM', 'WHERE'), ('AVG', 'ORDER BY')]),
    ('SELECT customer_id, COUNT(*) FROM orders WHERE customer_id > (SELECT MAX(customer_id) FROM customers) AND COUNT(*) > 10;', [('COUNT', 'WHERE')]),
    # CTEs
    ('WITH agg_cte AS (SELECT customer_id, SUM(amount) AS total_amount FROM orders WHERE SUM(amount) > 100 GROUP BY customer_id) SELECT * FROM agg_cte;', [('SUM', 'WHERE')]),
])
def test_wrong(query, errors):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == len(errors)
    for func, clause in errors:
        assert has_error(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, (func, clause))

@pytest.mark.parametrize('query', [
    'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;',
    'SELECT * FROM orders WHERE amount > 100;',
    'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id HAVING SUM(amount) > 100;',
    # subqueries
    'SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders);',
    'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id HAVING SUM(amount) > (SELECT AVG(amount) FROM orders);',
    # CTEs
    'WITH agg_cte AS (SELECT customer_id, SUM(amount) AS total_amount FROM orders GROUP BY customer_id) SELECT * FROM agg_cte;',
    'WITH filtered_orders AS (SELECT * FROM orders WHERE amount > 100) SELECT customer_id, SUM(amount) FROM filtered_orders GROUP BY customer_id;',
])
def test_correct(query):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING) == 0
