from tests import *

def test_distinct_in_sum():
    query = '''SELECT SUM(DISTINCT 1 + order_id) AS distinct_order_count FROM orders;'''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG) == 1
    assert has_error(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG, ('SUM(DISTINCT 1 + order_id)',))

def test_distinct_in_avg():
    query = '''SELECT AVG(DISTINCT amount) AS distinct_avg_amount FROM payments;'''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG) == 1
    assert has_error(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG, ('AVG(DISTINCT amount)',))

def test_no_distinct_in_sum():
    query = '''SELECT SUM(amount) AS total_amount FROM payments;'''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG) == 0

def test_no_distinct_in_avg():
    query = '''SELECT AVG(amount) AS avg_amount FROM payments;'''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG) == 0

def test_distinct_in_other_aggregates():
    query = '''SELECT COUNT(DISTINCT customer_id) AS distinct_customer_count FROM orders;'''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG) == 0

def test_distinct_in_sum_in_having():
    query = '''SELECT customer_id, SUM(amount) AS total_amount FROM payments GROUP BY customer_id HAVING SUM(DISTINCT amount) > 1000;'''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG) == 1
    assert has_error(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG, ('SUM(DISTINCT amount)',))

def test_distinct_in_solution():
    query = '''SELECT SUM(DISTINCT amount), AVG(DISTINCT amount) AS distinct_avg_amount FROM payments;'''
    solution = '''SELECT SUM(DISTINCT amount), AVG(amount) AS total_amount FROM payments;'''

    result = run_test(
        query,
        solutions=[solution],
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG) == 1
    assert has_error(result, SqlErrors.SEM_41_DISTINCT_IN_SUM_OR_AVG, ('AVG(DISTINCT amount)',))