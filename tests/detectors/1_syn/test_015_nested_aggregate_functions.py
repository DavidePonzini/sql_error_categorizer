from tests import *
import pytest

@pytest.mark.parametrize('query, aggregates', [
    (
        'SELECT col1, SUM(MAX(price)), SUM(price) FROM sales GROUP BY col1 HAVING AVG(MIN(quantity));',
        ['SUM(MAX(price))', 'AVG(MIN(quantity))']
    ),
    # subqueries
    (
        'SELECT col1, COUNT(SUM(price)) FROM (SELECT col1, AVG(COUNT(quantity)) AS total_count FROM sales GROUP BY col1 ) AS subquery GROUP BY col1;',
        ['COUNT(SUM(price))', 'AVG(COUNT(quantity))']
    ),
    # CTEs
    (
        'WITH cte AS (SELECT col1, MAX(SUM(price)) AS total_value FROM sales GROUP BY col1) SELECT col1, MIN(AVG(quantity)) FROM cte GROUP BY col1;',
        ['MAX(SUM(price))', 'MIN(AVG(quantity))']
    ),
])
def test_wrong(query, aggregates):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED) == len(aggregates)
    for agg in aggregates:
        assert has_error(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (agg,))

@pytest.mark.parametrize('query', [
    'SELECT col1, SUM(total_price) FROM table1 GROUP BY col1;',
    # subqueries
    'SELECT col1, COUNT(quantity) FROM (SELECT col1, SUM(price) AS total FROM sales GROUP BY col1) AS subquery GROUP BY col1;',
    'SELECT col1, AVG(total_quantity) FROM table1 WHERE total_quantity > (SELECT MAX(quantity) FROM table2) GROUP BY col1;',
    # CTEs
    'WITH cte AS (SELECT col1, MAX(price) AS max_price FROM sales GROUP BY col1) SELECT col1, MIN(max_price) FROM cte GROUP BY col1;',
])
def test_correct(query):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED) == 0


