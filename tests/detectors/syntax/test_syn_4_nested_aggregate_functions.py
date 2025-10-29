from tests import *

def test_nested_aggregate_functions():
    agg1 = 'SUM(MAX(price))'
    agg2 = 'AVG(MIN(quantity))'
    no_agg = 'SUM(price)'

    query = f'''SELECT col1, {agg1}, {no_agg} FROM sales GROUP BY col1 HAVING {agg2};'''

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED) == 2
    assert has_error(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (agg1,))
    assert has_error(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (agg2,))

def test_no_nested_aggregate_functions_subquery():
    agg_in_subquery = 'MAX(price)'
    outer_agg = 'SUM(total_price)'

    query = f'''
    SELECT col1, {outer_agg} FROM (
        SELECT col1, {agg_in_subquery} AS total_price
        FROM sales
        GROUP BY col1
    ) AS subquery
    GROUP BY col1;
    '''

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED) == 0

def test_aggregate_functions_subquery():
    agg_in_subquery = 'COUNT(SUM(price))'
    outer_agg = 'AVG(COUNT(quantity))'

    query = f'''
    SELECT col1, {outer_agg} FROM (
        SELECT col1, {agg_in_subquery} AS total_count
        FROM sales
        GROUP BY col1
    ) AS subquery
    GROUP BY col1;
    '''

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED) == 2
    assert has_error(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (agg_in_subquery,))
    assert has_error(detected_errors, SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (outer_agg,))


