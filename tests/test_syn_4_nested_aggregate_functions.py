from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error, has_any_error

def test_nested_aggregate_functions():
    agg1 = 'SUM(MAX(price))'
    agg2 = 'AVG(MIN(quantity))'
    no_agg = 'SUM(price)'

    query = f'''SELECT col1, {agg1}, {no_agg} FROM sales GROUP BY col1 HAVING {agg2};'''

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert has_error(detected_errors, SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (agg1,))
    assert has_error(detected_errors, SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (agg2,))
    assert not has_error(detected_errors, SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (no_agg,))

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

    assert not has_any_error(detected_errors, SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED)

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

    assert has_error(detected_errors, SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (agg_in_subquery,))
    assert has_error(detected_errors, SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED, (outer_agg,))


