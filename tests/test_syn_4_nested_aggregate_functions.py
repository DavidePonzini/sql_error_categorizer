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