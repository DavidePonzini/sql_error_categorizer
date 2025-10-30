from tests import *

def test_undefined_function_no_args():
    detected_errors = run_test(
        query='SELECT notafunction() FROM store;',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('notafunction', 'SELECT'))

def test_undefined_function_with_args():
    detected_errors = run_test(
        query='SELECT anotherfunc(col1, col2) FROM store;',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('anotherfunc', 'SELECT'))

def test_defined_function():
    detected_errors = run_test(
        query='SELECT SUM(col1) FROM store;',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 0

def test_undefined_function_in_where():
    detected_errors = run_test(
        query='SELECT * FROM store WHERE invalid_func(col1) > 10;',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('invalid_func', 'WHERE'))

def test_undefined_function_in_subquery():
    detected_errors = run_test(
        query='''
        SELECT *
        FROM store
        WHERE col1 IN (SELECT unknown_func(col2) FROM other_table);
        ''',
        detectors=[SyntaxErrorDetector],
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('unknown_func', 'SELECT'))