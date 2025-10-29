from tests import *

def test_simple():
    detected_errors = run_test(
        query='SELECT not_a_function();',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('not_a_function', 'SELECT'))

def test_standard_function():
    detected_errors = run_test(
        query='SELECT COUNT(*) FROM table;',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 0
    assert not has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('COUNT', 'SELECT'))

def test_subquery_from():
    detected_errors = run_test(
        query='''
        SELECT *
        FROM (SELECT not_a_function()) AS sub;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('not_a_function', 'SELECT'))

def test_subquery_where():
    detected_errors = run_test(
        query='''
        SELECT *
        FROM table
        WHERE column > (SELECT not_a_function(column) FROM table);
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION) == 1
    assert has_error(detected_errors, SqlErrors.SYN_5_UNDEFINED_FUNCTION, ('not_a_function', 'SELECT'))

def test_function_parameters1():
    detected_errors = run_test(
        query='SELECT * FROM table WHERE id = :id;',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER) == 1
    assert has_error(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER, (':id',))


def test_function_parameters2():
    detected_errors = run_test(
        query='SELECT * FROM table WHERE id = @id;',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER) == 1
    assert has_error(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER, ('@id',))


def test_function_parameters3():
    detected_errors = run_test(
        query='SELECT * FROM table WHERE id = ?;',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER) == 1
    assert has_error(detected_errors, SqlErrors.SYN_6_UNDEFINED_PARAMETER, ('?',))