from tests import *

def test_unmatched_opening_bracket():
    detected_errors = run_test(
        'SELECT * FROM orders WHERE (amount > 100;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('round', 1, 0))

def test_unmatched_closing_bracket():
    detected_errors = run_test(
        'SELECT * FROM orders WHERE (amount > 100));',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('round', 1, 2))

def test_curly_brackets():
    detected_errors = run_test(
        'SELECT * FROM orders WHERE {amount > 100};',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('curly', 1, 1))

def test_square_brackets_name():
    detected_errors = run_test(
        'SELECT * FROM orders WHERE [amount > 100];',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('square', 1, 1))

def test_square_brackets_stray():
    detected_errors = run_test(
        'SELECT * FROM orders WHERE amount > 100];',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('square', 0, 1))

def test_multiple_bracket_types_and_subqueries():
    detected_errors = run_test(
        '''
        SELECT * FROM [orders] WHERE (amount > 100] AND {status = 'shipped';
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS) == 3
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('round', 1, 0))
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('square', 1, 2))
    assert has_error(detected_errors, SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('curly', 1, 0))