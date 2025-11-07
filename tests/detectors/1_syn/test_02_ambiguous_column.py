from tests import *

def test_ambiguous_column():
    detected_errors = run_test(
        query='''SELECT street FROM store s, customer c;''', 
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN) == 1
    assert any([
        has_error(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN, ('street', ['s.street', 'c.street'])),
        has_error(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN, ('street', ['c.street', 's.street'])),
    ])

def test_ambiguous_column_no_error():
    detected_errors = run_test(
        query='SELECT s.street FROM store s, customer c;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN) == 0
    assert not any([
        has_error(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN, ('street', ['s.street', 'c.street'])),
        has_error(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN, ('street', ['c.street', 's.street'])),
    ])

def test_ambiguous_column_where():
    detected_errors = run_test(
        query='SELECT s.street FROM store s, customer c WHERE street = c.street;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN) == 1
    assert any([
        has_error(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN, ('street', ['s.street', 'c.street'])),
        has_error(detected_errors, SqlErrors.SYN_2_AMBIGUOUS_COLUMN, ('street', ['c.street', 's.street'])),
    ])