from tests import *

def test_undefined_column():
    detected_errors = run_test(
        query='SELECT id FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN) == 1
    assert has_error(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN, ('id',))

def test_defined_column():
    detected_errors = run_test(
        query='SELECT sid FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN) == 0
    assert not has_error(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN, ('sid',))

def test_undefined_column_where():
    detected_errors = run_test(
        query='SELECT sid FROM store WHERE id > 5;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN) == 1
    assert has_error(detected_errors, SqlErrors.SYN_4_UNDEFINED_COLUMN, ('id',))
