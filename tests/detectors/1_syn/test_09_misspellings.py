from tests import *

def test_misspelling_schema():
    detected_errors = run_test(
        query='SELECT * FROM miedma.store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, ('miedma.store', '"miedema"."store"'))

def test_misspelling_table_with_schema():
    detected_errors = run_test(
        query='SELECT * FROM miedema.stor;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, ('miedema.stor', '"miedema"."store"'))

def test_misspelling_table_without_schema():
    detected_errors = run_test(
        query='SELECT * FROM stor;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, ('stor', '"store"'))


def test_misspelling_column():
    detected_errors = run_test(
        query='SELECT sid FROM store WHERE ID = 1;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, ('ID', '"sid"'))

def test_misspelling_column_case_no_quotes():
    detected_errors = run_test(
        query='SELECT SID FROM store WHERE sID = 1;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 0

def test_misspelling_column_case_with_quotes():
    detected_errors = run_test(
        query='SELECT "Sid" FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, ('"Sid"', '"sid"'))

def test_misspelling_table_no_quotes():
    detected_errors = run_test(
        query='SELECT * FROM STORE;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 0

def test_misspelling_table_with_quotes():
    detected_errors = run_test(
        query='SELECT * FROM "Store";',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, ('"Store"', '"store"'))

def test_misspelling_schema_no_quotes():
    detected_errors = run_test(
        query='SELECT * FROM MIEDEMA.store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 0

def test_misspelling_schema_with_quotes():
    detected_errors = run_test(
        query='SELECT * FROM "MiedeMa".store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, ('"MiedeMa".store', '"miedema"."store"'))