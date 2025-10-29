from tests import *

def test_undefined_table():
    detected_errors = run_test(
        query='SELECT * FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT) == 1
    assert has_error(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT, ('store',))

def test_defined_table():
    detected_errors = run_test(
        query='SELECT * FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT) == 0
    assert not has_error(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT, ('store',))


def test_undefined_table_cte_name_found():
    detected_errors = run_test(
        query='''
        WITH cte AS (SELECT 1 AS id)
        SELECT * FROM cte;
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT) == 0
    assert not has_error(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT, ('cte',))


def test_undefined_table_cte_name_not_found():
    detected_errors = run_test(
        query='''
        WITH cte AS (SELECT 1 AS id)
        SELECT * FROM cte2;
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT) == 1
    assert has_error(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT, ('cte2',))

def test_undefined_table_cte():
    detected_errors = run_test(
        query='''
        WITH cte AS (SELECT 1 FROM not_a_table)
        SELECT * FROM store;
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT) == 2
    assert has_error(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT, ('not_a_table',))
    assert has_error(detected_errors, SqlErrors.SYN_7_UNDEFINED_OBJECT, ('store',))