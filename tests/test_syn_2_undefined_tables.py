from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

def test_undefined_table():
    detected_errors = run_test(
        query='SELECT * FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT, ('store',))

def test_defined_table():
    detected_errors = run_test(
        query='SELECT * FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema'
    )

    assert not has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT, ('store',))


def test_undefined_table_cte_name_found():
    detected_errors = run_test(
        query='''
        WITH cte AS (SELECT 1 AS id)
        SELECT * FROM cte;
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert not has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT, ('cte',))


def test_undefined_table_cte_name_not_found():
    detected_errors = run_test(
        query='''
        WITH cte AS (SELECT 1 AS id)
        SELECT * FROM cte2;
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT, ('cte2',))

def test_undefined_table_cte():
    detected_errors = run_test(
        query='''
        WITH cte AS (SELECT 1 FROM not_a_table)
        SELECT * FROM store;
        ''',
        detectors=[SyntaxErrorDetector],
    )

    assert has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT, ('not_a_table',))