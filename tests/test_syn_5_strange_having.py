from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

def test_having_no_group_by():
    detected_errors = run_test(
        query='SELECT * FROM store HAVING id = 1;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert has_error(detected_errors, SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY)

def test_having_with_group_by():
    detected_errors = run_test(
        query='SELECT * FROM store HAVING id = 1 GROUP BY id;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert not has_error(detected_errors, SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY)

def test_having_no_group_by_subquery():
    detected_errors = run_test(
        query='''
        SELECT *
        FROM (
            SELECT * FROM store
            HAVING id = 1;
        ) AS sub
        ''',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        debug=True
    )

    assert has_error(detected_errors, SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY)

def test_having_with_group_by_subquery():
    detected_errors = run_test(
        query='''
        SELECT *
        FROM (
            SELECT * FROM store
            GROUP BY id
            HAVING id = 1;
        ) AS sub
        ''',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert not has_error(detected_errors, SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY)

def test_having_no_group_by_cte():
    detected_errors = run_test(
        query='''
        WITH cte AS (
            SELECT * FROM store
            HAVING id = 1;
        )
        SELECT * FROM cte;
        ''',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert has_error(detected_errors, SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY)

def test_having_with_group_by_cte():
    detected_errors = run_test(
        query='''
        WITH cte AS (
            SELECT * FROM store
            HAVING id = 1 GROUP BY id;
        )
        SELECT * FROM cte;
        ''',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert not has_error(detected_errors, SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY)
