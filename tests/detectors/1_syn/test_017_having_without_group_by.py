from tests import *

def test_having_no_group_by():
    detected_errors = run_test(
        query='SELECT * FROM store HAVING id = 1;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_17_HAVING_WITHOUT_GROUP_BY) == 1

def test_having_with_group_by():
    detected_errors = run_test(
        query='SELECT * FROM store HAVING id = 1 GROUP BY id;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_17_HAVING_WITHOUT_GROUP_BY) == 0

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
        catalog_filename='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_17_HAVING_WITHOUT_GROUP_BY) == 1

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
        catalog_filename='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_17_HAVING_WITHOUT_GROUP_BY) == 0

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
        catalog_filename='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_17_HAVING_WITHOUT_GROUP_BY) == 1

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
        catalog_filename='miedema'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_17_HAVING_WITHOUT_GROUP_BY) == 0
