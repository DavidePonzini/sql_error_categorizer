from tests import *

def test_grouping_columns_ok():
    detected_errors = run_test(
        query='SELECT id, sum(col2) FROM store GROUP BY id',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN) == 0

def test_extraneous_grouping_column():
    detected_errors = run_test(
        query='SELECT id, sum(col2) FROM store GROUP BY id, col2',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN) == 1
    assert has_error(
        detected_errors,
        SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,
        ('col2', 'ONLY IN GROUP BY'),
    )

def test_omitted_grouping_column():
    detected_errors = run_test(
        query='SELECT id, col2, sum(col3) FROM store GROUP BY id',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN) == 1
    assert has_error(
        detected_errors,
        SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,
        ('col2', 'ONLY IN SELECT'),
    )

def test_extraneous_and_omitted_grouping_columns():
    detected_errors = run_test(
        query='SELECT id, col2, sum(col3) FROM store GROUP BY id, col4',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN) == 2
    
    assert has_error(
        detected_errors,
        SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,
        ('col2', 'ONLY IN SELECT'),
    )

    assert has_error(
        detected_errors,
        SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,
        ('col4', 'ONLY IN GROUP BY'),
    )