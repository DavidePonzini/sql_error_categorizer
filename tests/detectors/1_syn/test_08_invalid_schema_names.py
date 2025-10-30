from tests import *

def test_invalid_schema():
    detected_errors = run_test(
        query='SELECT * FROM notaschema.store;',
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_8_INVALID_SCHEMA_NAME) == 1
    assert has_error(detected_errors, SqlErrors.SYN_8_INVALID_SCHEMA_NAME, ('notaschema.store',))

def test_valid_schema():
    detected_errors = run_test(
        query='SELECT * FROM miedema.store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert count_errors(detected_errors, SqlErrors.SYN_8_INVALID_SCHEMA_NAME) == 0