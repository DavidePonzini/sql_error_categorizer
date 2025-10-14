from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

def test_undefined_column():
    detected_errors = run_test(
        query='SELECT id FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json'
    )

    assert has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_COLUMN, ('id',))

def test_defined_column():
    detected_errors = run_test(
        query='SELECT sid FROM store;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema'
    )

    assert not has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_COLUMN, ('sid',))

def test_undefined_column_where():
    detected_errors = run_test(
        query='SELECT sid FROM store WHERE id > 5;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema'
    )

    assert has_error(detected_errors, SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_COLUMN, ('id',))

def test_ambiguous_column():
    detected_errors = run_test(
        query='''SELECT street FROM store s, customer c;''', 
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema'
    )

    assert any([
        has_error(detected_errors, SqlErrors.SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN, ('street', ['s.street', 'c.street'])),
        has_error(detected_errors, SqlErrors.SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN, ('street', ['c.street', 's.street'])),
    ])

def test_ambiguous_column_no_error():
    detected_errors = run_test(
        query='SELECT s.street FROM store s, customer c;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema'
    )

    assert not any([
        has_error(detected_errors, SqlErrors.SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN, ('street', ['s.street', 'c.street'])),
        has_error(detected_errors, SqlErrors.SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN, ('street', ['c.street', 's.street'])),
    ])

def test_ambiguous_column_where():
    detected_errors = run_test(
        query='SELECT s.street FROM store s, customer c WHERE street = c.street;',
        detectors=[SyntaxErrorDetector],
        catalog_filename='cat_miedema.json',
        search_path='miedema'
    )

    assert any([
        has_error(detected_errors, SqlErrors.SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN, ('street', ['s.street', 'c.street'])),
        has_error(detected_errors, SqlErrors.SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN, ('street', ['c.street', 's.street'])),
    ])