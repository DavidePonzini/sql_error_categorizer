from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

def test_end():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1;;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON)
    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON)

def test_middle():
    detected_errors = run_test(
        query='''
        SELECT column1, column2; FROM table1
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON)
    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON)

def test_beginning():
    detected_errors = run_test(
        query='''
        ;SELECT column1, column2 FROM table1
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON)
    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON)

def test_correct():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON)
    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON)

def test_none():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON)
    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON)
