from tests import *

def test_end():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1;;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_38_ADDITIONAL_SEMICOLON) == 1
    assert count_errors(detected_errors, SqlErrors.SYN_22_OMITTING_THE_SEMICOLON) == 0

def test_middle():
    detected_errors = run_test(
        query='''
        SELECT column1, column2; FROM table1
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_38_ADDITIONAL_SEMICOLON) == 1
    assert count_errors(detected_errors, SqlErrors.SYN_22_OMITTING_THE_SEMICOLON) == 1

def test_beginning():
    detected_errors = run_test(
        query='''
        ;SELECT column1, column2 FROM table1;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_38_ADDITIONAL_SEMICOLON) == 1
    assert count_errors(detected_errors, SqlErrors.SYN_22_OMITTING_THE_SEMICOLON) == 0

def test_correct():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_38_ADDITIONAL_SEMICOLON) == 0
    assert count_errors(detected_errors, SqlErrors.SYN_22_OMITTING_THE_SEMICOLON) == 0

def test_none():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_38_ADDITIONAL_SEMICOLON) == 0
    assert count_errors(detected_errors, SqlErrors.SYN_22_OMITTING_THE_SEMICOLON) == 1
