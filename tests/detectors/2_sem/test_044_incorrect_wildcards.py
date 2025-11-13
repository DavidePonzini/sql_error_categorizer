from tests import *

def test_star_no_solutions():
    query = "SELECT * FROM employees WHERE name LIKE 'John*'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 1
    assert has_error(result, SqlErrors.SEM_44_INCORRECT_WILDCARD, ("name LIKE 'John*'",))

def test_star_with_solution_using_star():
    query = "SELECT * FROM employees WHERE name LIKE 'John*'"
    solution = "SELECT * FROM employees WHERE name LIKE 'John*'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 0

def test_star_with_solution_using_percent():
    query = "SELECT * FROM employees WHERE name LIKE 'John*'"
    solution = "SELECT * FROM employees WHERE name LIKE 'John%'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 1
    assert has_error(result, SqlErrors.SEM_44_INCORRECT_WILDCARD, ("name LIKE 'John*'",))

def test_question_mark_no_solutions():
    query = "SELECT * FROM employees WHERE name LIKE 'J?hn'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 1
    assert has_error(result, SqlErrors.SEM_44_INCORRECT_WILDCARD, ("name LIKE 'J?hn'",))

def test_question_mark_with_solution_using_question_mark():
    query = "SELECT * FROM employees WHERE name LIKE 'J?hn'"
    solution = "SELECT * FROM employees WHERE name LIKE 'J?hn'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 0

def test_question_mark_with_solution_using_underscore():
    query = "SELECT * FROM employees WHERE name LIKE 'J?hn'"
    solution = "SELECT * FROM employees WHERE name LIKE 'J_hn'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 1
    assert has_error(result, SqlErrors.SEM_44_INCORRECT_WILDCARD, ("name LIKE 'J?hn'",))

def test_percent_no_solutions():
    query = "SELECT * FROM employees WHERE name LIKE 'John%'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 0

def test_percent_with_solution_using_underscore():
    query = "SELECT * FROM employees WHERE name LIKE 'John%'"
    solution = "SELECT * FROM employees WHERE name LIKE 'John_'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 1
    assert has_error(result, SqlErrors.SEM_44_INCORRECT_WILDCARD, ("name LIKE 'John%'",))

def test_underscore_no_solutions():
    query = "SELECT * FROM employees WHERE name LIKE 'J_hn'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 0

def test_underscore_with_solution_using_percent():
    query = "SELECT * FROM employees WHERE name LIKE 'J_hn'"
    solution = "SELECT * FROM employees WHERE name LIKE 'J%hn'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 1
    assert has_error(result, SqlErrors.SEM_44_INCORRECT_WILDCARD, ("name LIKE 'J_hn'",))

def test_underscore_with_solution_using_percent_underscore():
    query = "SELECT * FROM employees WHERE name LIKE 'J_hn_'"
    solution = "SELECT * FROM employees WHERE name LIKE 'J_hn%'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 0

def test_percent_with_solution_using_underscore_percent():
    query = "SELECT * FROM employees WHERE name LIKE 'J%hn%'"
    solution = "SELECT * FROM employees WHERE name LIKE 'J_hn%'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[solution]
    )

    assert count_errors(result, SqlErrors.SEM_44_INCORRECT_WILDCARD) == 0