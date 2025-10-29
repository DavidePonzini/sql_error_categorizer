from tests import *

def test_percent():
    query = "SELECT * FROM employees WHERE name = 'John%'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 1
    assert has_error(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE, ("name = 'John%'",))

def test_underscore():
    query = "SELECT * FROM employees WHERE name = 'John_Doe'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 1
    assert has_error(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE, ("name = 'John_Doe'",))

def test_no_wildcards():
    query = "SELECT * FROM employees WHERE name = 'JohnDoe'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 0

def test_like_with_wildcards():
    query = "SELECT * FROM employees WHERE name LIKE 'John%'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 0

def test_regular_equality():
    query = "SELECT * FROM employees WHERE name = 'JohnDoe'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 0

def test_left_expression():
    query = "SELECT * FROM employees WHERE 'John%' = name"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 1
    assert has_error(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE, ("'John%' = name",))

def test_underscore_in_solution():
    query = "SELECT * FROM employees WHERE name = 'John_Doe'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[
            "SELECT * FROM employees WHERE name = 'John_Doe'",
        ]
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 0

def test_percent_in_solution():
    query = "SELECT * FROM employees WHERE status = '100%'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[
            "SELECT * FROM employees WHERE status = '100%'",
        ]
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 0

def test_only_one_in_solution():
    query = "SELECT * FROM employees WHERE name = 'John%' or name = 'Jane_'"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        solutions=[
            "SELECT * FROM employees WHERE name = 'Jane_'",
        ]
    )

    assert count_errors(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE) == 1
    assert has_error(result, SqlErrors.SEM_43_WILDCARDS_WITHOUT_LIKE, ("name = 'John%'",))