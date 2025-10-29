from tests import *

def test_like_no_wildcards():
    query = "SELECT * FROM employees WHERE name LIKE 'JohnDoe'"

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_88_LIKE_WITHOUT_WILDCARDS) == 1
    assert has_error(result, SqlErrors.COM_88_LIKE_WITHOUT_WILDCARDS, ("name LIKE 'JohnDoe'",))

def test_like_with_underscore():
    query = "SELECT * FROM employees WHERE name LIKE 'John_Doe'"

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_88_LIKE_WITHOUT_WILDCARDS) == 0

def test_like_with_percent():
    query = "SELECT * FROM employees WHERE name LIKE 'John%'"

    result = run_test(
        query,
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_88_LIKE_WITHOUT_WILDCARDS) == 0