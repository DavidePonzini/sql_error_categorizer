import pytest
from tests import *

def test_no_duplicates():
    query = 'SELECT col1 FROM table1 WHERE col2 = 10 GROUP BY col1 HAVING COUNT(*) > 1'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE) == 0

def test_no_duplicates_with_subquery():
    subquery = 'SELECT col3 FROM table2'
    query = f'SELECT col1 FROM table1 WHERE col2 IN ({subquery}) GROUP BY col1 HAVING COUNT(*) > 1'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE) == 0

def test_with_duplicate_where():
    query = 'SELECT col1 FROM table1 WHERE col2 = 10 WHERE col3 = 20'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE) == 1
    assert has_error(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE, ('WHERE', 2))

def test_with_multiple_duplicates():
    query = 'SELECT col1 FROM table1 WHERE col2 = 10 WHERE col3 = 20 GROUP BY col1 GROUP BY col2 COUNT(*) > 1 HAVING SUM(col4) < 100 GROUP BY col5'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE) == 2
    assert has_error(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE, ('WHERE', 2))
    assert has_error(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE, ('GROUP BY', 3))

def test_multiple_duplicates_with_subquery():
    subquery = 'SELECT col3 FROM table2 WHERE col4 = 30 WHERE col5 = 40 GROUP BY col3 GROUP BY col4 GROUP BY id HAVING COUNT(*) > 2'
    query = f'SELECT col1 SELECT col2 FROM table1 WHERE col2 IN ({subquery}) GROUP BY col3 GROUP BY col4 HAVING COUNT(*) > 1'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE) == 4
    assert has_error(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE, ('SELECT', 2))
    assert has_error(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE, ('WHERE', 2))
    assert has_error(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE, ('GROUP BY', 2))
    assert has_error(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE, ('GROUP BY', 3))

def test_multiple_join_on():
    query = 'SELECT col1 FROM table1 JOIN table2 ON table1.id = table2.id JOIN table3 ON table2.id = table3.id'

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_24_DUPLICATE_CLAUSE) == 0