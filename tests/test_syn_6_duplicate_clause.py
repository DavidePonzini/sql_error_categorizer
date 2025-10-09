from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

def test_cte():
    detected_errors = run_test(
        query='''
        WITH cte AS (
            SELECT column1, column2 FROM table1 FROM table2
        )
        SELECT * FROM cte;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'CTE cte'))

def test_multiple_ctes():
    detected_errors = run_test(
        query='''
        WITH cte1 AS (
            SELECT column1, column2 FROM table1
        ),
        cte2 AS (
            SELECT column1, column2 FROM table2 WHERE table3 = table4 FROM table5
        )
        SELECT * FROM cte1 JOIN cte2 ON cte1.column1 = cte2.column1;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'CTE cte2'))

def test_cte_with_multiple_parentheses():
    detected_errors = run_test(
        query='''
        WITH cte AS (
            SELECT column1, column2 FROM table1 WHERE (column3 = 4 AND (column4 = 5 FROM table2))
        )
        SELECT * FROM cte;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'CTE cte'))

def test_correct_cte():
    detected_errors = run_test(
        query='''
        WITH cte AS (
            SELECT column1, column2 FROM table1
        )
        SELECT * FROM cte;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE)

def test_main_query():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1 WHERE column3 = 4 FROM table2;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'MAIN QUERY'))

def test_multiple_clauses_main_query():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1 WHERE column3 = 4 ORDER BY column1 LIMIT 10 ORDER BY column2;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('ORDER BY', 'MAIN QUERY'))
    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'MAIN QUERY'))
    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('WHERE', 'MAIN QUERY'))
    assert not has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('LIMIT', 'MAIN QUERY'))

def test_subquery():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM table1 WHERE column3 = (SELECT column4 FROM table2 FROM table3 WHERE table2.column5 = table3.column5);
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'SUBQUERY'))

def test_multiple_subqueries():
    detected_errors = run_test(
        query='''
        SELECT column1, column2 FROM (SELECT column1, column2 FROM table1 WHERE column3 = 4 ORDER BY column1 ORDER BY column2) AS table1 
        WHERE column3 = (SELECT column4 FROM table2 WHERE table2.column5 FROM table5));
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('ORDER BY', 'SUBQUERY'))
    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'SUBQUERY'))

def test_query_subquery_cte():
    detected_errors = run_test(
        query='''
        WITH cte AS (
            SELECT column1, column2 FROM table1 WHERE column3 = 2 FROM table2;
        )
        SELECT * FROM cte WHERE column1 = (SELECT column4 FROM table3 FROM table4) WHERE column2 = 5;
        ''',
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'CTE cte'))
    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('WHERE', 'MAIN QUERY'))
    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE, ('FROM', 'SUBQUERY'))