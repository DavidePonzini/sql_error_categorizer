from tests import *
import pytest

ERROR = SqlErrors.UNDEFINED_FUNCTION

@pytest.mark.parametrize('query,func,clause,schema', [
    ('SELECT notafunction() FROM store;', 'notafunction', 'SELECT', 'pg_catalog'),
    ('SELECT anotherfunc(col1, col2) FROM store;', 'anotherfunc', 'SELECT', 'pg_catalog'),
    ('SELECT * FROM store WHERE invalid_func(col1) > 10;', 'invalid_func', 'WHERE', 'pg_catalog'),
    # subqueries
    ('''SELECT * FROM store WHERE col1 IN (SELECT unknown_func(col2) FROM other_table);''', 'unknown_func', 'SELECT', 'pg_catalog'),
    # CTEs
    ('''WITH temp AS (SELECT invalid_func(col) FROM store) SELECT * FROM temp;''', 'invalid_func', 'SELECT', 'pg_catalog'),
])
def test_wrong(query, func, clause, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == 1
    assert has_error(detected_errors, ERROR, (func, clause))

@pytest.mark.parametrize('query,schema', [
    ('SELECT SUM(col1) FROM store;', 'pg_catalog'),
    ('SELECT AVG(col2) FROM customer;', 'pg_catalog'),
    ('SELECT COUNT(*) FROM orders;', 'pg_catalog'),
    ('SELECT cid FROM customer WHERE LENGTH(cname) > 5;', 'pg_catalog'),
    ('SELECT cid FROM customer GROUP BY cid HAVING COUNT(order_id) > 2;', 'pg_catalog'),
    ('SELECT NOW();', 'pg_catalog'),
    ('SELECT col1, COUNT(col2) FILTER (WHERE col3 = 1) as count_col FROM table1 WHERE col4 = 2', 'pg_catalog'),
    ('SELECT EXTRACT(YEAR FROM CURRENT_DATE) AS current_year;', 'pg_catalog'),
    ('SELECT COALESCE(col1, col2, 0) AS result FROM table1;', 'pg_catalog'),
    ('SELECT NULLIF(col1, col2) AS result FROM table1;', 'pg_catalog'),
    ('SELECT UPPER(col1) AS upper_col FROM table1;', 'pg_catalog'),
    ('SELECT LOWER(col1) AS lower_col FROM table1;', 'pg_catalog'),
    ('SELECT DATE_TRUNC(col1, \'month\') AS truncated_date FROM table1;', 'pg_catalog'),
    ('''
        SELECT conname, pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'studenti'::regclass;
    ''', 'pg_catalog'),
    ('''
        SELECT\r
            COUNT(*) FILTER (WHERE c.balance IS NULL) AS total_customers_without_balance,\r
            AVG(l.amount) FILTER (WHERE c.points IS NULL) AS average_loan_amount,\r
            COUNT(*) FILTER (WHERE b.owner_id IS NULL) AS total_branches_without_owner,\r
            CAST(STRING_AGG(c.full_name, ', ') FILTER (WHERE l.borrower_id IS NULL) AS VARCHAR) AS full_name\r
        FROM customers c\r
        FULL JOIN loans l ON c.cust_id = l.borrower_id\r
        FULL JOIN branches b ON c.cust_id = b.owner_id;
    ''', 'pg_catalog'),
    ('''
        SELECT
            full_name AS customer_name,
            balance AS available_balance,
            COUNT(*) OVER() AS "Customer_Count"
        FROM customers
        WHERE balance > 1000
        AND points IS NOT NULL
        AND identifier LIKE 'CUST%';

    ''', 'pg_catalog'),
    # subqueries
    ('SELECT * FROM store WHERE sid >= (SELECT MAX(col1) FROM store);', 'pg_catalog'),
    # CTEs
    ('''WITH temp AS (SELECT MAX(col1) FROM store) SELECT * FROM temp;''', 'pg_catalog'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == 0
