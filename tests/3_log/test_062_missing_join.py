from tests import *
import pytest

ERROR = SqlErrors.LOG_62_MISSING_JOIN

@pytest.mark.parametrize('query, solutions, expected_errors, schema, search_path', [
    (
        # expected: products orders [customers]
        # provided:          orders
        'SELECT * FROM orders o;',
        [
            'SELECT * FROM orders o, products p;',
            'SELECT * FROM orders o, products p, customers c;',
        ],
        [('myschema', 'products')],
        None,
        'myschema',
    ),
    # subqueries
    (
        # expected: table1 table2 table3 table4
        # provided: table1 table2        table4
        'SELECT * FROM table1 WHERE a IN (SELECT * FROM table2 WHERE EXISTS (SELECT * FROM table4)) AS sub;',
        ['SELECT * FROM table1 WHERE a IN (SELECT * FROM table2, table3 WHERE EXISTS (SELECT * FROM table4)) AS sub;'],
        [('myschema', 'table3')],
        None,
        'myschema',
    ),
    # CTEs
    (
        # expected: table1 table2        table4
        # provided: table1
        'WITH cte AS (SELECT * FROM table1) SELECT * FROM cte;',
        ['WITH cte AS (SELECT * FROM table1 JOIN table2 ON table1.a = table2.a) SELECT * FROM cte, table4;'],
        [('myschema', 'table2'), ('myschema', 'table4')],
        None,
        'myschema',
    ),
    
],)
def test_wrong(query: str, solutions: list[str], expected_errors: list[tuple[str, str]], schema: str | None, search_path: str):
    detected_errors = run_test(
        query=query,
        solutions=solutions,
        catalog_filename=schema,
        search_path=search_path,
        detectors=[LogicalErrorDetector],
    )

    assert count_errors(detected_errors, ERROR) == len(expected_errors)
    for expected_error in expected_errors:
        assert DetectedError(ERROR, expected_error) in detected_errors

@pytest.mark.parametrize('query, solutions, schema', [
    (
        # expected: customers store
        # provided: customers       transaction
        'SELECT cid, sid FROM customers c JOIN store s ON c.street = s.street;',
        ['SELECT cid, sid FROM customers c JOIN transaction t ON c.cid = t.cid;'],
        'miedema'
    ),
    (
        # expected: products orders [customers]
        # provided: products                    table1
        'SELECT * FROM orders, table1 p;',
        [
            'SELECT * FROM orders o,products p;',
            'SELECT * FROM orders o, products p, customers c;',
        ],
        None
    ),
    (
        # expected: customers transaction
        # provided: customers transaction
        'SELECT cid, sid FROM customers c JOIN transaction t ON c.cid = t.cid;',
        ['SELECT cid, sid FROM customers c JOIN transaction t ON c.cid = t.cid;'],
        'miedema'
    ),
    (
        # expected: products orders [customers]
        # provided: products orders
        'SELECT * FROM orders o, products p;',
        [
            'SELECT * FROM orders o, products p;',
            'SELECT * FROM orders o, products p, customers c;',
        ],
        None
    ),
    (
        # expected: products orders [customers]
        # provided: products orders  customers
        'SELECT * FROM orders o, products p, customers c;',
        [
            'SELECT * FROM orders o, products p;',
            'SELECT * FROM orders o, products p, customers c;',
        ],
        None
    ),
    (
        # expected: products orders [customers]
        # provided: products orders  customers  users
        'SELECT * FROM orders o, products p, customers c, users u;',
        [
            'SELECT * FROM orders o, products p;',
            'SELECT * FROM orders o, products p, customers c;',
        ],
        None,
    ),
    (
        # no solutions (return no errors)
        'SELECT a, b, c FROM table1 JOIN table2 ON table1.id = table2.id;',
        [],
        None,
    ),
    # subqueries
    (
        # expected: table1 table2 table3 table4
        # provided: table1 table2        table4 table5
        'SELECT * FROM table1 WHERE a IN (SELECT * FROM table2, table4 WHERE EXISTS (SELECT * FROM table5)) AS sub;',
        ['SELECT * FROM table1 WHERE a IN (SELECT * FROM table2, table3 WHERE EXISTS (SELECT * FROM table4)) AS sub;'],
        None,
    ),
    
    (
        # expected: table1 table2 table3 table4
        # provided: table1 table2 table3 table4
        'SELECT * FROM table1 WHERE a IN (SELECT * FROM table2, table3 WHERE EXISTS (SELECT * FROM table4)) AS sub;',
        ['SELECT * FROM table1 WHERE a IN (SELECT * FROM table2, table3 WHERE EXISTS (SELECT * FROM table4)) AS sub;'],
        None
    ),
    (
        # expected: table1 table2 table3 table4
        # provided: table1 table2 table3 table4 table5
        'SELECT * FROM table1 WHERE a IN (SELECT * FROM table2, table3, table5 WHERE EXISTS (SELECT * FROM table4)) AS sub;',
        ['SELECT * FROM table1 WHERE a IN (SELECT * FROM table2, table3 WHERE EXISTS (SELECT * FROM table4)) AS sub;'],
        None,
    ),
    # CTEs
    (
        # expected: table1 table2        table4
        # provided: table1        table3 table4
        'WITH cte AS (SELECT * FROM table1, table3) SELECT * FROM cte JOIN table4 ON cte.b = table4.b;',
        ['WITH cte AS (SELECT * FROM table1 JOIN table2 ON table1.a = table2.a) SELECT * FROM cte, table4;'],
        None,
    ),
    (
        # expected: table1 table2        table4
        # provided: table1 table2        table4
        'WITH cte AS (SELECT * FROM table1 JOIN table2 ON table1.a = table2.a) SELECT * FROM cte, table4;',
        ['WITH cte AS (SELECT * FROM table1 JOIN table2 ON table1.a = table2.a) SELECT * FROM cte, table4;'],
        None
    ),
    (
        # expected: table1 table2        table4
        # provided: table1 table2 table3 table4
        'WITH cte AS (SELECT * FROM table1, table2, table3) SELECT * FROM cte JOIN table4 ON cte.b = table4.b;',
        ['WITH cte AS (SELECT * FROM table1 JOIN table2 ON table1.a = table2.a) SELECT * FROM cte, table4;'],
        None,
    ),

])
def test_correct(query: str, solutions: list[str], schema: str | None):
    detected_errors = run_test(
        query=query,
        solutions=solutions,
        catalog_filename=schema,
        search_path=schema,
        detectors=[LogicalErrorDetector],
    )

    assert count_errors(detected_errors, ERROR) == 0