from tests import *
import pytest

ERROR = SqlErrors.TOO_MANY_COLUMNS_IN_SUBQUERY


@pytest.mark.parametrize('query, subquery', [
    (
        'SELECT col1, col2 FROM main WHERE col3 IN (SELECT col4, col5 FROM subq)',
        'SELECT col4, col5 FROM subq',
    ),
    (
        'SELECT col1 FROM main WHERE col2 = (SELECT col3, col4 FROM subq)',
        'SELECT col3, col4 FROM subq',
    ),
    (
        'SELECT (SELECT col1, col2 FROM subq) AS subquery_result FROM main',
        'SELECT col1, col2 FROM subq',
    ),
])
def test_wrong(query, subquery):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, ERROR) == 1
    assert has_error(detected_errors, ERROR, (subquery, 1))


@pytest.mark.parametrize('query,schema', [
    ('SELECT col1 FROM main WHERE EXISTS (SELECT col2, col3 FROM subq)', 'gen1'),
    ('SELECT col1 FROM main WHERE col2 = (SELECT col3 FROM subq)', 'gen1'),
    ('SELECT col1 FROM (SELECT col2, col3 FROM subq) AS subquery_alias', 'gen1'),
    ('''SELECT customers.full_name,
            loan_totals.total_loan_amount,
            account_totals.total_balance
    FROM customers
    JOIN (
        SELECT borrower_id, SUM(amount) AS total_loan_amount
        FROM loans
        GROUP BY borrower_id
    ) AS loan_totals
    ON loan_totals.borrower_id = customers.cust_id
    JOIN (
        SELECT ref_customer, SUM(balance) AS total_balance
        FROM accounts
        GROUP BY ref_customer
    ) AS account_totals
    ON account_totals.ref_customer = customers.cust_id
    WHERE loan_totals.total_loan_amount > 5000
    AND account_totals.total_balance > (
        SELECT AVG(balance)
        FROM accounts
    );''','gen1'),
    ("select nome, cognome from studenti where not (nome in (select nome from professori) and cognome in (select cognome from professori))", 'unicorsi'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        search_path=schema,
        catalog_filename=schema,
    )

    assert count_errors(detected_errors, ERROR) == 0
