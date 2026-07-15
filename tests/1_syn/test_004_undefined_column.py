from tests import *
import pytest

ERROR = SqlErrors.UNDEFINED_COLUMN

@pytest.mark.parametrize('query,column,schema', [
    ('SELECT id FROM store;', 'id', 'miedema'),
    ('SELECT sid FROM store WHERE id > 5;', 'id', 'miedema'),
    # subqueries
    ('SELECT sid FROM store WHRE sid > ALL(SELECT customer_id FROM customer);', 'customer_id', 'miedema'),
    ('SELECT sid FROM store WHERE sid IN (SELECT customer_id FROM customer);', 'customer_id', 'miedema'),
    ('SELECT sid FROM store WHERE sid > ALL(SELECT customer_id FROM customer);', 'customer_id', 'miedema'),
    
    # CTEs
    ('WITH temp AS (SELECT store_id FROM store) SELECT * FROM temp;', 'store_id', 'miedema'),
    ('WITH temp AS (SELECT sid FROM store) SELECT store_id FROM temp;', 'store_id', 'miedema'),
])
def test_wrong(query, column, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == 1
    assert has_error(detected_errors, ERROR, (column,))

@pytest.mark.parametrize('query,schema', [
    ('SELECT sid FROM store;', 'miedema'),
    ('SELECT sid FROM store', 'miedema, unicorsi'),
    ('SELECT sid FROM store', 'unicorsi, miedema'),
    ('SELECT sid FROM store WHERE street = \'Eindhoven\';', 'miedema'),
    ('SELECT sid FROM store WHERE street = \'Eindhoven\';', 'miedema'),
    ('SELECT cid c FROM customer ORDER BY c;', 'miedema'),
    ('SELECT current_role', 'miedema'),
    ('SELECT current_role FROM customer', 'miedema'),
    # subqueries
    ('SELECT sid FROM store WHERE sid > ALL(SELECT cid FROM customer);', 'miedema'),
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
    );''', 'gen1'),
    ('''select A.studente, A.relatore, A.media
        from (select studenti.relatore, esami.studente, avg(voto) as media
                    from (studenti join professori on studenti.relatore=professori.id) join esami on studenti.matricola=esami.studente
                    group by studenti.relatore, esami.studente) as A
            join (select relatore, max(media) as massimo
                from (select studenti.relatore, esami.studente, avg(voto) as media
                        from (studenti join professori on studenti.relatore=professori.id) join esami on studenti.matricola=esami.studente
                        group by studenti.relatore, esami.studente) as medie
                group by relatore) as B on A.relatore = B.relatore and A.media = B.massimo
        ''', 'unicorsi'),
    # CTEs
    ('WITH temp AS (SELECT sname as name FROM store) SELECT name FROM temp;', 'miedema'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema
    )

    assert count_errors(detected_errors, ERROR) == 0