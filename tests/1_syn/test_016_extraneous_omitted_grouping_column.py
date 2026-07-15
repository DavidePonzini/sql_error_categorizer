from tests import *
import pytest

ERROR = SqlErrors.EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN

@pytest.mark.parametrize('query,schema', [
    ('SELECT id, sum(col2) FROM store GROUP BY id', None),
    ('''
        SELECT                                                 
            c.full_name,
            COUNT(t.trans_key) AS total_transactions,
            (
                SELECT AVG(t2.amount)
                FROM transactions t2
                JOIN accounts a2 ON a2.acc_key = t2.related_account
                WHERE a2.balance > 1000
                    AND a2.acc_type = 'Savings'
            ) AS average_transaction_amount
        FROM customers c
        JOIN accounts a ON a.ref_customer = c.cust_id
        JOIN transactions t ON t.related_account = a.acc_key
        WHERE c.full_name LIKE 'Smith___%'
        GROUP BY c.full_name;
    ''', None),
    ('SELECT id, sum(col2) FROM store GROUP BY id, col2', None),
    ('SELECT date, COUNT(*) FROM transaction GROUP BY tid,pid', 'miedema'),
    ('''
        SELECT
            s.nome n,
            s.cognome
        FROM
            Studenti AS s
            JOIN Esami AS e ON e.studente = s.matricola
            JOIN Corsi AS c ON e.corso = c.id AND (c.denominazione = 'Basi Di Dati' OR c.denominazione = 'Interfacce Grafiche')
        WHERE
            (
                c.denominazione = 'Basi Di Dati'
                AND e.data < '06/01/2010'
                AND e.voto >= 18
            ) and (
                c.denominazione = 'Interfacce Grafiche'
                AND e.data < '06/01/2010'
                AND e.voto < 18
            )
        GROUP BY s.matricola
     ''', 'unicorsi'),

])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        search_path=schema,
        catalog_filename=schema,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, ERROR) == 0

@pytest.mark.parametrize('query,schema,errors', [
    (
        'SELECT id, SUM(col2) FROM store GROUP BY 1, 2',
        None,
        [('col2', 'AGGREGATED IN GROUP BY')],
    ),
    (
        'SELECT id, SUM(col2) FROM store GROUP BY id, SUM(col2)',
        None,
        [('col2', 'AGGREGATED IN GROUP BY')],
    ),
    (
        'SELECT id, col2, sum(col3) FROM store GROUP BY id',
        None,
        [('col2', 'ONLY IN SELECT')],
    ),
    (
        'SELECT id, col2, sum(col3) FROM store GROUP BY id, col4',
        None,
        [('col2', 'ONLY IN SELECT')],
    ),
    (
        'SELECT date, COUNT(*) FROM transaction GROUP BY pid',
        'miedema',
        [('date', 'ONLY IN SELECT')],
    ),
])
def test_wrong(query, schema, errors):
    detected_errors = run_test(
        query=query,
        search_path=schema,
        catalog_filename=schema,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, ERROR) == len(errors)
    for error in errors:
        assert has_error(detected_errors, ERROR, error)
