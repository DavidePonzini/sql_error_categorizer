from tests import *
import pytest
import itertools

ERROR = SqlErrors.AMBIGUOUS_COLUMN

@pytest.mark.parametrize('query,column,table_aliases,schema', [
    ('SELECT street FROM store s, customer c;', 'street', ['s.street', 'c.street'], 'miedema'),
    ('SELECT s.street FROM store s, customer c WHERE street = c.street;', 'street', ['s.street', 'c.street'], 'miedema'),
    ('select street from store natural join customer n join customer c on c.cid = n.cid;', 'street', ['NATURAL JOIN(store,n).street', 'c.street'], 'miedema'),
    # subqueries
    ('SELECT * FROM store s, customer c WHERE cid IN (SELECT street FROM store s2, customer c2);', 'street', ['s2.street', 'c2.street'], 'miedema'),
    # CTEs
    ('WITH temp AS (SELECT street FROM store s, customer c) SELECT street FROM temp;', 'street', ['s.street', 'c.street'], 'miedema'),
])
def test_wrong(query, column, table_aliases, schema):
    detected_errors = run_test(
        query=query, 
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == 1
    assert any([ has_error(detected_errors, ERROR, (column, list(perm))) for perm in itertools.permutations(table_aliases) ])

@pytest.mark.parametrize('query,schema', [
    ('SELECT s.street FROM store s, customer c;', 'miedema'),
    ('SELECT s.* FROM store s, customer c;', 'miedema'),
    ('select professori.cognome,professori.nome, count(studenti.matricola) from studenti right outer join professori on studenti.relatore=professori.id group by professori.cognome,professori.nome order by professori.cognome,professori.nome asc;', 'unicorsi'),
    ("SELECT DISTINCT studente FROM Studenti s JOIN CorsiDiLaurea c ON s.corsodilaurea = c.id JOIN Esami e ON s.matricola = e.studente WHERE c.denominazione = 'Informatica' AND e.corso = 'bdd1n' AND e.voto >= 18 AND s.matricola NOT IN (SELECT studente FROM Esami WHERE corso = 'graf' AND voto >= 18 AND data >= '06/01/2010' AND Data <= '06/30/2010');", 'unicorsi'),
    ("select Cognome, Nome, Relatore from Studenti natural join Professori order by Cognome asc;", 'unicorsi'),
    ("SELECT studenti.cognome, studenti.nome, professori.cognome AS relatore FROM studenti JOIN professori ON studenti.relatore = professori.id ORDER BY cognome, nome ASC;", 'unicorsi'),
    ("select distinct matricola as studente from studenti join CorsiDiLaurea on corsodilaurea=CorsiDiLaurea.Id join Esami on matricola=Studente join Corsi on Corsi.Id=Corso where CorsiDiLaurea.denominazione='9' and Corso= 'bdd1n' or Corso ='ig' and Data<'06/01/2010' or Data>'06/30/2010'", 'unicorsi'),
    # subqueries
    ('SELECT * FROM store s, customer c WHERE cid IN (SELECT s2.street FROM store s2, customer c2);', 'miedema'),
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
    ("select nome, cognome from studenti where not (nome in (select nome from professori) and cognome in (select cognome from professori))", 'unicorsi'),
    ('''select  p.Cognome ,p.Nome , count(c.id) as numerocorsi
        from Professori p
        join Corsi c on c.Professore = p.id
        group by p.id, p.Cognome , p.Nome
        having count(c.id) = (
                    select max(cnt) from (
                        select count(*) as cnt from Corsi
                        group by Professore
                    )t
         )''', 'unicorsi'),
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
    ('''SELECT customers.full_name,
                loan_totals.total_loan_amount,
                account_totals.total_balance
        FROM customers
        JOIN (
            SELECT borrower_id, SUM(amount) AS total_loan_amount
            FROM loans
            GROUP BY borrower_id
        ) loan_totals
        ON loan_totals.borrower_id = customers.cust_id
        JOIN (
            SELECT ref_customer, SUM(balance) AS total_balance
            FROM accounts
            GROUP BY ref_customer
        ) account_totals
        ON account_totals.ref_customer = customers.cust_id
        WHERE loan_totals.total_loan_amount > 5000
        AND account_totals.total_balance > (
            SELECT AVG(total_balance)
            FROM (
                SELECT SUM(balance) AS total_balance
                FROM accounts
                GROUP BY ref_customer
            ) avg_table
        );
        ''', 'gen1'),
    # CTEs
    ('WITH temp AS (SELECT s.street FROM store s, customer c) SELECT street FROM temp;', 'miedema'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema
    )

    assert count_errors(detected_errors, ERROR) == 0
