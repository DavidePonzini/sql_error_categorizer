from tests import *
import pytest

ERROR = SqlErrors.MISSPELLINGS

@pytest.mark.parametrize('query,expected_corrections', [
    ('SELECT * FROM miedma.store;', [('miedma.store', '"miedema"."store"')]),
    ('SELECT * FROM miedema.stor;', [('miedema.stor', '"miedema"."store"')]),
    ('SELECT * FROM stor;', [('stor', '"store"')]),
    ('SELECT sid FROM store WHERE ID = 1;', [('ID', '"sid"')]),
    ('SELECT "Sid" FROM store;', [('"Sid"', '"sid"')]),
    ('SELECT * FROM "Store";', [('"Store"', '"store"')]),
    ('SELECT * FROM "MiedeMa".store;', [('"MiedeMa".store', '"miedema"."store"')]),
    # subqueries
    ('SELECT * FROM miedema.store WHERE sID IN (SELECT id FROM store);', [('id', '"sid"')]),
    # CTEs
    ('WITH temp AS (SELECT * FROM stores) SELECT * FROM temp;', [('stores', '"store"')]),
])
def test_wrong(query, expected_corrections):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema',
    )

    assert count_errors(detected_errors, ERROR) == len(expected_corrections)
    for correction in expected_corrections:
        assert has_error(detected_errors, ERROR, correction)

@pytest.mark.parametrize('query,schema', [
    ('SELECT SID FROM store;', 'miedema'),
    ('SELECT SID FROM store WHERE sID = 1;', 'miedema'),
    ('SELECT * FROM STORE;', 'miedema'),
    ('SELECT * FROM MIEDEMA.store;', 'miedema'),
    # subqueries
    ('SELECT * FROM store WHERE sid IN (SELECT sid FROM store);', 'miedema'),
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
    ('WITH temp AS (SELECT * FROM store) SELECT * FROM temp;', 'miedema'),
])
def test_correct(query, schema):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename=schema,
        search_path=schema,
    )

    assert count_errors(detected_errors, ERROR) == 0
