from tests import *
import pytest

ERROR = SqlErrors.SYN_13_DATA_TYPE_MISMATCH
CATALOG = 'miedema'

@pytest.mark.parametrize('query,errors', [
    ('SELECT * FROM store WHERE sname > 2;', [('"store"."sname" > 2', 'varchar & int', None)]),
    ('SELECT * FROM store UNION SELECT * FROM product;', [('Main Query', 'setop types inconsistent')]),
    ('SELECT sid, SUM(sname) FROM store WHERE sname LIKE 3 AND sid BETWEEN \'a\' AND 10;', [('SUM("store"."sname")', 'varchar', 'numeric'), ('"store"."sname" LIKE 3', 'int', 'string'), ('"store"."sid" BETWEEN \'a\' AND 10', 'varchar', 'decimal')]),
    # subqueries
    ('SELECT sid, sname FROM store WHERE sid >= (SELECT AVG(sname),sid FROM store);', [('AVG("store"."sname")', 'varchar', 'numeric'), ('"store"."sid" >= (SELECT AVG("store"."sname") AS "_col_0", "store"."sid" AS "sid" FROM "miedema"."store" AS "store")', 'decimal & list', None)]),
    ('SELECT * FROM store WHERE sid IN (SELECT sid FROM store WHERE sid >= ALL(SELECT sname FROM store));', [('"store"."sid" >= ALL (SELECT "store"."sname" AS "sname" FROM "miedema"."store" AS "store")', 'decimal & varchar', None)]),
    ('SELECT s.sid FROM store s WHERE s.sid >= (SELECT AVG(p.pid) FROM product p WHERE s.name >= p.pid);', [('"s"."sname" >= "p"."pid"', 'varchar & decimal', None)]),
    # CTEs
    ('WITH x AS (SELECT sid FROM store WHERE sname IS \'Lidl\' UNION SELECT sname FROM store WHERE sid IN (1,2,\'ciao\')) SELECT * FROM x;', [('"store"."sname" IS \'Lidl\'', 'varchar', 'boolean|null'),('CTE ', 'setop types inconsistent'),('"store"."sid" IN (1, 2, \'ciao\')', 'varchar', 'decimal')]),
])
def test_wrong(query, errors):
    detected_errors = run_test(
        catalog_filename=CATALOG,
        search_path=CATALOG,
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, ERROR) == len(errors)
    for snippet in errors:
        assert has_error(detected_errors, ERROR, snippet)


@pytest.mark.parametrize('query', [
    'SELECT * FROM store WHERE sid BETWEEN 1 AND 5;',
    'SELECT * FROM store UNION SELECT * FROM customer;',
    'SELECT cname FROM costumer JOIN transaction ON costumer.cid=transaction.cid WHERE price >= 20 AND date = CURRENT_TIMESTAMP()',
    # subqueries
    'SELECT sname FROM store WHERE sid IN (SELECT sid FROM store JOIN transaction ON store.sid=transaction.sid WHERE pid = 5)',
    'SELECT cid, SUM(quantity) FROM shoppinglist GROUP BY cid HAVING SUM(quantity) > (SELECT AVG(quantity) FROM shoppinglist);'
    # CTEs
    'WITH x AS (SELECT sid FROM store WHERE sname = \'Lidl\' UNION SELECT sid FROM store WHERE sid IN (1,2,3)) SELECT * FROM x;',
    'WITH x AS (SELECT AVG(price) AS avg_price FROM transaction) SELECT * FROM transaction WHERE price > (SELECT avg_price FROM x);',
])
def test_correct(query):
    detected_errors = run_test(
        catalog_filename=CATALOG,
        search_path=CATALOG,
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, ERROR) == 0
