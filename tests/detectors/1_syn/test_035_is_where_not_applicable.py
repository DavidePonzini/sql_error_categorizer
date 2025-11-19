from tests import *
import pytest

CATALOG = 'miedema'

@pytest.mark.parametrize('query,errors', [
    ('SELECT * FROM store WHERE sname IS 2;', [('"store"."sname" IS 2', 'int', 'boolean|null')]),
    ('SELECT * FROM store UNION SELECT * FROM product WHERE pname IS TRUE;', [('"product"."pname" IS TRUE', 'varchar', 'boolean')]),
    ('SELECT sid, SUM(sname) FROM store WHERE (sid > 2) IS TRUE AND sname IS (SELECT NULL);', [('"store"."sname" IS (SELECT NULL AS "_col_0")', 'null', 'boolean|null')]),
    # subqueries
    ('SELECT sid, sname FROM store WHERE sid >= (SELECT sid FROM store WHERE sid IS FALSE);', [('"store"."sid" IS FALSE', 'decimal', 'boolean')]),
    ('SELECT * FROM store WHERE sid IN (SELECT sid FROM store WHERE sid >= ALL(SELECT sname FROM store WHERE sid IS 7));', [('"store"."sid" IS 7', 'int', 'boolean|null')]),
    # CTEs
    ('WITH x AS (SELECT sname FROM store WHERE sname IS \'Lidl\' UNION SELECT sname FROM store) SELECT * FROM x;', [('"store"."sname" IS \'Lidl\'', 'varchar', 'boolean|null')]),
])
def test_wrong(query, errors):
    detected_errors = run_test(
        catalog_filename=CATALOG,
        search_path=CATALOG,
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_35_IS_WHERE_NOT_APPLICABLE) == len(errors)
    for snippet in errors:
        assert has_error(detected_errors, SqlErrors.SYN_35_IS_WHERE_NOT_APPLICABLE, snippet)


@pytest.mark.parametrize('query', [
    'SELECT * FROM store WHERE sid IS NOT NULL;',
    'SELECT * FROM store UNION SELECT * FROM customer WHERE (cid > 2) IS TRUE;',
    'SELECT cname FROM costumer JOIN transaction ON costumer.cid=transaction.cid WHERE price >= 20 AND date = CURRENT_TIMESTAMP() AND cname IS NOT NULL;',
    # subqueries
    'SELECT sname FROM store WHERE sid IN (SELECT sid FROM store JOIN transaction ON store.sid=transaction.sid WHERE pid = 5 AND (price < 100) IS TRUE);',
    'SELECT cid, SUM(quantity) FROM shoppinglist GROUP BY cid HAVING SUM(quantity) > (SELECT AVG(quantity) FROM shoppinglist WHERE price IS NOT NULL);'
    # CTEs
    'WITH x AS (SELECT sid FROM store WHERE sname = \'Lidl\' UNION SELECT sid FROM store WHERE sid IS NOT NULL) SELECT * FROM x;',
    'WITH x AS (SELECT AVG(price) AS avg_price FROM transaction WHERE ((price + 2) > 10) IS FALSE) SELECT * FROM transaction WHERE price > (SELECT avg_price FROM x);',
])
def test_correct(query):
    detected_errors = run_test(
        catalog_filename=CATALOG,
        search_path=CATALOG,
        query=query,
        detectors=[SyntaxErrorDetector],
    )

    assert count_errors(detected_errors, SqlErrors.SYN_35_IS_WHERE_NOT_APPLICABLE) == 0
