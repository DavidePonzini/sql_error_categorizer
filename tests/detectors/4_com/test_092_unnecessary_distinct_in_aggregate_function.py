from tests import *
import pytest

@pytest.mark.parametrize('query,expected', [
    (
        'SELECT SUM(DISTINCT sid) FROM store',
        ['SUM(DISTINCT sid)']
    ),
    (
        'SELECT COUNT(DISTINCT sid) FROM store',
        ['COUNT(DISTINCT sid)']
    ),
    (
        'SELECT MIN(DISTINCT sid) FROM store',
        ['MIN(DISTINCT sid)']
    ),
    (
        'SELECT MAX(DISTINCT sid) FROM store',
        ['MAX(DISTINCT sid)']
    ),
    (
        'SELECT AVG(DISTINCT sid) FROM store',
        ['AVG(DISTINCT sid)']
    ),
    (
        'SELECT MIN(DISTINCT quantity) FROM shoppinglist',
        ['MIN(DISTINCT quantity)']
    ),
    (
        'SELECT MAX(DISTINCT quantity) FROM shoppinglist',
        ['MAX(DISTINCT quantity)']
    ),
    (
        'SELECT COUNT(DISTINCT sid), MIN(DISTINCT cname), SUM(DISTINCT street)  FROM store',
        ['COUNT(DISTINCT sid)', 'MIN(DISTINCT cname)']
    ),
    (
        'WITH temp AS (SELECT MIN(DISTINCT sid) AS min_sid FROM store) SELECT * FROM temp',
        ['MIN(DISTINCT sid)']
    ),
    (
        'SELECT * FROM store WHERE sid IN (SELECT MAX(DISTINCT sid) FROM store)',
        ['MAX(DISTINCT sid)']
    ),
])
def test_unnecessary(query: str, expected: list[str]):
    result = run_test(
        query,
        catalog_filename='miedema',
        search_path='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_92_UNNECESSARY_DISTINCT_IN_AGGREGATE_FUNCTION) == len(expected)
    
    for exp_str in expected:
        assert has_error(result, SqlErrors.COM_92_UNNECESSARY_DISTINCT_IN_AGGREGATE_FUNCTION, (exp_str,))

@pytest.mark.parametrize('query', [
    'SELECT MIN(sid) FROM store',
    'SELECT MAX(sid) FROM store',
    'SELECT SUM(sid) FROM store',
    'SELECT COUNT(sid) FROM store',
    'SELECT AVG(sid) FROM store',
    'SELECT SUM(DISTINCT cname) FROM store',  # cname is not unique
    'SELECT COUNT(DISTINCT street) FROM store',  # street is not unique
    'SELECT AVG(DISTINCT quantity) FROM shoppinglist',  # quantity is not unique
])
def test_distinct_necessary(query: str):
    result = run_test(
        query,
        catalog_filename='miedema',
        search_path='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_92_UNNECESSARY_DISTINCT_IN_AGGREGATE_FUNCTION) == 0