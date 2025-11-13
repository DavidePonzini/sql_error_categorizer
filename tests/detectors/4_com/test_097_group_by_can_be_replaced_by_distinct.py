import pytest
from tests import *

@pytest.mark.parametrize('query, expected', [
    (
        'SELECT a, b, c, d FROM t GROUP BY a, b, c, d;',
        {('a', None), ('b', None), ('c', None), ('d', None)}
    ),
    (
        'SELECT cname FROM customer GROUP BY cname;',
        {('cname', 0)}
    ),
    (
        'SELECT cid, cname FROM customer, store GROUP BY cid, cname;',
        {('cid', 0), ('cname', 0)}
    ),
])
def test_error(query, expected):
    result = run_test(
        query,
        search_path='miedema',
        catalog_filename='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_97_GROUP_BY_CAN_BE_REPLACED_WITH_DISTINCT) == 1
    assert has_error(result, SqlErrors.COM_97_GROUP_BY_CAN_BE_REPLACED_WITH_DISTINCT, (expected,))

@pytest.mark.parametrize('query', [
    'SELECT a, b, c, d FROM t GROUP BY a, b;',
    'SELECT a, b, COUNT(*) FROM t GROUP BY a, b;',
    'SELECT a, b, c FROM t;',
    'SELECT COUNT(*) FROM t GROUP BY a;',
    'SELECT cid FROM customer, store GROUP BY cid, cname;',
    'SELECT cname, SUM(cid) FROM customer GROUP BY cname;',
])
def test_correct(query):
    result = run_test(
        query,
        search_path='miedema',
        catalog_filename='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_97_GROUP_BY_CAN_BE_REPLACED_WITH_DISTINCT) == 0