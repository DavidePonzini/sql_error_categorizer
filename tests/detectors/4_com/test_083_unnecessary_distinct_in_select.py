import pytest
from tests import *

def test_unnecessary():
    query = 'SELECT DISTINCT cid, cname FROM customer'

    result = run_test(
        query,
        search_path='miedema',
        catalog_filename='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_83_UNNECESSARY_DISTINCT_IN_SELECT_CLAUSE) == 1
    assert has_error(result, SqlErrors.COM_83_UNNECESSARY_DISTINCT_IN_SELECT_CLAUSE, ('SELECT DISTINCT cid, cname FROM customer',))

@pytest.mark.parametrize('query', [
    'SELECT DISTINCT c1.cid, c1.cname FROM customer c1 JOIN customer c2 ON c1.cid <> c2.cid',
    'SELECT DISTINCT street FROM customer',
])
def test_necessary(query):
    result = run_test(
        query,
        search_path='miedema',
        catalog_filename='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_83_UNNECESSARY_DISTINCT_IN_SELECT_CLAUSE) == 0
