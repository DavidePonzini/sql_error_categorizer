from tests import *
import pytest

@pytest.mark.parametrize('query,expected_corrections', [
    ('SELECT * FROM miedma.store;', [('miedma.store', '"miedema"."store"')]),
    ('SELECT * FROM miedema.stor;', [('miedema.stor', '"miedema"."store"')]),
    ('SELECT * FROM stor;', [('stor', '"store"')]),
    ('SELECT sid FROM store WHERE ID = 1;', [('ID', '"sid"')]),
    ('SELECT "Sid" FROM store;', [('"Sid"', '"sid"')]),
    ('SELECT * FROM "Store";', [('"Store"', '"store"')]),
    ('SELECT * FROM "MiedeMa".store;', [('"MiedeMa".store', '"miedema"."store"')]),
])
def test_wrong(query, expected_corrections):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema',
        debug=True,
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == len(expected_corrections)
    for correction in expected_corrections:
        assert has_error(detected_errors, SqlErrors.SYN_9_MISSPELLINGS, correction)

@pytest.mark.parametrize('query', [
    'SELECT SID FROM store;',
    'SELECT SID FROM store WHERE sID = 1;',
    'SELECT * FROM STORE;',
    'SELECT * FROM MIEDEMA.store;',
])
def test_correct(query):
    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector],
        catalog_filename='miedema',
        search_path='miedema',
    )

    assert count_errors(detected_errors, SqlErrors.SYN_9_MISSPELLINGS) == 0
