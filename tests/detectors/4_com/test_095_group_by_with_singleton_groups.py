import pytest
from tests import *
from sql_error_categorizer.catalog import Constraint, ConstraintColumn

@pytest.mark.parametrize('query, group_by_constraint, singleton_constraint', [
    (
        'SELECT DISTINCT cid, cname FROM customer GROUP BY cid, cname HAVING COUNT(*) = 1;',
        Constraint({ConstraintColumn('cid', 0), ConstraintColumn('cname', 0)}),
        Constraint({ConstraintColumn('cid', 0)})
    ),
    (
        'SELECT cid, cname FROM customer GROUP BY cid, cname HAVING COUNT(*) = 1;',
        Constraint({ConstraintColumn('cid', 0), ConstraintColumn('cname', 0)}),
        Constraint({ConstraintColumn('cid', 0)})
    ),
    (
        'SELECT COUNT(*) FROM customer GROUP BY cid;',
        Constraint({ConstraintColumn('cid', 0)}),
        Constraint({ConstraintColumn('cid', 0)})
    ),
    (
        'SELECT cid, COUNT(*) FROM customer, store GROUP BY cid, sid;',
        Constraint({ConstraintColumn('cid', 0), ConstraintColumn('sid', 1)}),
        Constraint({ConstraintColumn('cid', 0), ConstraintColumn('sid', 1)})
    )

])
def test_singleton(query, group_by_constraint, singleton_constraint):
    result = run_test(
        query,
        search_path='miedema',
        catalog_filename='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_95_GROUP_BY_WITH_SINGLETON_GROUPS) == 1
    assert has_error(result, SqlErrors.COM_95_GROUP_BY_WITH_SINGLETON_GROUPS, (group_by_constraint, singleton_constraint))

@pytest.mark.parametrize('query', [
    'SELECT DISTINCT cname, COUNT(*) FROM customer GROUP BY cname',
    'SELECT cid, cname, COUNT(*) FROM customer, store GROUP BY cid, cname;',
    'SELECT cid, COUNT(*) FROM customer, store GROUP BY cid;',
])
def test_necessary(query):
    result = run_test(
        query,
        search_path='miedema',
        catalog_filename='miedema',
        detectors=[ComplicationDetector],
    )

    assert count_errors(result, SqlErrors.COM_95_GROUP_BY_WITH_SINGLETON_GROUPS) == 0
