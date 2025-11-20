from tests import *
import pytest

ERROR = SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION


@pytest.mark.parametrize('query,expected_errors,dataset', [
    (
        "SELECT * FROM orders WHERE a = a",
        [('tautology',)],
        None
    ),
    (
        "SELECT * FROM orders WHERE 1 = 0",
        [('contradiction',), ('redundant_disjunct', '1 = 0')],
        None
    ),
    (
        "SELECT * FROM orders WHERE (sal < 500 AND comm > 1000) OR sal >= 500",
        [('redundant_conjunct', ('sal < 500 AND comm > 1000', 'sal < 500'))],
        None
    ),
    (
        "SELECT * FROM orders WHERE sal > 500 OR sal > 1000",
        [('redundant_disjunct', 'sal > 1000')],
        None
    ),
    (
        "SELECT * FROM store WHERE sname <= 'Coop' OR sname IN ('Coop', 'Lidl') OR sname >= 'Lidl';",
        [('redundant_disjunct', "sname IN ('Coop', 'Lidl')")],
        'miedema'
    ),
    # subqueries
    pytest.param(
        "SELECT * FROM orders WHERE amount > (SELECT MAX(amount) FROM orders) OR amount <= (SELECT MAX(amount) FROM orders);",
        [('redundant_disjunct', 'amount <= (SELECT MAX(amount) FROM orders)')],
        None,
        marks=pytest.mark.xfail(reason="Subquery comparison handling not implemented yet"),
    ),
    pytest.param(
        "SELECT * FROM orders WHERE amount < (SELECT MIN(amount) FROM orders) AND amount >= (SELECT MIN(amount) FROM orders);",
        [('redundant_conjunct', ('amount < (SELECT MIN(amount) FROM orders) AND amount >= (SELECT MIN(amount) FROM orders)', 'amount < (SELECT MIN(amount) FROM orders)'))],
        None,
        marks=pytest.mark.xfail(reason="Subquery comparison handling not implemented yet"),
    ),
    (
        'SELECT * FROM employees WHERE department_id IN (SELECT department_id FROM departments WHERE location_id = 1700 OR location_id >= 1700);',
        [('redundant_disjunct', 'location_id = 1700')],
        None
    ),
    pytest.param(
        'SELECT * FROM employees WHERE department_id >= (SELECT MIN(department_id) FROM departments',
        [('tautology',)],
        None,
        marks=pytest.mark.xfail(reason="Subquery comparison handling not implemented yet"),
    ),
    # CTEs
    pytest.param(
        'WITH dept_cte AS (SELECT department_id FROM departments WHERE department_id < 100) SELECT * FROM dept_cte WHERE department_id > 100;',
        [('contradiction',)],
        None,
        marks=pytest.mark.xfail(reason="CTE handling not implemented yet"),
    ),
])
def test_wrong(query, expected_errors, dataset):
    detected_errors = run_test(
        query,
        detectors=[SemanticErrorDetector],
        catalog_filename=dataset,
        search_path=dataset,
    )

    assert count_errors(detected_errors, ERROR) == len(expected_errors)
    for snippet in expected_errors:
        assert has_error(detected_errors, ERROR, snippet)

@pytest.mark.parametrize('query,dataset', [
    ("SELECT * FROM orders WHERE a > b", None),
    ("SELECT * FROM orders WHERE amount <= 100", None),
    ("SELECT * FROM orders WHERE sal < 500 AND comm <= 1000", None),
    ("SELECT * FROM orders WHERE sal > 500 OR comm > 1000", None),
    ("SELECT * FROM orders WHERE sal BETWEEN 100 AND 500", None),
    ("SELECT * FROM orders WHERE sal IN (100, 200, 300)", None),
    ("SELECT * FROM orders WHERE NOT (sal = 500)", None),
    ("SELECT * FROM orders WHERE name LIKE 'A%'", None),
    ("SELECT * FROM customer WHERE street LIKE 'Main %'", 'miedema'),
    # subqueries
    ("SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)", None),
    ("SELECT * FROM employees WHERE department_id IN (SELECT department_id FROM departments WHERE location_id = 1700 OR location_id = 1800)", None),
    # CTEs
    ("WITH dept_cte AS (SELECT department_id, dep_name FROM departments WHERE department_id < 100) SELECT * FROM dept_cte WHERE dep_name LIKE 'A%';", None),
])
def test_correct(query, dataset):
    detected_errors = run_test(
        query,
        detectors=[SemanticErrorDetector],
        catalog_filename=dataset,
        search_path=dataset,
    )

    assert count_errors(detected_errors, ERROR) == 0
