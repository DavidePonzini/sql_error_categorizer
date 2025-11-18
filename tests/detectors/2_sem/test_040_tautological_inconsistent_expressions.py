import pytest
from tests import *

def test_tautology():
    query = "SELECT * FROM orders WHERE a = a"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 1
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('tautology',))

def test_contradiction():
    query = "SELECT * FROM orders WHERE 1 = 0"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 2
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('contradiction',))
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('redundant_disjunct', '1 = 0'))

def test_contingent_expression():
    query = "SELECT * FROM orders WHERE amount > 100"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 0

def test_redundant_conjunction():
    query = "SELECT * FROM orders WHERE (sal < 500 AND comm > 1000) OR sal >= 500"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 1
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('redundant_conjunct', ('sal < 500 AND comm > 1000', 'sal < 500')))

def test_redundant_disjunction():
    query = "SELECT * FROM orders WHERE sal > 500 OR sal > 1000"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 1
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('redundant_disjunct', 'sal > 1000'))

def test_redundant_disjunction_strings():
    query = "SELECT * FROM store WHERE sname <= 'Coop' OR sname IN ('Coop', 'Lidl') or sname >= 'Lidl';"

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
        catalog_filename='miedema',
        search_path='miedema',
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 1
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('redundant_disjunct', "sname IN ('Coop', 'Lidl')"))


def test_redundant_disjunction_on_subquery():
    query = '''
    SELECT * FROM employees WHERE department_id IN (
        SELECT department_id FROM departments WHERE location_id = 1700 OR location_id >= 1700
    )
    '''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 1
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('redundant_disjunct', 'location_id = 1700'))


def test_no_errors_on_contingent_subquery():
    query = '''
    SELECT * FROM employees WHERE department_id IN (
        SELECT department_id FROM departments WHERE location_id = 1700 OR location_id = 1800
    )
    '''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 0

@pytest.mark.skip(reason="Subquery handling not yet implemented")
def test_tautology_with_subquery():
    query = '''
    SELECT * FROM employees WHERE department_id >= (
        SELECT MIN(department_id)
        FROM departments
    )
    '''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 1
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('tautology',))

@pytest.mark.skip(reason="CTE handling not yet implemented")
def test_contradiction_with_cte():
    query = '''
    WITH dept_cte AS (
        SELECT department_id FROM departments WHERE department_id < 100
    )
    SELECT * FROM dept_cte WHERE department_id > 100
    '''

    result = run_test(
        query,
        detectors=[SemanticErrorDetector],
    )

    assert count_errors(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION) == 1
    assert has_error(result, SqlErrors.SEM_40_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('contradiction',))