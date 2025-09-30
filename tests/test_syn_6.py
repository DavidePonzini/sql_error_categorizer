from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error
import pytest

@pytest.mark.parametrize("query,expected_result", [
    ('SELECT a.column1 FROM table1 a JOIN table2 b ON a.column2 = b.column2 WHERE c.column3 > 100;', ('c','column3','WHERE')),
    ('SELECT * FROM table1 t1 WHERE t1.column1 = 5 HAVING t2.column2 = 10;', ('t2','column2','HAVING')),
])
def test_syn_6_common_syntax_error_using_an_undefined_correlation_name(query, expected_result):

    detected_errors = run_test(
        query=query,
        detectors=[SyntaxErrorDetector]
    )

    assert has_error(detected_errors, SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_USING_AN_UNDEFINED_CORRELATION_NAME, expected_result)