import pytest
from tests import *

nonstandard_ops = {
    '=='    : '=',
    '==='   : '=',
    '!=='   : '<>',
    '&&'    : ' AND ',
    '||'    : ' OR ',
    '!'     : ' NOT ',
    '>>'    : '>',
    '<<'    : '<',
    '≠'     : '<>',
    '≥'     : '>=',
    '≤'     : '<=',
}

    
@pytest.mark.parametrize('op,expected', nonstandard_ops.items())
def test_nonstandard_operator(op: str, expected: str):
    query = f'SELECT * FROM users WHERE age {op} 30;'

    detected_errors = run_test(
        query,
        detectors=[SyntaxErrorDetector]
    )

    assert count_errors(detected_errors, SqlErrors.SYN_37_NONSTANDARD_OPERATORS) == 1
    assert has_error(detected_errors, SqlErrors.SYN_37_NONSTANDARD_OPERATORS, (op, expected))
