import sqlglot.expressions as exp
from .types import AtomicType, ResultType
from ...catalog import Catalog
from functools import singledispatch
from .util import error_message

@singledispatch
def get_type(expression: exp.Expression, catalog: Catalog, search_path: str) -> ResultType:
    '''Returns the type of the given SQL expression.'''
    return AtomicType(messages=[error_message(expression, "Unknown expression type")])