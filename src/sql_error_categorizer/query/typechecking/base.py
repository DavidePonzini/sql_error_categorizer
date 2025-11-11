import sqlglot.expressions as exp
from .types import AtomicType, ResultType
from ...catalog import Catalog
from functools import singledispatch

@singledispatch
def get_type(expression: exp.Expression, catalog: Catalog, search_path: str) -> ResultType:
    return AtomicType(messages=[("Unknown expression type.", expression.sql())])