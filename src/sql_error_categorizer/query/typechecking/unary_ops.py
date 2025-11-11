from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType, DataType
from .util import is_number, error_message

@get_type.register
def _(expression: exp.Neg, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if not is_number(expression.type.this):
        old_messages.append((f"Invalid minus type. {error_message('NUMERIC', inner_type)}", expression.sql()))
    
    return AtomicType(data_type=expression.type.this, nullable=inner_type.nullable, constant=inner_type.constant, messages=old_messages, value=inner_type.value)

@get_type.register
def _(expression: exp.Not, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append((f"Invalid operand type. {error_message('BOOLEAN', inner_type)}", expression.sql()))

    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Paren, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)

@get_type.register
def _(expression: exp.Alias, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)

# To handle COUNT(DISTINCT ...) or similar constructs
@get_type.register
def _(expression: exp.Distinct, catalog: Catalog, search_path: str) -> ResultType:
    
    if len(expression.expressions) != 1:
        return AtomicType(messages=[("DISTINCT with multiple expressions is not valid.", expression.sql())])

    return get_type(expression.expressions[0], catalog, search_path)
