from .base import get_type
from ...catalog import Catalog
from sqlglot import exp
from .types import ResultType, AtomicType, DataType, TupleType
from .util import is_number, is_date, to_number, to_date, error_message

@get_type.register
def _(expression: exp.Literal, catalog: Catalog, search_path: str) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, value=expression.this)

@get_type.register
def _(expression: exp.Boolean, catalog: Catalog, search_path: str) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True)

@get_type.register
def _(expression: exp.Null, catalog: Catalog, search_path: str) -> ResultType:
    return AtomicType(data_type=DataType.Type.NULL, constant=True)

@get_type.register
def _(expression: exp.Tuple, catalog: Catalog, search_path: str) -> ResultType:

    old_messages = []
    types = []
    for item in expression.expressions:
        item_type = get_type(item, catalog, search_path)
        if item_type.messages:
            old_messages.extend(item_type.messages)
        types.append(item_type)

    if not types:
        old_messages.append(error_message(expression, "Empty arguments"))

    return TupleType(types=types, messages=old_messages, nullable=any(t.nullable for t in types), constant=all(t.constant for t in types))

@get_type.register
def _(expression: exp.Cast, catalog: Catalog, search_path: str) -> ResultType:

    original_type = get_type(expression.this, catalog, search_path)

    new_type = expression.type.this
    
    old_messages = original_type.messages

    if new_type in (DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(error_message(expression, "Invalid type."))

    # handle cast to numeric types
    if is_number(new_type) and not to_number(original_type):
        old_messages.append(error_message(expression, "Invalid cast to numeric type."))
    # handle cast to date types
    if is_date(new_type) and not to_date(original_type):
        old_messages.append(error_message(expression, "Invalid cast to date type."))

    return AtomicType(data_type=new_type, nullable=original_type.nullable, constant=original_type.constant, messages=old_messages, value=original_type.value)

@get_type.register
def _(expression: exp.CurrentDate, catalog: Catalog, search_path: str) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True)

@get_type.register
def _(expression: exp.CurrentTimestamp, catalog: Catalog, search_path: str) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True)

@get_type.register
def _(expression: exp.Column, catalog: Catalog, search_path: str) -> ResultType:
    if expression.type.this in (DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        return AtomicType(messages=[error_message(expression.name, "Unknown column type")])
    else:
        schema = expression.args.get("db") or search_path
        table = expression.args.get("table")

        nullable = catalog[schema][table][expression.name].is_nullable
        return AtomicType(data_type=expression.type.this, constant=False, nullable=nullable)
