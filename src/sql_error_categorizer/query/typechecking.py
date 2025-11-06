from functools import singledispatch
from dateutil.parser import parse

from sqlglot import exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.expressions import DataType

from sql_error_categorizer.catalog.catalog import Catalog
from sql_error_categorizer.catalog.types import ResultType, AtomicType, TupleType

# region primitive types
@singledispatch
def get_type(expression: exp.Expression, catalog: Catalog, search_path: str) -> ResultType:
    return AtomicType(messages=[("Unknown expression type.", expression.sql())])

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
        old_messages.append(("Empty tuple type.", expression.sql()))

    return TupleType(types=types, messages=old_messages, nullable=any(t.nullable for t in types), constant=all(t.constant for t in types))

@get_type.register
def _(expression: exp.Cast, catalog: Catalog, search_path: str) -> ResultType:

    original_type = get_type(expression.this, catalog, search_path)

    new_type = expression.type.this
    
    old_messages = original_type.messages

    if new_type in (DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(("Invalid cast type.", expression.sql()))

    # handle cast to numeric types
    if is_number(new_type) and not to_number(original_type):
        old_messages.append(("Invalid cast to numeric type.", expression.sql()))

    # handle cast to date types
    if is_date(new_type) and not to_date(original_type):
        old_messages.append(("Invalid cast to date type.", expression.sql()))

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
        return AtomicType(messages=[("Unknown column type.", expression.name)])
    else:
        schema = expression.args.get("db") or search_path
        table = expression.args.get("table")
        nullable = catalog.__getitem__(schema_name=schema).__getitem__(table_name=table).__getitem__(column_name=expression.name).is_nullable
        return AtomicType(data_type=expression.type.this, constant=False, nullable=nullable)

# endregion

# region unary ops

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

# # endregion

# region functions

@get_type.register
def _(expression: exp.Count, catalog: Catalog, search_path: str) -> ResultType:
    old_messages = get_type(expression.this, catalog, search_path).messages

    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Avg, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if not is_number(inner_type.data_type):
        old_messages.append((f"Invalid AVG operand type. {error_message('NUMERIC', inner_type)}", expression.sql()))

    return AtomicType(data_type=expression.type.this, nullable=True, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Sum, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if not is_number(inner_type.data_type):
        old_messages.append((f"Invalid SUM operand type. {error_message('NUMERIC', inner_type)}", expression.sql()))

    return AtomicType(data_type=expression.type.this, nullable=True, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Min, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type in (DataType.Type.BOOLEAN, DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(("Invalid MIN operand type.", expression.sql()))

    return AtomicType(data_type=inner_type.data_type, nullable=inner_type.nullable, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Max, catalog: Catalog, search_path: str) -> ResultType:
    inner_type = get_type(expression.this, catalog, search_path)

    old_messages = inner_type.messages

    if inner_type.data_type in (DataType.Type.BOOLEAN, DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(("Invalid MAX operand type.", expression.sql()))

    return AtomicType(data_type=inner_type.data_type, nullable=inner_type.nullable, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Concat, catalog: Catalog, search_path: str) -> ResultType:
    old_messages = []
    args_type = []

    for arg in expression.expressions:
        arg_type = get_type(arg, catalog, search_path)
        if arg_type.messages:
            old_messages.extend(arg_type.messages)
        args_type.append(arg_type)
        

    if not args_type:
        old_messages.append(("Empty CONCAT arguments.", expression.sql()))
    
    # if all args are NULL, result is NULL
    if all(target_type.data_type == DataType.Type.NULL for target_type in args_type):
        return AtomicType(data_type=DataType.Type.NULL, constant=True, messages=old_messages)

    constant = all(target_type.constant for target_type in args_type)
    nullable = any(target_type.nullable for target_type in args_type)

    # Always returns VARCHAR
    return AtomicType(data_type=expression.type.this, nullable=nullable, constant=constant, messages=old_messages)

# endregion

# region binary op

@get_type.register
def _(expression: exp.Binary, catalog: Catalog, search_path: str) -> ResultType:
    left_type = get_type(expression.this, catalog, search_path)
    right_type = get_type(expression.expression, catalog, search_path)

    old_messages = left_type.messages + right_type.messages
    
    # handle comparison operators
    if isinstance(expression, exp.Predicate):
        return typecheck_comparisons(left_type, right_type, expression, old_messages)
    

    if left_type != right_type:

        if not to_number(left_type) and left_type.data_type != DataType.Type.NULL:
            old_messages.append((f"Invalid left operand type. {error_message('NUMERIC', left_type)}", expression.sql()))

        if not to_number(right_type) and right_type.data_type != DataType.Type.NULL:
            old_messages.append((f"Invalid right operand type. {error_message('NUMERIC', right_type)}", expression.sql()))

    elif not is_number(left_type.data_type) and not is_number(right_type.data_type):
        if left_type.data_type != DataType.Type.NULL or right_type.data_type != DataType.Type.NULL:
            old_messages.append(("Non-numeric operands type on arithmetic operation.", expression.sql()))

    return AtomicType(data_type=expression.type.this, nullable=left_type.nullable or right_type.nullable, constant=left_type.constant and right_type.constant, messages=old_messages)

# handle comparison typechecking (e.g =, <, >, etc.)
def typecheck_comparisons(left_type: ResultType, right_type: ResultType, expression: exp.Binary, old_messages: list) -> ResultType:

    # for boolean comparisons we can have only equality/inequality
    if DataType.Type.BOOLEAN == left_type.data_type == right_type.data_type:
        if not isinstance(expression, (exp.EQ, exp.NEQ)):
            old_messages.append(("Invalid comparison operator. Must use '=' or '<>'.", expression.sql()))

    if left_type != right_type and left_type.data_type != DataType.Type.NULL and right_type.data_type != DataType.Type.NULL:
        
        # handle implicit casts
        if to_number(left_type) and to_number(right_type):
            return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

        if to_date(left_type) and to_date(right_type):
            return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

        old_messages.append(("Type mismatch in comparison.", expression.sql()))
        

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

# region logical ops
@get_type.register
def _(expression: exp.Like, catalog: Catalog, search_path: str) -> ResultType:
    left_type = get_type(expression.this, catalog, search_path)
    right_type = get_type(expression.expression, catalog, search_path)

    old_messages = left_type.messages + right_type.messages
    
    if not is_string(left_type.data_type) and left_type.data_type != DataType.Type.NULL:
        old_messages.append((f"Invalid left operand type. {error_message('STRING', left_type)}", expression.sql()))

    if not is_string(right_type.data_type) and right_type.data_type != DataType.Type.NULL:
        old_messages.append((f"Invalid right operand type. {error_message('STRING', right_type)}", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Is, catalog: Catalog, search_path: str) -> ResultType:  
    left_type = get_type(expression.this, catalog, search_path)
    right_type = get_type(expression.expression, catalog, search_path)

    old_messages = left_type.messages + right_type.messages

    # IS right operand must be BOOLEAN or NULL constant
    if right_type.data_type not in (DataType.Type.BOOLEAN, DataType.Type.NULL) or not right_type.constant:
        old_messages.append((f"Invalid right operand type. {error_message('BOOLEAN or NULL', right_type)}", expression.sql()))

    # if right is BOOLEAN and left is not NULL, left must be BOOLEAN
    if right_type.data_type == DataType.Type.BOOLEAN and left_type.data_type != DataType.Type.NULL:
        if left_type.data_type != DataType.Type.BOOLEAN:
            old_messages.append((f"Invalid left operand type. {error_message('BOOLEAN', left_type)}", expression.sql()))
    
    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Between, catalog: Catalog, search_path: str) -> ResultType:
    target_type = get_type(expression.this, catalog, search_path)
    low_type = get_type(expression.args.get("low"), catalog, search_path)
    high_type = get_type(expression.args.get("high"), catalog, search_path)

    old_messages = target_type.messages + low_type.messages + high_type.messages

    # if the target is NULL, the result will always be NULL (no matter the bounds)
    if target_type.data_type == DataType.Type.NULL:
        return AtomicType(data_type=expression.type.this, constant=True, messages=old_messages)

    if low_type.data_type != target_type.data_type and low_type.data_type != DataType.Type.NULL:

        # check for implicit casts
        if (to_number(target_type) and not to_number(low_type)) or (to_date(target_type) and not to_date(low_type)):
            old_messages.append((f"Invalid low bound type. {error_message(target_type, low_type)}", expression.sql()))

    if high_type.data_type != target_type.data_type and high_type.data_type != DataType.Type.NULL:
        
        # check for implicit casts
        if (to_number(target_type) and not to_number(high_type)) or (to_date(target_type) and not to_date(high_type)):
            old_messages.append((f"Invalid high bound type. {error_message(target_type, high_type)}", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)


@get_type.register
def _(expression: exp.In, catalog: Catalog, search_path: str) -> ResultType:
    target_type = get_type(expression.this, catalog, search_path)

    old_messages = target_type.messages

    # Case IN (<list>)
    for item in expression.expressions:
        item_type = get_type(item, catalog, search_path)
        if target_type != item_type:
            old_messages.append((f"Invalid item type. {error_message(target_type, item_type)}", expression.sql()))

    # Case IN (subquery)
    if expression.args.get("query"):
        subquery_type = get_type(expression.args.get("query"), catalog, search_path)
        if target_type != subquery_type:
            old_messages.append((f"Invalid right operand type. {error_message(target_type, subquery_type)}", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

# AND, OR
@get_type.register
def _(expression: exp.Connector, catalog: Catalog, search_path: str) -> ResultType:
    left_type = get_type(expression.this, catalog, search_path)
    right_type = get_type(expression.expression, catalog, search_path)

    old_messages = left_type.messages + right_type.messages

    if left_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append((f"Invalid left operand type. {error_message(DataType.Type.BOOLEAN, left_type)}", expression.sql()))

    if right_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append((f"Invalid right operand type. {error_message(DataType.Type.BOOLEAN, right_type)}", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

# ANY, ALL
@get_type.register
def _(expression: exp.SubqueryPredicate, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)

# EXISTS
@get_type.register
def _(expression: exp.Exists, catalog: Catalog, search_path: str) -> ResultType:
    old_messages = get_type(expression.this, catalog, search_path).messages
    return AtomicType(data_type=DataType.Type.BOOLEAN, nullable=False, constant=True, messages=old_messages)

# endregion

# region query

@get_type.register
def _(expression: exp.Select, catalog: Catalog, search_path: str) -> ResultType:
    types = []
    old_messages = []

    for col in expression.expressions:
        col_type = get_type(col, catalog, search_path)
        if col_type.messages:
            old_messages.extend(col_type.messages)
        types.append(col_type)

    if not types:
        old_messages.append(("Empty SELECT expression.", expression.sql()))

    where = expression.args.get("where")
    if where:
        old_messages.extend(collect_errors(where.this, catalog, search_path))

    having = expression.args.get("having")
    if having:
        old_messages.extend(collect_errors(having, catalog, search_path))

    if len(types) == 1:
        return AtomicType(data_type=types[0].data_type, messages=old_messages, nullable=types[0].nullable, constant=types[0].constant)

    return TupleType(types=types, messages=old_messages, nullable=any(t.nullable for t in types), constant=all(t.constant for t in types))

@get_type.register
def _(expression: exp.Subquery, catalog: Catalog, search_path: str) -> ResultType:
    return get_type(expression.this, catalog, search_path)

# endregion

# region utils

def to_date(target: ResultType) -> bool:
    if target.data_type in DataType.TEMPORAL_TYPES:
        return True
    if target.data_type in DataType.TEXT_TYPES and target.value is not None:
        try:
            parse(target.value)
            return True
        except ValueError:
            return False
    return False
    
def to_number(target: ResultType) -> bool:
    if target.data_type in DataType.NUMERIC_TYPES:
        return True
    if target.data_type in DataType.TEXT_TYPES and target.value is not None:
        try:
            float(target.value)
            return True
        except ValueError:
            return False
    return False

def is_number(target: DataType.Type):
    return target in DataType.NUMERIC_TYPES

def is_string(target: DataType.Type):
    return target in DataType.TEXT_TYPES

def is_date(target: DataType.Type):
    return target in DataType.TEMPORAL_TYPES

def rewrite_expression(expression: exp.Expression, catalog: Catalog, search_path: str = 'public') -> exp.Expression:
    '''
    Rewrites the expression by annotating types to its nodes based on the catalog.
    '''

    schema = catalog.to_sqlglot_schema()

    return annotate_types(qualify(expression, schema=schema, db=search_path, validate_qualify_columns=False), schema)

# This function needs to be called on a typed expression
def collect_errors(expression: exp.Expression, catalog: Catalog, search_path: str = 'public') -> list[(str, str)]:
    return get_type(expression, catalog, search_path).messages

def error_message(expected: ResultType | str, found: ResultType | str) -> str:

    if isinstance(expected, ResultType):
        expected = expected.data_type.value

    if isinstance(found, ResultType):
        found = found.data_type.value

    return f"Expected type {expected.lower()}, but found type {found.lower()}."

# endregion