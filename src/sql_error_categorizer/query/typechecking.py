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
def get_type(expression: exp.Expression) -> ResultType:
    return AtomicType(messages=[("Unknown expression type", expression.sql())])

@get_type.register
def _(expression: exp.Literal) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, value=expression.this)

@get_type.register
def _(expression: exp.Boolean) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True)

@get_type.register
def _(expression: exp.Null) -> ResultType:
    return AtomicType(data_type=DataType.Type.NULL, constant=True)

@get_type.register
def _(expression: exp.Tuple) -> ResultType:

    old_messages = []
    types = []
    for item in expression.expressions:
        item_type = get_type(item)
        if item_type.messages:
            old_messages.extend(item_type.messages)
        types.append(item_type)

    if not types:
        old_messages.append(("Empty tuple type", expression.sql()))
        return AtomicType(messages=old_messages)

    return TupleType(types=types, messages=old_messages, nullable=any(t.nullable for t in types), constant=all(t.constant for t in types))

@get_type.register
def _(expression: exp.Cast) -> ResultType:

    original_type = get_type(expression.this)

    new_type = expression.type.this
    
    old_messages = original_type.messages

    if new_type in (DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(("Invalid cast type", expression.sql()))

    # handle cast to numeric types
    if is_number(new_type) and not to_number(original_type):
        old_messages.append(("Invalid cast to numeric type", expression.sql()))

    # handle cast to date types
    if is_date(new_type) and not to_date(original_type):
        old_messages.append(("Invalid cast to date type", expression.sql()))

    return AtomicType(data_type=new_type, nullable=original_type.nullable, constant=original_type.constant, messages=old_messages, value=original_type.value)

@get_type.register
def _(expression: exp.CurrentDate) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True)

@get_type.register
def _(expression: exp.CurrentTimestamp) -> ResultType:
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True)

@get_type.register
def _(expression: exp.Column) -> ResultType:
    if expression.type.this in (DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        return AtomicType(messages=[("Unknown column type", expression.sql())])
    else:
        # TODO: find an elegant way to determinate if a column is nullable
        return AtomicType(data_type=expression.type.this, constant=False)

# endregion

# region unary ops

@get_type.register
def _(expression: exp.Neg) -> ResultType:
    inner_type = get_type(expression.this)

    old_messages = inner_type.messages

    if not is_number(expression.type.this):
        old_messages.append(("Invalid unary minus type", expression.sql()))
    
    return AtomicType(data_type=expression.type.this, nullable=inner_type.nullable, constant=inner_type.constant, messages=old_messages, value=inner_type.value)

@get_type.register
def _(expression: exp.Not) -> ResultType:
    inner_type = get_type(expression.this)

    old_messages = inner_type.messages

    if inner_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append(("Invalid NOT operand type", expression.sql()))

    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Paren) -> ResultType:
    return get_type(expression.this)

@get_type.register
def _(expression: exp.Alias) -> ResultType:
    return get_type(expression.this)

# To handle COUNT(DISTINCT ...) or similar constructs
@get_type.register
def _(expression: exp.Distinct) -> ResultType:
    
    if len(expression.expressions) != 1:
        return AtomicType(messages=[("DISTINCT with multiple expressions is not supported", expression.sql())])

    return get_type(expression.expressions[0])

# # endregion

# region functions

@get_type.register
def _(expression: exp.Count) -> ResultType:
    old_messages = get_type(expression.this).messages

    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Avg) -> ResultType:
    inner_type = get_type(expression.this)

    old_messages = inner_type.messages

    if not to_number(inner_type):
        old_messages.append(("Invalid AVG operand type", expression.sql()))

    return AtomicType(data_type=expression.type.this, nullable=True, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Sum) -> ResultType:
    inner_type = get_type(expression.this)

    old_messages = inner_type.messages

    if not to_number(inner_type):
        old_messages.append(("Invalid SUM operand type", expression.sql()))

    return AtomicType(data_type=expression.type.this, nullable=True, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Min) -> ResultType:
    inner_type = get_type(expression.this)

    old_messages = inner_type.messages

    if inner_type.data_type in (DataType.Type.BOOLEAN, DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(("Invalid MIN operand type", expression.sql()))

    return AtomicType(data_type=inner_type.data_type, nullable=inner_type.nullable, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Max) -> ResultType:
    inner_type = get_type(expression.this)

    old_messages = inner_type.messages

    if inner_type.data_type in (DataType.Type.BOOLEAN, DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(("Invalid MAX operand type", expression.sql()))

    return AtomicType(data_type=inner_type.data_type, nullable=inner_type.nullable, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Concat) -> ResultType:
    old_messages = []
    args_type = []

    for arg in expression.expressions:
        arg_type = get_type(arg)
        if arg_type.messages:
            old_messages.extend(arg_type.messages)
        args_type.append(arg_type)
        

    if not args_type:
        return AtomicType(messages=[("CONCAT with no arguments", expression.sql())])
    
    # if all args are NULL, result is NULL
    if all(target_type.data_type == DataType.Type.NULL for target_type in args_type):
        return AtomicType(data_type=DataType.Type.NULL, constant=True, messages=old_messages)

    constant = all(target_type.constant for target_type in args_type)
    nullable = any(target_type.nullable for target_type in args_type)

    # Always returns VARCHAR
    return AtomicType(data_type=expression.type.this, nullable=nullable, constant=constant, messages=old_messages)

# endregion

# # region binary op

# @get_type.register
# def _(expression: exp.Binary, referenced_tables: list[Table]) -> Type:
#     left_type = get_type(expression.this, referenced_tables)
#     right_type = get_type(expression.expression, referenced_tables)

#     if (early_errors := check_errors([left_type, right_type])) is not None:
#         return early_errors

#     if AtomicType(EnumType.NULL) in (left_type, right_type):
#         return AtomicType(EnumType.NULL, constant=True)
    
#     # handle comparison operators
#     if isinstance(expression, exp.Predicate):
#         return typecheck_comparisons(left_type, right_type, expression)

#     if TupleType in (left_type, right_type):
#         return ErrorType(f"Invalid use of binary operator on tuple expression: {expression.sql()}")
    
#     if to_number(left_type) and to_number(right_type):
#         return AtomicType(EnumType.NUMBER, constant=left_type.constant and right_type.constant, nullable=left_type.nullable or right_type.nullable)
        
#     return ErrorType(f"Invalid use of binary operator on non-numeric expressions: {expression.sql()}")

# # handle comparison typechecking (e.g =, <, >, etc.)
# def typecheck_comparisons(left_type: Type, right_type: Type, expression: exp.Binary) -> Type:

#     # for boolean comparisons we can have only equality/inequality
#     if AtomicType(EnumType.BOOLEAN) == left_type == right_type:
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False) if isinstance(expression, (exp.EQ, exp.NEQ)) else ErrorType(f"Invalid use of comparison operator on boolean expressions: {expression.sql()}")

#     if left_type == right_type:
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

#     # handle implicit casts
#     if to_number(left_type) and to_number(right_type):
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

#     if to_date(left_type) and to_date(right_type):
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

#     return ErrorType(f"Invalid use of comparison operator on incompatible types: {expression.sql()}")

# region logical ops
@get_type.register
def _(expression: exp.Like) -> ResultType:
    left_type = get_type(expression.this)
    right_type = get_type(expression.expression)

    old_messages = left_type.messages + right_type.messages
    
    if not is_string(left_type.data_type) and left_type.data_type != DataType.Type.NULL:
        old_messages.append(("Invalid left operand type on LIKE operation", expression.sql()))

    if not is_string(right_type.data_type) and right_type.data_type != DataType.Type.NULL:
        old_messages.append(("Invalid right operand type on LIKE operation", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Is) -> ResultType:  
    left_type = get_type(expression.this)
    right_type = get_type(expression.expression)

    old_messages = left_type.messages + right_type.messages

    # IS right operand must be BOOLEAN or NULL constant
    if right_type.data_type not in (DataType.Type.BOOLEAN, DataType.Type.NULL) or not right_type.constant:
        old_messages.append(("Invalid right operand type on IS operation", expression.sql()))

    # if right is BOOLEAN and left is not NULL, left must be BOOLEAN
    if right_type.data_type == DataType.Type.BOOLEAN and left_type.data_type != DataType.Type.NULL:
        if left_type.data_type != DataType.Type.BOOLEAN:
            old_messages.append(("Invalid left operand type on IS operation with BOOLEAN", expression.sql()))
    
    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

@get_type.register
def _(expression: exp.Between) -> ResultType:
    target_type = get_type(expression.this)
    low_type = get_type(expression.args.get("low"))
    high_type = get_type(expression.args.get("high"))

    old_messages = target_type.messages + low_type.messages + high_type.messages

    # if the target is NULL, the result will always be NULL (no matter the bounds)
    if target_type.data_type == DataType.Type.NULL:
        return AtomicType(data_type=expression.type, constant=True, messages=old_messages)

    if low_type.data_type != target_type.data_type and low_type.data_type != DataType.Type.NULL:

        # check for implicit casts
        if (to_number(target_type) and not to_number(low_type)) or (to_date(target_type) and not to_date(low_type)):
            old_messages.append(("Invalid low bound type on BETWEEN operation", expression.sql()))

    if high_type.data_type != target_type.data_type and high_type.data_type != DataType.Type.NULL:
        
        # check for implicit casts
        if (to_number(target_type) and not to_number(high_type)) or (to_date(target_type) and not to_date(high_type)):
            old_messages.append(("Invalid high bound type on BETWEEN operation", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)


@get_type.register
def _(expression: exp.In) -> ResultType:
    target_type = get_type(expression.this)

    old_messages = target_type.messages

    # Case IN (<list>)
    for item in expression.expressions:
        item_type = get_type(item)
        if target_type != item_type:
            old_messages.append(("Invalid IN list item type", expression.sql()))

    # Case IN (subquery)
    if expression.args.get("query"):
        subquery_type = get_type(expression.args.get("query"))
        if target_type != subquery_type:
            old_messages.append(("The argument type of the IN subquery must match the target type", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

# AND, OR
@get_type.register
def _(expression: exp.Connector) -> ResultType:
    left_type = get_type(expression.this)
    right_type = get_type(expression.expression)

    old_messages = left_type.messages + right_type.messages

    if left_type.data_type != DataType.Type.BOOLEAN or right_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append(("Invalid logical operator operand type", expression.sql()))

    # Always returns boolean
    return AtomicType(data_type=expression.type.this, nullable=False, constant=True, messages=old_messages)

# ANY, ALL
@get_type.register
def _(expression: exp.SubqueryPredicate) -> ResultType:
    return get_type(expression.this)

# EXISTS
@get_type.register
def _(expression: exp.Exists) -> ResultType:
    old_messages = get_type(expression.this).messages
    return AtomicType(data_type=DataType.Type.BOOLEAN, nullable=False, constant=True, messages=old_messages)

# endregion

# region query

@get_type.register
def _(expression: exp.Select) -> ResultType:
    types = []
    old_messages = []

    for col in expression.expressions:
        col_type = get_type(col)
        if col_type.messages:
            old_messages.extend(col_type.messages)
        types.append(col_type)

    if not types:
        old_messages.append(("Empty SELECT expression", expression.sql()))

    where = expression.args.get("where")
    if where:
        old_messages.extend(collect_errors(where))

    having = expression.args.get("having")
    if having:
        old_messages.extend(collect_errors(having))

    if len(types) == 1:
        return AtomicType(data_type=types[0].data_type, messages=old_messages, nullable=types[0].nullable, constant=types[0].constant)

    return TupleType(types=types, messages=old_messages, nullable=any(t.nullable for t in types), constant=all(t.constant for t in types))

@get_type.register
def _(expression: exp.Subquery) -> ResultType:
    return get_type(expression.this)

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
    Rewrites the expression by annotating types to its nodes based on the referenced tables.
    '''

    schema = catalog.to_sqlglot_catalog()

    return annotate_types(qualify(expression, schema=schema, db=search_path), schema)

def collect_errors(expression: exp.Expression) -> list[(str, str)]:
    return get_type(expression).messages

# endregion