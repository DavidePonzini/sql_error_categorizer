from functools import singledispatch

from sqlglot import exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify_columns import qualify_columns
from dateutil.parser import parse
from sqlglot.expressions import DataType
from sql_error_categorizer.catalog.catalog import Table
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
    if new_type in DataType.NUMERIC_TYPES and not to_number(original_type):
        old_messages.append(("Invalid cast to numeric type", expression.sql()))

    # handle cast to date types
    if new_type in DataType.TEMPORAL_TYPES and not to_date(original_type):
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

    if expression.type.this not in DataType.NUMERIC_TYPES:
        old_messages.append(("Invalid unary minus type", expression.sql()))
    
    return AtomicType(data_type=expression.type.this, nullable=inner_type.nullable, constant=inner_type.constant, messages=old_messages, value=inner_type.value)

@get_type.register
def _(expression: exp.Not) -> ResultType:
    old_messages = get_type(expression.this).messages

    if expression.type.this != DataType.Type.BOOLEAN:
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

# # region logical ops
# @get_type.register
# def _(expression: exp.Like, referenced_tables: list[Table]) -> Type:
#     left_type = get_type(expression.this, referenced_tables)
#     right_type = get_type(expression.expression, referenced_tables)

#     if (early_errors := check_errors([left_type, right_type])) is not None:
#         return early_errors

#     if TupleType in (left_type, right_type):
#         return ErrorType(f"Invalid use of LIKE on tuple expression: {expression.sql()}")

#     if AtomicType(EnumType.NULL) in (left_type, right_type):
#         return AtomicType(EnumType.NULL, constant=True)

#     if AtomicType(EnumType.STRING) == left_type == right_type:
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False) if right_type.constant else ErrorType(f"Invalid use of LIKE on non-constant pattern: {expression.sql()}")
        
#     return ErrorType(f"Invalid use of LIKE on non-string expressions: {expression.sql()}")

# @get_type.register
# def _(expression: exp.Is, referenced_tables: list[Table]) -> Type:  
#     left_type = get_type(expression.this, referenced_tables)
#     right_type = get_type(expression.expression, referenced_tables)

#     if (early_errors := check_errors([left_type, right_type])) is not None:
#         return early_errors
    
#     # IS NULL
#     if right_type == AtomicType(EnumType.NULL):
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

#     # IS TRUE, IS FALSE, IS UNKNOWN
#     if AtomicType(EnumType.BOOLEAN) == right_type == left_type:
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False) if right_type.constant else ErrorType(f"Invalid use of IS operator on non-constant expression: {expression.sql()}")

#     return ErrorType(f"Invalid use of IS operator: {expression.sql()}")

# @get_type.register
# def _(expression: exp.Between, referenced_tables: list[Table]) -> Type:
#     target_type = get_type(expression.this, referenced_tables)
#     low_type = get_type(expression.args.get("low"), referenced_tables)
#     high_type = get_type(expression.args.get("high"), referenced_tables)

#     if (early_errors := check_errors([target_type, low_type, high_type])) is not None:
#         return early_errors

#     if AtomicType(EnumType.NULL) == target_type == low_type == high_type:
#         return AtomicType(EnumType.NULL, constant=True)

#     if target_type == low_type == high_type:
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)
    
#     # handle implicit casts
#     if to_number(target_type) and to_number(low_type) and to_number(high_type):
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

#     if to_date(target_type) and to_date(low_type) and to_date(high_type):
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

#     return ErrorType(f"Invalid use of BETWEEN on incompatible types: {expression.sql()}")


# @get_type.register
# def _(expression: exp.In, referenced_tables: list[Table]) -> Type:
#     target_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([target_type])) is not None:
#         return early_errors

#     if expression.expressions:
#         for item in expression.expressions:
#             item_type = get_type(item, referenced_tables)
#             if target_type != item_type:
#                 return ErrorType(f"Invalid use of IN on incompatible types: {expression.sql()}")

#     #TODO: handle subquery case
#     return NotImplementedType()

# AND, OR
@get_type.register
def _(expression: exp.Connector) -> ResultType:
    left_type = get_type(expression.this)
    right_type = get_type(expression.expression)

    old_messages = left_type.messages + right_type.messages

    if left_type.data_type != DataType.Type.BOOLEAN or right_type.data_type != DataType.Type.BOOLEAN:
        old_messages.append(("Invalid logical operator operand type", expression.sql()))


    return AtomicType(data_type=expression.this.type, nullable=False, constant=True, messages=old_messages)

# # endregion

# # region utils

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

def create_schema(referenced_tables: list[Table]) -> dict:
    return {
        table.name.lower(): {
            col.name.lower(): col.column_type
            for col in table.columns
        }
        for table in referenced_tables
    }

def rewrite_expression(expression: exp.Expression, referenced_tables: list[Table]) -> exp.Expression:
    '''
    Rewrites the expression by annotating types to its nodes based on the referenced tables.
    '''
    schema = create_schema(referenced_tables)
    return annotate_types(qualify_columns(expression, schema), schema)

def collect_errors(expression: exp.Expression, referenced_tables: list[Table]) -> list[(str, str)]:
    return get_type(rewrite_expression(expression, referenced_tables)).messages

# endregion