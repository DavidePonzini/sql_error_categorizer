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
    return AtomicType()

@get_type.register
def _(expression: exp.Literal) -> ResultType:
    if expression.type.this == DataType.Type.UNKNOWN:
        return AtomicType(messages=[("Unknown literal type", expression.sql())])
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

    return TupleType(types=types, messages=old_messages, nullable=any(t.nullable for t in types), constant=all(t.constant for t in types))

@get_type.register
def _(expression: exp.Cast) -> ResultType:

    original_type = get_type(expression.this)

    new_type = expression.type.this
    
    old_messages = original_type.messages

    if new_type in (DataType.Type.UNKNOWN, DataType.Type.USERDEFINED):
        old_messages.append(("Invalid cast type", expression.sql()))
        return AtomicType(messages=old_messages)

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

# # region unary ops

# @get_type.register
# def _(expression: exp.Neg, referenced_tables: list[Table]) -> Type:
#     inner_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors
    
#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of unary minus on tuple expression: {expression.sql()}")

#     if to_number(inner_type):
#         return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=inner_type.constant)

#     return ErrorType(f"Invalid use of unary minus on non-numeric expression: {expression.sql()}")

# @get_type.register
# def _(expression: exp.Not, referenced_tables: list[Table]) -> Type:
#     inner_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors
    
#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of NOT on tuple expression: {expression.sql()}")
    
#     if inner_type == AtomicType(EnumType.BOOLEAN):
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

#     return ErrorType(f"Invalid use of NOT on non-boolean expression: {expression.sql()}")

# @get_type.register
# def _(expression: exp.Paren, referenced_tables: list[Table]) -> Type:
#     return get_type(expression.this, referenced_tables)

@get_type.register
def _(expression: exp.Alias) -> ResultType:
    return get_type(expression.this)

# # To handle COUNT(DISTINCT ...) or similar constructs
# @get_type.register
# def _(expression: exp.Distinct, referenced_tables: list[Table]) -> Type:
    
#     if len(expression.expressions) != 1:
#         return ErrorType(f"Invalid use of DISTINCT: {expression.sql()}")

#     inner_type = get_type(expression.expressions[0], referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors

#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of DISTINCT on tuple expression: {expression.sql()}")
    
#     return inner_type

# # endregion

# # region functions

# @get_type.register
# def _(expression: exp.Count, referenced_tables: list[Table]) -> Type:
#     inner_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors

#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of COUNT on tuple expression: {expression.sql()}")

#     return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=True)

# @get_type.register
# def _(expression: exp.Avg, referenced_tables: list[Table]) -> Type:
#     inner_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors

#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of AVG on tuple expression: {expression.sql()}")

#     if to_number(inner_type):
#         return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=True)

#     return ErrorType(f"Invalid use of AVG on non-numeric expression: {expression.sql()}")

# @get_type.register
# def _(expression: exp.Sum, referenced_tables: list[Table]) -> Type:
#     inner_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors

#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of SUM on tuple expression: {expression.sql()}")

#     if to_number(inner_type):
#         return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=True)

#     return ErrorType(f"Invalid use of SUM on non-numeric expression: {expression.sql()}")

# @get_type.register
# def _(expression: exp.Min, referenced_tables: list[Table]) -> Type:
#     inner_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors

#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of MIN on tuple expression: {expression.sql()}")

#     if inner_type == AtomicType(EnumType.BOOLEAN):
#         return ErrorType(f"Invalid use of MIN on boolean expression: {expression.sql()}")

#     return AtomicType(inner_type.enum_type, nullable=inner_type.nullable, constant=True)

# @get_type.register
# def _(expression: exp.Max, referenced_tables: list[Table]) -> Type:
#     inner_type = get_type(expression.this, referenced_tables)

#     if (early_errors := check_errors([inner_type])) is not None:
#         return early_errors

#     if inner_type == TupleType:
#         return ErrorType(f"Invalid use of MAX on tuple expression: {expression.sql()}")

#     if inner_type == AtomicType(EnumType.BOOLEAN):
#         return ErrorType(f"Invalid use of MAX on boolean expression: {expression.sql()}")

#     return AtomicType(inner_type.enum_type, nullable=inner_type.nullable, constant=True)

# @get_type.register
# def _(expression: exp.Concat, referenced_tables: list[Table]) -> Type:
#     args_type = [get_type(arg, referenced_tables) for arg in expression.expressions]

#     if not args_type:
#         return ErrorType(f"CONCAT requires at least one argument: {expression.sql()}")
        
#     if (early_errors := check_errors(args_type)) is not None:
#         return early_errors
    
#     # if all args are NULL, result is NULL
#     if all(target_type == AtomicType(EnumType.NULL) for target_type in args_type):
#         return AtomicType(EnumType.NULL, nullable=True, constant=True)

#     constant = all(target_type.constant for target_type in args_type)
#     nullable = any(target_type.nullable for target_type in args_type)

#     return AtomicType(EnumType.STRING, constant=constant, nullable=nullable)

# # endregion

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

# # AND, OR
# @get_type.register
# def _(expression: exp.Connector, referenced_tables: list[Table]) -> Type:
#     left_type = get_type(expression.this, referenced_tables)
#     right_type = get_type(expression.expression, referenced_tables)

#     if (early_errors := check_errors([left_type, right_type])) is not None:
#         return early_errors

#     if AtomicType(EnumType.BOOLEAN) == left_type == right_type:
#         return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)
    
#     return ErrorType(f"Invalid use of logical operator on non-boolean expressions: {expression.sql()}")

# # endregion

# # region utils

# def check_errors(types: list[Type]) -> ErrorType | NotImplementedType | None:

#     # collect error messages
#     messages = [target_type.message for target_type in types if target_type == ErrorType]

#     # check for NotImplementedType
#     if any(target_type == NotImplementedType for target_type in types) or messages:

#         # if we have already encountered errors, prioritize them
#         return ErrorType("\n".join(messages)) if messages else NotImplementedType()

#     return None

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

def determinate_type(expression: exp.Expression, referenced_tables: list[Table]) -> ResultType:
    schema = create_schema(referenced_tables)
    typed_expression = annotate_types(qualify_columns(expression, schema), schema)
    return get_type(typed_expression)

def collect_errors(expression: exp.Expression, referenced_tables: list[Table]) -> list[(str, str)]:
    return determinate_type(expression, referenced_tables).messages

# endregion