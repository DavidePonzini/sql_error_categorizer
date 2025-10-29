from dataclasses import dataclass
from functools import singledispatch
from enum import Enum
from sqlglot import exp
from sql_error_categorizer.catalog.catalog import Table
from dateutil.parser import parse

# region types definitions
class Type:
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower().split('type')[0]

class EnumType(Enum):
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    NULL = "null"

@dataclass
class AtomicType(Type):
    enum_type: EnumType
    nullable: bool = True
    constant: bool = False
    value: str | None = None

    @property
    def name(self) -> str:
        return self.enum_type.value

    def __eq__(self, other):
        if not isinstance(other, AtomicType):
            return False
        return self.enum_type == other.enum_type
    
    def __repr__(self):
        return f"AtomicType({self.enum_type})"

class TupleType(Type):

    @property
    def name(self) -> str:
        return f"tuple({', '.join(t.name for t in self.types)})"

    def __init__(self, types: list[Type]):
        self.types = types

    def __eq__(self, other):
        if not isinstance(other, TupleType):
            return False
        return self.types == other.types
    
    def __repr__(self):
        inner = ", ".join(repr(t) for t in self.types)
        return f"TupleType([{inner}])"

@dataclass    
class ErrorType(Type):
    message: str

    def __repr__(self):
        return f"ErrorType(\"{self.message.split(':')[0]}\")"
    
    def __eq__(self, other):
        return other is ErrorType
    
class NotImplementedType(Type):
    def __repr__(self):
        return "NotImplementedType()"
    
    def __eq__(self, other):
        return other is NotImplementedType
    
# endregion

# region primitive types
@singledispatch
def get_type(expression: exp.Expression, referenced_tables: list[Table]) -> Type:
    return NotImplementedType()

@get_type.register
def _(expression: exp.Literal, referenced_tables: list[Table]) -> Type:
    if expression.is_string:
        return AtomicType(EnumType.STRING, nullable=False, constant=True, value=expression.this)
    elif expression.is_number:
        return AtomicType(EnumType.NUMBER, nullable=False, constant=True, value=expression.this)
    return ErrorType(f"Unknown literal type for value: {expression.this}")

@get_type.register
def _(expression: exp.Boolean, referenced_tables: list[Table]) -> Type:
    return AtomicType(EnumType.BOOLEAN, nullable=False, constant=True)

@get_type.register
def _(expression: exp.Null, referenced_tables: list[Table]) -> Type:
    return AtomicType(EnumType.NULL, constant=True)

@get_type.register
def _(expression: exp.Tuple, referenced_tables: list[Table]) -> Type:

    types_list = [get_type(item, referenced_tables) for item in expression.expressions]
        
    if (early_errors := check_errors(types_list)) is not None:
        return early_errors

    return ErrorType(f"Empty tuple has no type: {expression.sql()}") if not types_list else TupleType(types_list)

# We use cast only for date conversions (it is reasonably valid if we encounter "DATE '2020-01-01'" or "TIMESTAMP '2020-01-01 10:00:00'")
@get_type.register
def _(expression: exp.Cast, referenced_tables: list[Table]) -> Type:

    original_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([original_type])) is not None:
        return early_errors
    
    # the casting is valid if the original type is a date or a string that can be converted to date
    if to_date(original_type):

        target_type = expression.args.get("to")
        if not target_type:
            return ErrorType(f"CAST missing target type: {expression.sql()}")

        if target_type.sql().upper() in ["DATE", "TIMESTAMP"]:
            return AtomicType(EnumType.DATE, nullable=False, constant=True, value=original_type.value)

    # unsupported cast
    return NotImplementedType()

@get_type.register
def _(expression: exp.CurrentDate, referenced_tables: list[Table]) -> Type:
    return AtomicType(EnumType.DATE, nullable=False, constant=True)

@get_type.register
def _(expression: exp.CurrentTimestamp, referenced_tables: list[Table]) -> Type:
    return AtomicType(EnumType.DATE, nullable=False, constant=True)

@get_type.register
def _(expression: exp.Column, referenced_tables: list[Table]) -> Type:
    col_name = expression.name
    table_name = expression.table

    result = []
    for table in referenced_tables:
        if table_name == '' or table.name == table_name:
            for col in table.columns:
                if col.name == col_name:

                    col_type = to_res_type(col.column_type)

                    # text, blob, etc. are not implemented
                    if col_type == NotImplementedType: 
                        return col_type

                    result.append(AtomicType(col_type, nullable=col.is_nullable))

    if not result:
        return ErrorType(f"Column not found: {col_name}")
    
    if len(result) == 1:
        return result.pop()
    
    return ErrorType(f"Ambiguous column reference: {col_name}")

# endregion

# region unary ops

@get_type.register
def _(expression: exp.Neg, referenced_tables: list[Table]) -> Type:
    inner_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors
    
    if inner_type == TupleType:
        return ErrorType(f"Invalid use of unary minus on tuple expression: {expression.sql()}")

    if to_number(inner_type):
        return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=inner_type.constant)

    return ErrorType(f"Invalid use of unary minus on non-numeric expression: {expression.sql()}")

@get_type.register
def _(expression: exp.Not, referenced_tables: list[Table]) -> Type:
    inner_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors
    
    if inner_type == TupleType:
        return ErrorType(f"Invalid use of NOT on tuple expression: {expression.sql()}")
    
    if inner_type == AtomicType(EnumType.BOOLEAN):
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    return ErrorType(f"Invalid use of NOT on non-boolean expression: {expression.sql()}")

@get_type.register
def _(expression: exp.Paren, referenced_tables: list[Table]) -> Type:
    return get_type(expression.this, referenced_tables)

@get_type.register
def _(expression: exp.Alias, referenced_tables: list[Table]) -> Type:
    return get_type(expression.this, referenced_tables)

# To handle COUNT(DISTINCT ...) or similar constructs
@get_type.register
def _(expression: exp.Distinct, referenced_tables: list[Table]) -> Type:
    
    if len(expression.expressions) != 1:
        return ErrorType(f"Invalid use of DISTINCT: {expression.sql()}")

    inner_type = get_type(expression.expressions[0], referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors

    if inner_type == TupleType:
        return ErrorType(f"Invalid use of DISTINCT on tuple expression: {expression.sql()}")
    
    return inner_type

# endregion

# region functions

@get_type.register
def _(expression: exp.Count, referenced_tables: list[Table]) -> Type:
    inner_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors

    if inner_type == TupleType:
        return ErrorType(f"Invalid use of COUNT on tuple expression: {expression.sql()}")

    return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=True)

@get_type.register
def _(expression: exp.Avg, referenced_tables: list[Table]) -> Type:
    inner_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors

    if inner_type == TupleType:
        return ErrorType(f"Invalid use of AVG on tuple expression: {expression.sql()}")

    if to_number(inner_type):
        return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=True)

    return ErrorType(f"Invalid use of AVG on non-numeric expression: {expression.sql()}")

@get_type.register
def _(expression: exp.Sum, referenced_tables: list[Table]) -> Type:
    inner_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors

    if inner_type == TupleType:
        return ErrorType(f"Invalid use of SUM on tuple expression: {expression.sql()}")

    if to_number(inner_type):
        return AtomicType(EnumType.NUMBER, nullable=inner_type.nullable, constant=True)

    return ErrorType(f"Invalid use of SUM on non-numeric expression: {expression.sql()}")

@get_type.register
def _(expression: exp.Min, referenced_tables: list[Table]) -> Type:
    inner_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors

    if inner_type == TupleType:
        return ErrorType(f"Invalid use of MIN on tuple expression: {expression.sql()}")

    if inner_type == AtomicType(EnumType.BOOLEAN):
        return ErrorType(f"Invalid use of MIN on boolean expression: {expression.sql()}")

    return AtomicType(inner_type.enum_type, nullable=inner_type.nullable, constant=True)

@get_type.register
def _(expression: exp.Max, referenced_tables: list[Table]) -> Type:
    inner_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([inner_type])) is not None:
        return early_errors

    if inner_type == TupleType:
        return ErrorType(f"Invalid use of MAX on tuple expression: {expression.sql()}")

    if inner_type == AtomicType(EnumType.BOOLEAN):
        return ErrorType(f"Invalid use of MAX on boolean expression: {expression.sql()}")

    return AtomicType(inner_type.enum_type, nullable=inner_type.nullable, constant=True)

@get_type.register
def _(expression: exp.Concat, referenced_tables: list[Table]) -> Type:
    args_type = [get_type(arg, referenced_tables) for arg in expression.expressions]

    if not args_type:
        return ErrorType(f"CONCAT requires at least one argument: {expression.sql()}")
        
    if (early_errors := check_errors(args_type)) is not None:
        return early_errors
    
    # if all args are NULL, result is NULL
    if all(target_type == AtomicType(EnumType.NULL) for target_type in args_type):
        return AtomicType(EnumType.NULL, nullable=True, constant=True)

    constant = all(target_type.constant for target_type in args_type)
    nullable = any(target_type.nullable for target_type in args_type)

    return AtomicType(EnumType.STRING, constant=constant, nullable=nullable)

# endregion

# region binary op

@get_type.register
def _(expression: exp.Binary, referenced_tables: list[Table]) -> Type:
    left_type = get_type(expression.this, referenced_tables)
    right_type = get_type(expression.expression, referenced_tables)

    if (early_errors := check_errors([left_type, right_type])) is not None:
        return early_errors

    if AtomicType(EnumType.NULL) in (left_type, right_type):
        return AtomicType(EnumType.NULL, constant=True)
    
    # handle comparison operators
    if isinstance(expression, exp.Predicate):
        return typecheck_comparisons(left_type, right_type, expression)

    if TupleType in (left_type, right_type):
        return ErrorType(f"Invalid use of binary operator on tuple expression: {expression.sql()}")
    
    if to_number(left_type) and to_number(right_type):
        return AtomicType(EnumType.NUMBER, constant=left_type.constant and right_type.constant, nullable=left_type.nullable or right_type.nullable)
        
    return ErrorType(f"Invalid use of binary operator on non-numeric expressions: {expression.sql()}")

# handle comparison typechecking (e.g =, <, >, etc.)
def typecheck_comparisons(left_type: Type, right_type: Type, expression: exp.Binary) -> Type:

    # for boolean comparisons we can have only equality/inequality
    if AtomicType(EnumType.BOOLEAN) == left_type == right_type:
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False) if isinstance(expression, (exp.EQ, exp.NEQ)) else ErrorType(f"Invalid use of comparison operator on boolean expressions: {expression.sql()}")

    if left_type == right_type:
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    # handle implicit casts
    if to_number(left_type) and to_number(right_type):
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    if to_date(left_type) and to_date(right_type):
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    return ErrorType(f"Invalid use of comparison operator on incompatible types: {expression.sql()}")

# region logical ops
@get_type.register
def _(expression: exp.Like, referenced_tables: list[Table]) -> Type:
    left_type = get_type(expression.this, referenced_tables)
    right_type = get_type(expression.expression, referenced_tables)

    if (early_errors := check_errors([left_type, right_type])) is not None:
        return early_errors

    if TupleType in (left_type, right_type):
        return ErrorType(f"Invalid use of LIKE on tuple expression: {expression.sql()}")

    if AtomicType(EnumType.NULL) in (left_type, right_type):
        return AtomicType(EnumType.NULL, constant=True)

    if AtomicType(EnumType.STRING) == left_type == right_type:
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False) if right_type.constant else ErrorType(f"Invalid use of LIKE on non-constant pattern: {expression.sql()}")
        
    return ErrorType(f"Invalid use of LIKE on non-string expressions: {expression.sql()}")

@get_type.register
def _(expression: exp.Is, referenced_tables: list[Table]) -> Type:  
    left_type = get_type(expression.this, referenced_tables)
    right_type = get_type(expression.expression, referenced_tables)

    if (early_errors := check_errors([left_type, right_type])) is not None:
        return early_errors
    
    # IS NULL
    if right_type == AtomicType(EnumType.NULL):
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    # IS TRUE, IS FALSE, IS UNKNOWN
    if AtomicType(EnumType.BOOLEAN) == right_type == left_type:
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False) if right_type.constant else ErrorType(f"Invalid use of IS operator on non-constant expression: {expression.sql()}")

    return ErrorType(f"Invalid use of IS operator: {expression.sql()}")

@get_type.register
def _(expression: exp.Between, referenced_tables: list[Table]) -> Type:
    target_type = get_type(expression.this, referenced_tables)
    low_type = get_type(expression.args.get("low"), referenced_tables)
    high_type = get_type(expression.args.get("high"), referenced_tables)

    if (early_errors := check_errors([target_type, low_type, high_type])) is not None:
        return early_errors

    if AtomicType(EnumType.NULL) == target_type == low_type == high_type:
        return AtomicType(EnumType.NULL, constant=True)

    if target_type == low_type == high_type:
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)
    
    # handle implicit casts
    if to_number(target_type) and to_number(low_type) and to_number(high_type):
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    if to_date(target_type) and to_date(low_type) and to_date(high_type):
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    return ErrorType(f"Invalid use of BETWEEN on incompatible types: {expression.sql()}")


@get_type.register
def _(expression: exp.In, referenced_tables: list[Table]) -> Type:
    target_type = get_type(expression.this, referenced_tables)

    if (early_errors := check_errors([target_type])) is not None:
        return early_errors

    if expression.expressions:
        for item in expression.expressions:
            item_type = get_type(item, referenced_tables)
            if target_type != item_type:
                return ErrorType(f"Invalid use of IN on incompatible types: {expression.sql()}")

    #TODO: handle subquery case
    return NotImplementedType()

# AND, OR
@get_type.register
def _(expression: exp.Connector, referenced_tables: list[Table]) -> Type:
    left_type = get_type(expression.this, referenced_tables)
    right_type = get_type(expression.expression, referenced_tables)

    if (early_errors := check_errors([left_type, right_type])) is not None:
        return early_errors

    if AtomicType(EnumType.BOOLEAN) == left_type == right_type:
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)
    
    return ErrorType(f"Invalid use of logical operator on non-boolean expressions: {expression.sql()}")

# endregion

# region utils

def check_errors(types: list[Type]) -> ErrorType | NotImplementedType | None:

    # collect error messages
    messages = [target_type.message for target_type in types if target_type == ErrorType]

    # check for NotImplementedType
    if any(target_type == NotImplementedType for target_type in types) or messages:

        # if we have already encountered errors, prioritize them
        return ErrorType("\n".join(messages)) if messages else NotImplementedType()

    return None

def to_date(target: Type) -> bool:
    if target == AtomicType(EnumType.DATE):
        return True
    if target == AtomicType(EnumType.STRING) and target.value is not None:
        try:
            parse(target.value)
            return True
        except ValueError:
            return False
    return False
    
def to_number(target: Type) -> bool:
    if target == AtomicType(EnumType.NUMBER):
        return True
    if target == AtomicType(EnumType.STRING) and target.value is not None:
        try:
            float(target.value)
            return True
        except ValueError:
            return False
    return False

def to_res_type(original_type: str) -> EnumType | NotImplementedType:
    original_type = original_type.upper()
    if original_type in ["VARCHAR", "TEXT", "STRING"] or original_type.startswith("CHAR"):
        return EnumType.STRING
    elif original_type in ["DECIMAL", "REAL"] or original_type.startswith("INT") or original_type.startswith("FLOAT") or original_type.startswith("DOUBLE") or original_type.endswith("INT") or original_type.startswith("NUM"):
        return EnumType.NUMBER
    elif original_type.startswith("BOOL"):
        return EnumType.BOOLEAN
    elif original_type.startswith("DATE") or original_type.startswith("TIMESTAMP"):
        return EnumType.DATE
    else:
        return NotImplementedType()

# endregion