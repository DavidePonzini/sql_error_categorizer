from dataclasses import dataclass
from functools import singledispatch
from enum import Enum
from sqlglot import exp
from sql_error_categorizer.catalog.catalog import Table
from dateutil.parser import parse

class Type(Enum):
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    NONE = "none"
    NULL = "null"

@dataclass
class ResultType:
    type: Type
    value: any = None
    nullable: bool = True
    constant: bool = False

# region primitive types
@singledispatch
def get_type(expression: exp.Expression, referenced_tables: list[Table]) -> list[ResultType]:
    return [ResultType(Type.NONE)]

@get_type.register
def _(expression: exp.Literal, referenced_tables: list[Table]) -> list[ResultType]:
    if expression.is_string:
        return [ResultType(Type.STRING, constant=True, value=expression.this, nullable=False)]
    elif expression.is_number:
        return [ResultType(Type.NUMBER, constant=True, value=expression.this, nullable=False)]
    return [ResultType(Type.NONE)]

@get_type.register
def _(expression: exp.Boolean, referenced_tables: list[Table]) -> list[ResultType]:
    return [ResultType(Type.BOOLEAN, constant=True, value=expression.this, nullable=False)]

@get_type.register
def _(expression: exp.Null, referenced_tables: list[Table]) -> list[ResultType]:
    return [ResultType(Type.NULL, nullable=True, constant=True)]

# endregion

# region dates

# We use cast only for date conversions (it is reasonably valid only if we do "DATE '2020-01-01'" or "TIMESTAMP '2020-01-01 10:00:00'")
@get_type.register
def _(expression: exp.Cast, referenced_tables: list[Table]) -> list[ResultType]:
    original_types = get_type(expression.this, referenced_tables)
    # the casting is valid if the original type is a date or a string that can be converted to date
    if len(original_types) == 1 and (original_types[0].type == Type.DATE or to_date(original_types[0])):
        if expression.args.get("to").sql().upper() in ["DATE", "TIMESTAMP"]:
            return [ResultType(Type.DATE, constant=True, value=original_types[0].value, nullable=False)]
    return [ResultType(Type.NONE)]
    
@get_type.register
def _(expression: exp.CurrentDate, referenced_tables: list[Table]) -> list[ResultType]:
    return [ResultType(Type.DATE, constant=True, nullable=False)]

@get_type.register
def _(expression: exp.CurrentTimestamp, referenced_tables: list[Table]) -> list[ResultType]:
    return [ResultType(Type.DATE, constant=True, nullable=False)]

# endregion

# region columns
@get_type.register
def _(expression: exp.Column, referenced_tables: list[Table]) -> list[ResultType]:
    col_name = expression.name
    table_name = expression.table

    result = []
    for table in referenced_tables:
        if table_name == '' or table.name == table_name:
            for col in table.columns:
                if col.name == col_name:
                    result.append(ResultType(to_res_type(col.column_type), nullable=col.is_nullable))
    if len(result) == 1:
        return result
    
    # unknown column or ambiguous reference
    return [ResultType(Type.NONE)]

# endregion

# region unary op

@get_type.register
def _(expression: exp.Neg, referenced_tables: list[Table]) -> list[ResultType]:
    inner_types = get_type(expression.this, referenced_tables)

    if len(inner_types) != 1:
        return [ResultType(Type.NONE)]
    
    if not inner_types[0].constant:
        return [ResultType(Type.NONE)]
    
    if inner_types[0].type == Type.NUMBER:
        return [ResultType(Type.NUMBER, constant=True, value=inner_types[0].value, nullable=False)]
    
    # handle implicit casts
    if inner_types[0].type == Type.STRING and to_number(inner_types[0]):
        return [ResultType(Type.NUMBER, constant=True, value=inner_types[0].value, nullable=False)]
        
    return [ResultType(Type.NONE)]

@get_type.register
def _(expression: exp.Not, referenced_tables: list[Table]) -> list[ResultType]:
    inner_types = get_type(expression.this, referenced_tables)

    if len(inner_types) != 1:
        return [ResultType(Type.NONE)]
    
    if inner_types[0].type == Type.BOOLEAN or inner_types[0].type == Type.NULL:
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]
    
    return [ResultType(Type.NONE)]

@get_type.register
def _(expression: exp.Paren, referenced_tables: list[Table]) -> list[ResultType]:
    return get_type(expression.this, referenced_tables)

# endregion

# region binary op

@get_type.register
def _(expression: exp.Binary, referenced_tables: list[Table]) -> list[ResultType]:
    left_types = get_type(expression.this, referenced_tables)
    right_types = get_type(expression.expression, referenced_tables)

    if len(left_types) != 1 or len(right_types) != 1:
        return [ResultType(Type.NONE)]
    
    # handle comparison operators
    if isinstance(expression, exp.Predicate):
        return [typecheck_comparisons(left_types[0], right_types[0], expression)]
            
    
    if left_types[0].type == Type.NULL or right_types[0].type == Type.NULL:
        return [ResultType(Type.NULL)]
    
    if not left_types[0].constant or not right_types[0].constant:
        return [ResultType(Type.NONE)]
    
    if left_types[0].type == Type.NUMBER and right_types[0].type == Type.NUMBER:
        return [ResultType(Type.NUMBER, constant=True, nullable=False)]
    
    # handle implicit casts
    if left_types[0].type == Type.NUMBER or right_types[0].type == Type.NUMBER:
        if to_number(left_types[0]) and to_number(right_types[0]):
            return [ResultType(Type.NUMBER, constant=True, nullable=False)]
        
    return [ResultType(Type.NONE)]

# handle comparison typechecking (e.g =, <, >, etc.)
def typecheck_comparisons(type1: ResultType, type2: ResultType, expression: exp.Binary) -> ResultType:

    # for boolean comparisons we can have only equality/inequality
    if type1.type == Type.BOOLEAN and type2.type == Type.BOOLEAN:
        return ResultType(Type.BOOLEAN, constant=True, nullable=False) if isinstance(expression, (exp.EQ, exp.NEQ)) else ResultType(Type.NONE)
    
    # if null appears in comparison, result is unknown so we return NONE
    if type1.type == Type.NULL or type2.type == Type.NULL:
        return ResultType(Type.NONE)

    if type1.type == type2.type:
        return ResultType(Type.BOOLEAN, constant=True, nullable=False)
    
    # handle implicit casts
    if type1.type == Type.NUMBER or type2.type == Type.NUMBER:
        if to_number(type1) and to_number(type2):
            return ResultType(Type.BOOLEAN, constant=True, nullable=False)
        
    if type1.type == Type.DATE or type2.type == Type.DATE:
        if to_date(type1) and to_date(type2):
            return ResultType(Type.BOOLEAN, constant=True, nullable=False)

    return ResultType(Type.NONE)

@get_type.register
def _(expression: exp.Alias, referenced_tables: list[Table]) -> list[ResultType]:
    return get_type(expression.this, referenced_tables)

# AND, OR
@get_type.register
def _(expression: exp.Connector, referenced_tables: list[Table]) -> list[ResultType]:
    left_types = get_type(expression.this, referenced_tables)
    right_types = get_type(expression.expression, referenced_tables)

    if len(left_types) != 1 or len(right_types) != 1:
        return [ResultType(Type.NONE)]
    
    if left_types[0].type == Type.BOOLEAN and right_types[0].type == Type.BOOLEAN:
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]
    
    return [ResultType(Type.NONE)]

# endregion

# region utils

def to_date(res: ResultType) -> bool:
    if res.type == Type.DATE:
        return True
    if res.type == Type.STRING and res.value is not None:
        try:
            parse(res.value)
            return True
        except ValueError:
            return False
    return False
    
def to_number(res: ResultType) -> bool:
    if res.type == Type.NUMBER:
        return True
    if res.type == Type.STRING and res.value is not None:
        try:
            float(res.value)
            return True
        except ValueError:
            return False
    return False

def to_res_type(original_type: str) -> Type:
    original_type = original_type.upper()
    if original_type in ["VARCHAR", "TEXT"] or original_type.startswith("CHAR"):
        return Type.STRING
    elif original_type in ["DECIMAL", "NUMERIC", "REAL"] or original_type.startswith("INT") or original_type.startswith("FLOAT") or original_type.startswith("DOUBLE") or original_type.endswith("INT"):
        return Type.NUMBER
    elif original_type.startswith("BOOL"):
        return Type.BOOLEAN
    elif original_type.startswith("DATE") or original_type.startswith("TIMESTAMP"):
        return Type.DATE
    else:
        return Type.NONE

# endregion