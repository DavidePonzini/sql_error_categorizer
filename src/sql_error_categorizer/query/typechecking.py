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
        return isinstance(other, ErrorType)
    
class NotImplementedType(Type):
    def __repr__(self):
        return "NotImplementedType()"
    
    def __eq__(self, other):
        return isinstance(other, NotImplementedType)
    
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

    types_list = []
    for item in expression.expressions:
        item_type = get_type(item, referenced_tables)

        if item_type == ErrorType or item_type == NotImplementedType:
            return item_type

        types_list.append(item_type)

    return ErrorType("Empty tuple has no type") if not types_list else TupleType(types_list)

# We use cast only for date conversions (it is reasonably valid if we encounter "DATE '2020-01-01'" or "TIMESTAMP '2020-01-01 10:00:00'")
@get_type.register
def _(expression: exp.Cast, referenced_tables: list[Table]) -> Type:

    original_type = get_type(expression.this, referenced_tables)

    if original_type == ErrorType or original_type == NotImplementedType:
        return original_type
    
    # the casting is valid if the original type is a date or a string that can be converted to date
    if to_date(original_type):

        to = expression.args.get("to")
        if not to:
            return ErrorType(f"CAST missing target type: {expression.sql()}")

        if to.sql().upper() in ["DATE", "TIMESTAMP"]:
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
        return result
    
    return ErrorType(f"Ambiguous column reference: {col_name}")

# endregion

# region unary ops

@get_type.register
def _(expression: exp.Neg, referenced_tables: list[Table]) -> Type:
    inner_types = get_type(expression.this, referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types
    
    if inner_types == TupleType:
        return ErrorType("Invalid use of unary minus on tuple expression")
    
    if not inner_types.constant:
        return ErrorType("Invalid use of unary minus on non-constant expression")

    if to_number(inner_types):
        return AtomicType(EnumType.NUMBER, nullable=False, constant=True)
        
    return ErrorType("Invalid use of unary minus on non-numeric expression")

@get_type.register
def _(expression: exp.Not, referenced_tables: list[Table]) -> Type:
    inner_types = get_type(expression.this, referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types
    
    if inner_types == TupleType:
        return ErrorType("Invalid use of NOT on tuple expression")
    
    if inner_types == AtomicType(EnumType.BOOLEAN):
        return AtomicType(EnumType.BOOLEAN, constant=True, nullable=False)

    return ErrorType("Invalid use of NOT on non-boolean expression")

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
        return ErrorType("Invalid use of DISTINCT")

    inner_types = get_type(expression.expressions[0], referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types

    if inner_types == TupleType:
        return ErrorType("Invalid use of DISTINCT on tuple expression")
    
    return inner_types

# endregion

# region functions

@get_type.register
def _(expression: exp.Count, referenced_tables: list[Table]) -> Type:
    inner_types = get_type(expression.this, referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types

    if inner_types == TupleType:
        return ErrorType("Invalid use of COUNT on tuple expression")

    return AtomicType(EnumType.NUMBER, nullable=inner_types.nullable, constant=True)

@get_type.register
def _(expression: exp.Avg, referenced_tables: list[Table]) -> Type:
    inner_types = get_type(expression.this, referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types

    if inner_types == TupleType:
        return ErrorType("Invalid use of AVG on tuple expression")

    if to_number(inner_types):
        return AtomicType(EnumType.NUMBER, nullable=inner_types.nullable, constant=True)

    return ErrorType("Invalid use of AVG on non-numeric expression")

@get_type.register
def _(expression: exp.Sum, referenced_tables: list[Table]) -> Type:
    inner_types = get_type(expression.this, referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types

    if inner_types == TupleType:
        return ErrorType("Invalid use of SUM on tuple expression")

    if to_number(inner_types):
        return AtomicType(EnumType.NUMBER, nullable=inner_types.nullable, constant=True)

    return ErrorType("Invalid use of SUM on non-numeric expression")

@get_type.register
def _(expression: exp.Min, referenced_tables: list[Table]) -> Type:
    inner_types = get_type(expression.this, referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types
    
    if inner_types == TupleType:
        return ErrorType("Invalid use of MIN on tuple expression")

    if inner_types == AtomicType(EnumType.BOOLEAN):
        return ErrorType("Invalid use of MIN on boolean expression")

    return AtomicType(inner_types, nullable=inner_types.nullable, constant=True)

@get_type.register
def _(expression: exp.Max, referenced_tables: list[Table]) -> Type:
    inner_types = get_type(expression.this, referenced_tables)

    if inner_types == ErrorType or inner_types == NotImplementedType:
        return inner_types
    
    if inner_types == TupleType:
        return ErrorType("Invalid use of MAX on tuple expression")

    if inner_types == AtomicType(EnumType.BOOLEAN):
        return ErrorType("Invalid use of MAX on boolean expression")

    return AtomicType(inner_types, nullable=inner_types.nullable, constant=True)

@get_type.register
def _(expression: exp.Concat, referenced_tables: list[Table]) -> Type:
    arg_types = []
    for arg in expression.expressions:
        i_type = get_type(arg, referenced_tables)
        if i_type == ErrorType or i_type == NotImplementedType:
            return i_type
        arg_types.append(i_type)
    
    # if all args are NULL, result is NULL
    if all(t == AtomicType(EnumType.NULL) for t in arg_types):
        return AtomicType(EnumType.NULL, nullable=True, constant=True)
    
    constant = all(t.constant for t in arg_types)
    nullable = any(t.nullable for t in arg_types)

    return AtomicType(EnumType.STRING, constant=constant, nullable=nullable)

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

# region logical ops
@get_type.register
def _(expression: exp.Like, referenced_tables: list[Table]) -> list[ResultType]:
    left_types = get_type(expression.this, referenced_tables)
    right_types = get_type(expression.expression, referenced_tables)

    if len(left_types) != 1 or len(right_types) != 1:
        return [ResultType(Type.NONE)]

    if Type.STRING == left_types[0].type == right_types[0].type and right_types[0].constant:
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]
        
    return [ResultType(Type.NONE)]

@get_type.register
def _(expression: exp.Is, referenced_tables: list[Table]) -> list[ResultType]:  
    left_types = get_type(expression.this, referenced_tables)
    right_types = get_type(expression.expression, referenced_tables)

    if len(left_types) != 1 or len(right_types) != 1:
        return [ResultType(Type.NONE)]
    
    # IS NULL or IS NOT NULL
    if right_types[0].type == Type.NULL and right_types[0].constant:
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]

    # IS TRUE, IS FALSE, IS UNKNOWN
    if Type.BOOLEAN == right_types[0].type == left_types[0].type and right_types[0].constant:
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]
    
    return [ResultType(Type.NONE)]

@get_type.register
def _(expression: exp.Between, referenced_tables: list[Table]) -> list[ResultType]:
    target_types = get_type(expression.this, referenced_tables)
    low_types = get_type(expression.args.get("low"), referenced_tables)
    high_types = get_type(expression.args.get("high"), referenced_tables)

    if len(target_types) != 1 or len(low_types) != 1 or len(high_types) != 1:
        return [ResultType(Type.NONE)]

    if any(t.type == Type.NONE for t in [target_types[0], low_types[0], high_types[0]]):
        return [ResultType(Type.NONE)]

    if target_types[0].type == low_types[0].type == high_types[0].type:
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]
    
    # handle implicit casts
    if to_number(target_types[0]) and to_number(low_types[0]) and to_number(high_types[0]):
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]

    if to_date(target_types[0]) and to_date(low_types[0]) and to_date(high_types[0]):
        return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]

    return [ResultType(Type.NONE)]

@get_type.register
def _(expression: exp.In, referenced_tables: list[Table]) -> list[ResultType]:
    target_types = get_type(expression.this, referenced_tables)

    if target_types.type == Type.NONE:
        return [ResultType(Type.NONE)]
    
    if expression.expressions:
        for item in expression.expressions:
            item_types = get_type(item, referenced_tables)
            if not check_list_types(target_types, item_types):
                return [ResultType(Type.NONE)]

    elif (subquery := expression.args.get("query")) is not None:
        ...
        # TODO: handle subqueries properly
        

    else:
        return [ResultType(Type.NONE)]


    return [ResultType(Type.BOOLEAN, constant=True, nullable=False)]
        

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

def to_date(target: Type) -> bool:
    if target == AtomicType(EnumType.DATE):
        return True
    if target == AtomicType(EnumType.STRING) and target.name is not None:
        try:
            parse(target.value)
            return True
        except ValueError:
            return False
    return False
    
def to_number(target: Type) -> bool:
    if target == AtomicType(EnumType.NUMBER):
        return True
    if target == AtomicType(EnumType.STRING) and target.name is not None:
        try:
            float(target.value)
            return True
        except ValueError:
            return False
    return False

def to_res_type(original_type: str) -> Type:
    original_type = original_type.upper()
    if original_type in ["VARCHAR", "TEXT"] or original_type.startswith("CHAR"):
        return AtomicType(EnumType.STRING)
    elif original_type in ["DECIMAL", "NUMERIC", "REAL"] or original_type.startswith("INT") or original_type.startswith("FLOAT") or original_type.startswith("DOUBLE") or original_type.endswith("INT"):
        return AtomicType(EnumType.NUMBER)
    elif original_type.startswith("BOOL"):
        return AtomicType(EnumType.BOOLEAN)
    elif original_type.startswith("DATE") or original_type.startswith("TIMESTAMP"):
        return AtomicType(EnumType.DATE)
    else:
        return NotImplementedType()

# endregion