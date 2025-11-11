from .types import ResultType, DataType
from dateutil.parser import parse

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

def error_message(expected: ResultType | str, found: ResultType | str) -> str:

    if isinstance(expected, ResultType):
        expected = expected.data_type.value

    if isinstance(found, ResultType):
        found = found.data_type.value

    return f"Expected type {expected.lower()}, but found type {found.lower()}."