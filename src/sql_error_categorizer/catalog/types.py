from sqlglot.expressions import DataType
from dataclasses import dataclass, field


@dataclass
class ResultType:
    messages: list[(str,str)] = field(default_factory=list)
    data_type: DataType.Type = DataType.Type.UNKNOWN
    nullable: bool = True
    constant: bool = False
    value: str | None = None

@dataclass
class AtomicType(ResultType):

    def __str__(self) -> str:
        return self.data_type.value.lower()

    def __eq__(self, other):
        if not isinstance(other, AtomicType):
            return False

        # handle numeric equivalence (e.g. INT and FLOAT are compatible)
        if self.data_type in DataType.NUMERIC_TYPES:
            return other.data_type in DataType.NUMERIC_TYPES
        
        # handle text equivalence (e.g. VARCHAR and TEXT are compatible)
        if self.data_type in DataType.TEXT_TYPES:
            return other.data_type in DataType.TEXT_TYPES

        # handle temporal equivalence (e.g. DATE and TIMESTAMP are compatible)
        if self.data_type in DataType.TEMPORAL_TYPES:
            return other.data_type in DataType.TEMPORAL_TYPES

        return self.data_type == other.data_type

@dataclass
class TupleType(ResultType):

    # we use LIST to represent tuples (since we will never use this constructor)
    data_type: DataType.Type = DataType.Type.LIST 
    types: list[ResultType] = field(default_factory=list)

    def __str__(self) -> str:
        return f"tuple({', '.join(str(target_type) for target_type in self.types)})"

    def __eq__(self, other):
        if not isinstance(other, TupleType):
            return False
        return self.types == other.types
    

def resolve_type(type_str: str) -> DataType.Type:
    '''
    Resolves a string representation of a type to a Type object.
    '''
    return DataType.build(type_str, udt=True).this