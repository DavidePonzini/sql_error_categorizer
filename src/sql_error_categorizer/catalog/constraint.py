from dataclasses import dataclass
from enum import Enum

class UniqueConstraintType(Enum):
    '''Enumeration of unique constraint types.'''
    PRIMARY_KEY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'

@dataclass(frozen=True)
class UniqueConstraintColumn:
    '''Represents a column that is part of a unique constraint.'''
    
    name: str
    '''Name of the column.'''

    table_idx: int | None = None
    '''Index of the table in `referenced_tables`. If None, the column is not associated with a specific table in `referenced_tables`.'''

    def __repr__(self) -> str:
        if self.table_idx is not None:
            return f'{self.table_idx}.{self.name}'
        return self.name

    def to_dict(self) -> dict:
        '''Converts the UniqueConstraintColumn to a dictionary.'''
        return {
            'name': self.name,
            'table_idx': self.table_idx,
        }
    
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, UniqueConstraintColumn):
            return False

        return self.name == value.name and self.table_idx == value.table_idx

    @classmethod
    def from_dict(cls, data: dict) -> 'UniqueConstraintColumn':
        '''Creates a UniqueConstraintColumn from a dictionary.'''
        return cls(
            name=data['name'],
            table_idx=data.get('table_idx')
        )

@dataclass
class UniqueConstraint:
    '''A unique constraint on a set of columns in a table.'''

    columns: set[UniqueConstraintColumn]
    constraint_type: UniqueConstraintType

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        return f'{indent}UniqueConstraint({self.constraint_type.value}: {self.columns})'
    
    def to_dict(self) -> dict:
        '''Converts the UniqueConstraint to a dictionary.'''
        return {
            'columns': [col.to_dict() for col in self.columns],
            'constraint_type': self.constraint_type.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'UniqueConstraint':
        '''Creates a UniqueConstraint from a dictionary.'''
        return cls(
            columns={UniqueConstraintColumn.from_dict(col) for col in data['columns']},
            constraint_type=UniqueConstraintType(data['constraint_type'])
        )
