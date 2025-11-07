from dataclasses import dataclass, field
from enum import Enum

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

    columns: set[UniqueConstraintColumn] = field(default_factory=set)
    is_pk: bool = False
    '''Whether this unique constraint is a primary key.'''

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        if self.is_pk:
            return f'{indent}PRIMARY KEY({self.columns})'
        return f'{indent}UNIQUE({self.columns})'

    def to_dict(self) -> dict:
        '''Converts the UniqueConstraint to a dictionary.'''
        return {
            'columns': [col.to_dict() for col in self.columns],
            'is_pk': self.is_pk,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'UniqueConstraint':
        '''Creates a UniqueConstraint from a dictionary.'''
        return cls(
            columns={UniqueConstraintColumn.from_dict(col) for col in data['columns']},
            is_pk=data['is_pk']
        )
