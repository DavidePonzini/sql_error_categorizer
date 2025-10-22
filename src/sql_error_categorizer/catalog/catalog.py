from dataclasses import dataclass, field
import json
from typing import Self
from enum import Enum
from copy import deepcopy

# region UniqueConstraint
class UniqueConstraintType(Enum):
    PRIMARY_KEY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'

class UniqueConstraint:
    def __init__(self, columns: set[str], constraint_type: UniqueConstraintType) -> None:
        self.columns = columns
        self.constraint_type = constraint_type

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        return f'{indent}UniqueConstraint({self.constraint_type.value}: {self.columns})'
    
    def to_dict(self) -> dict:
        return {
            'columns': list(self.columns),  # JSON-friendly (list)
            'constraint_type': self.constraint_type.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'UniqueConstraint':
        return cls(columns=set(c.lower() for c in data['columns']),
                   constraint_type=UniqueConstraintType(data['constraint_type']))
# endregion

# region Column
@dataclass
class Column:
    name: str
    column_type: str = 'UNKNOWN'
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    is_nullable: bool = True
    is_constant: bool = False
    fk_schema: str | None = None
    fk_table: str | None = None
    fk_column: str | None = None

    @property
    def is_fk(self) -> bool:
        return all([self.fk_schema, self.fk_table, self.fk_column])

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        return f'{indent}Column(name=\'{self.name}\', type=\'{self.column_type}\', is_fk={self.is_fk}, is_nullable={self.is_nullable})'

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'column_type': self.column_type,
            'numeric_precision': self.numeric_precision,
            'numeric_scale': self.numeric_scale,
            'is_nullable': self.is_nullable,
            'fk_schema': self.fk_schema,
            'fk_table': self.fk_table,
            'fk_column': self.fk_column,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Column':
        return cls(
            name=data['name'],
            column_type=data['column_type'],
            numeric_precision=data.get('numeric_precision'),
            numeric_scale=data.get('numeric_scale'),
            is_nullable=data.get('is_nullable', True),
            fk_schema=(data.get('fk_schema') or None),
            fk_table=(data.get('fk_table') or None),
            fk_column=(data.get('fk_column') or None),
        )
# endregion

# region Table
@dataclass
class Table:
    '''A database table, with columns and unique constraints. Supports multiple columns with the same name (e.g. from joins).'''
    name: str
    unique_constraints: list[UniqueConstraint] = field(default_factory=list)
    columns: list[Column] = field(default_factory=list)

    def add_unique_constraint(self, columns: set[str], constraint_type: UniqueConstraintType) -> None:
        self.unique_constraints.append(UniqueConstraint(columns, constraint_type))

    def add_column(self, name: str, column_type: str, numeric_precision: int | None = None, numeric_scale: int | None = None,
                   is_nullable: bool = True, fk_schema: str | None = None, fk_table: str | None = None, fk_column: str | None = None) -> Column:
        column = Column(name=name,
                        column_type=column_type, numeric_precision=numeric_precision, numeric_scale=numeric_scale,
                        is_nullable=is_nullable,
                        fk_schema=fk_schema, fk_table=fk_table, fk_column=fk_column)
        self.columns.append(column)
        return column
    
    def has_column(self, column_name: str) -> bool:
        '''Checks if a column exists in the table.'''
        return any(col.name == column_name for col in self.columns)

    def __getitem__(self, column_name: str) -> Column:
        '''Gets a column from the table, creating it if it does not exist.'''
        for col in self.columns:
            if col.name == column_name:
                return col

        new_col = Column(name=column_name)
        self.columns.append(new_col)
        return new_col

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level

        columns = '\n'.join([col.__repr__(level + 1) for col in self.columns])
        if len(self.unique_constraints) < 2:
            unique_constraints_str = ', '.join([uc.__repr__(0) for uc in self.unique_constraints])
        else:
            unique_constraints_str = '\n' + '\n'.join([uc.__repr__(level + 1) for uc in self.unique_constraints]) + '\n' + indent
        
        if len(self.columns) > 0:
            columns = '\n' + columns + '\n' + indent
        
        return f'{indent}Table(name=\'{self.name}\', columns=[{columns}], unique_constraints=[{unique_constraints_str}])'

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'unique_constraints': [uc.to_dict() for uc in self.unique_constraints],
            'columns': [col.to_dict() for col in self.columns],
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Table':
        table = cls(name=data['name'])
        # Unique constraints first (so Column.is_pk works immediately on repr, etc.)
        for uc_data in data.get('unique_constraints', []):
            uc = UniqueConstraint.from_dict(uc_data)
            table.unique_constraints.append(uc)
        # Columns
        for col_data in (data.get('columns') or []):
            col = Column.from_dict(col_data)
            # Keep internal store normalized to lowercase
            table.columns.append(col)
        return table
# endregion

# region Schema
@dataclass
class Schema:
    name: str
    _tables: dict[str, Table] = field(default_factory=dict)
    functions: set[str] = field(default_factory=set)

    def __getitem__(self, table_name: str) -> Table:
        '''Gets a table from the schema, creating it if it does not exist.'''
        if table_name not in self._tables:
            self._tables[table_name] = Table(table_name)
        return self._tables[table_name]

    def __setitem__(self, table_name: str, table: Table) -> None:
        '''Sets a table in the schema, replacing any existing table with the same name.'''
        self._tables[table_name] = table
    
    def has_table(self, table_name: str) -> bool:
        '''Checks if a table exists in the schema.'''
        return table_name in self._tables
    
    def has_column(self, table_name: str, column_name: str) -> bool:
        '''Checks if a column exists in the schema.'''
        if not self.has_table(table_name):
            return False
        return self.__getitem__(table_name).has_column(column_name)

    @property
    def table_names(self) -> set[str]:
        '''Returns all table names in the schema.'''
        return set(self._tables.keys())

    def __repr__(self, level: int = 0) -> str:
        indent = '  ' * level
        tables = '\n'.join([table.__repr__(level + 1) for table in self._tables.values()])
        return f'{indent}Schema(name=\'{self.name}\', tables=[\n{tables}\n{indent}])'

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'tables': {name: tbl.to_dict() for name, tbl in self._tables.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Schema':
        schema = cls(name=data['name'])
        for _, tbl_data in (data.get('tables') or {}).items():
            tbl = Table.from_dict(tbl_data)
            schema._tables[tbl.name] = tbl
        return schema
# endregion

# region Catalog
@dataclass
class Catalog:
    _schemas: dict[str, Schema] = field(default_factory=dict)

    def __getitem__(self, schema_name: str) -> Schema:
        '''Gets a schema from the catalog, creating it if it does not exist.'''

        if schema_name not in self._schemas:
            self._schemas[schema_name] = Schema(schema_name)
        return self._schemas[schema_name]
    
    def __setitem__(self, schema_name: str, schema: Schema) -> Schema:
        '''Sets a schema in the catalog, replacing any existing schema with the same name.'''
        
        self._schemas[schema_name] = schema
        return schema
    
    def has_schema(self, schema_name: str) -> bool:
        '''Checks if a schema exists in the catalog.'''
        
        return schema_name in self._schemas
    
    def copy_table(self, schema_name: str, table_name: str, table: Table) -> Table:
        '''Copies a table into the catalog, creating the schema if it does not exist.'''
        
        new_table = deepcopy(table)
        self[schema_name][table_name] = new_table
        
        return new_table

    def has_table(self, schema_name: str, table_name: str) -> bool:
        '''
            Checks if a table exists in the specified schema in the catalog.

            Returns False if the schema or table do not exist.
        '''

        if not self.has_schema(schema_name):
            return False
        return self.__getitem__(schema_name).has_table(table_name)

    def add_column(self, schema_name: str, table_name: str, column_name: str,
                   column_type: str, numeric_precision: int | None = None, numeric_scale: int | None = None,
                   is_nullable: bool = True,
                   fk_schema: str | None = None, fk_table: str | None = None, fk_column: str | None = None) -> None:
        '''Adds a column to the catalog, creating the schema and table if they do not exist.'''

        self[schema_name][table_name].add_column(name=column_name,
                                                 column_type=column_type, numeric_precision=numeric_precision, numeric_scale=numeric_scale,
                                                 is_nullable=is_nullable,
                                                 fk_schema=fk_schema, fk_table=fk_table, fk_column=fk_column)
        
    @property
    def schema_names(self) -> set[str]:
        '''Returns all schema names in the catalog.'''
        return set(self._schemas.keys())

    @property
    def table_names(self) -> set[str]:
        '''Returns all table names in the catalog, regardless of schema.'''

        result = set()
        for schema in self._schemas.values():
            result.update(schema.table_names)
        return result

    def copy(self) -> Self:
        '''Creates a deep copy of the catalog.'''
        return deepcopy(self)
    
    def __repr__(self) -> str:
        schemas = [schema.__repr__(1) for schema in self._schemas.values()]

        result = 'Catalog('
        for schema in schemas:
            result += '\n' + schema
        result += '\n)'

        return result

    
    def to_dict(self) -> dict:
        return {
            'version': 1,
            'schemas': {name: sch.to_dict() for name, sch in self._schemas.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Catalog':
        cat = cls()
        for _, sch_data in (data.get('schemas') or {}).items():
            sch = Schema.from_dict(sch_data)
            cat._schemas[sch.name] = sch
        return cat

    # String-based JSON (handy for DB/blob storage)
    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, s: str) -> 'Catalog':
        return cls.from_dict(json.loads(s))

    # Convenience file helpers
    def save_json(self, path: str, *, indent: int | None = 2) -> None:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=indent)

    @classmethod
    def load_json(cls, path: str) -> 'Catalog':
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return cls.from_dict(data)