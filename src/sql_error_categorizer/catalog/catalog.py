from dataclasses import dataclass, field
from typing import Self
from enum import Enum
from copy import deepcopy

class UniqueConstraintType(Enum):
    PRIMARY_KEY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'

class UniqueConstraint:
    def __init__(self, columns: set[str], constraint_type: UniqueConstraintType) -> None:
        self.columns = {col.lower() for col in columns}
        self.constraint_type = constraint_type

    def __repr__(self) -> str:
        return f"UniqueConstraint({self.constraint_type.value}: {self.columns})"

@dataclass
class Column:
    name: str
    table: 'Table' = field(repr=False)
    column_type: str
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    is_nullable: bool = True
    fk_schema: str | None = None
    fk_table: str | None = None
    fk_column: str | None = None

    @property
    def is_pk(self) -> bool:
        unique_constraints = [uc for uc in self.table.unique_constraints if self.name in uc.columns]
        return any(uc.constraint_type == UniqueConstraintType.PRIMARY_KEY for uc in unique_constraints)

    @property
    def is_fk(self) -> bool:
        return all([self.fk_schema, self.fk_table, self.fk_column])


@dataclass
class Table:
    name: str
    schema: 'Schema' = field(repr=False)
    unique_constraints: set[UniqueConstraint] = field(default_factory=set)
    _columns: dict[str, Column] = field(default_factory=dict)

    def add_unique_constraint(self, columns: set[str], constraint_type: UniqueConstraintType) -> None:
        self.unique_constraints.add(UniqueConstraint(columns, constraint_type))

    def add_column(self, name: str, column_type: str, numeric_precision: int | None = None, numeric_scale: int | None = None,
                   is_nullable: bool = True, fk_schema: str | None = None, fk_table: str | None = None, fk_column: str | None = None) -> None:
        column = Column(name=name,
                        table=self,
                        column_type=column_type, numeric_precision=numeric_precision, numeric_scale=numeric_scale,
                        is_nullable=is_nullable,
                        fk_schema=fk_schema, fk_table=fk_table, fk_column=fk_column)
        self._columns[name] = column

    def get_column(self, column_name: str) -> Column:
        return self._columns[column_name.lower()]

    @property
    def columns(self) -> set[str]:
        '''Returns all column names in the table.'''
        return set(self._columns.keys())
    

@dataclass
class Schema:
    name: str
    _tables: dict[str, Table] = field(default_factory=dict)

    def get_table(self, table_name: str) -> Table:
        '''Gets a table from the schema, creating it if it does not exist.'''
        table_name = table_name.lower()  # Normalize names

        if table_name not in self._tables:
            self._tables[table_name] = Table(name=table_name, schema=self)
        return self._tables[table_name]
    
    def has_table(self, table_name: str) -> bool:
        '''Checks if a table exists in the schema.'''
        return table_name.lower() in self._tables
    
    @property
    def tables(self) -> set[str]:
        '''Returns all table names in the schema.'''
        return set(self._tables.keys())
    
    @property
    def columns(self) -> set[str]:
        '''Returns all column names in the schema, across all tables.'''
        return {col for table in self._tables.values() for col in table.columns}

@dataclass
class Catalog:
    _schemas: dict[str, Schema] = field(default_factory=dict)

    def get_schema(self, schema_name: str) -> Schema:
        '''Gets a schema from the catalog, creating it if it does not exist.'''
        schema_name = schema_name.lower()   # Normalize names

        if schema_name not in self._schemas:
            self._schemas[schema_name] = Schema(schema_name)
        return self._schemas[schema_name]
    
    def has_schema(self, schema_name: str) -> bool:
        '''Checks if a schema exists in the catalog.'''
        return schema_name.lower() in self._schemas
    
    def get_table(self, schema_name: str, table_name: str) -> Table:
        '''Gets a table from the catalog, creating the schema and table if they do not exist.'''
        schema_name = schema_name.lower()
        table_name = table_name.lower()

        schema = self.get_schema(schema_name)
        return schema.get_table(table_name)        

    def has_table(self, schema_name: str, table_name: str) -> bool:
        '''Checks if a table exists in the catalog.'''
        schema_name = schema_name.lower()
        table_name = table_name.lower()

        if not self.has_schema(schema_name):
            return False
        return self.get_schema(schema_name).has_table(table_name)

    def add_column(self, schema_name: str, table_name: str, column_name: str,
                   column_type: str, numeric_precision: int | None = None, numeric_scale: int | None = None,
                   is_nullable: bool = True,
                   fk_schema: str | None = None, fk_table: str | None = None, fk_column: str | None = None) -> None:

        schema_name = schema_name.lower()
        table_name = table_name.lower()
        column_name = column_name.lower()

        '''Adds a column to the catalog, creating the schema and table if they do not exist.'''
        table = self.get_table(schema_name, table_name)

        table.add_column(name=column_name,
                         column_type=column_type, numeric_precision=numeric_precision, numeric_scale=numeric_scale,
                         is_nullable=is_nullable,
                         fk_schema=fk_schema, fk_table=fk_table, fk_column=fk_column)

    @property
    def schemas(self) -> set[str]:
        '''Returns all schema names in the catalog.'''
        return set(self._schemas.keys())

    @property
    def tables(self) -> set[str]:
        '''Returns all table names in the catalog, across all schemas.'''
        return {table for schema in self._schemas.values() for table in schema.tables}

    @property
    def columns(self) -> set[str]:
        '''Returns all column names in the catalog, across all tables in all schemas.'''
        return {col for schema in self._schemas.values() for col in schema.columns}

    @property
    def functions(self) -> set[str]:
        # TODO: Implement function cataloging
        return set()

    def copy(self) -> Self:
        '''Creates a deep copy of the catalog.'''
        return deepcopy(self)