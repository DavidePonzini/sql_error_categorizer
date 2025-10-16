from dataclasses import dataclass
from sqlglot import exp

@dataclass
class OrderByColumn:
    '''Represents a single column in an ORDER BY clause, with its sorting direction.'''
    column: str
    '''The name of the column to order by.'''
    table: str
    '''The table the column belongs to. Its name matches `referenced_tables` in the query it belongs to.'''
    ascending: bool = True
    '''The sorting direction, either True for ascending or False for descending. Defaults to True.'''


def normalize_identifier_name(identifier: str) -> str:
    '''Normalize an SQL identifier by stripping quotes and converting to lowercase if unquoted.'''
    if identifier.startswith('"') and identifier.endswith('"') and len(identifier) > 1:
        return identifier[1:-1]
    
    return identifier.lower()

def normalize_ast_column_real_name(column: exp.Column) -> str:
    '''Returns the column real name, in lowercase if unquoted.'''

    quoted = column.this.quoted
    name = column.this.name

    return name if quoted else name.lower()

def normalize_ast_column_name(column: exp.Column) -> str:
    '''Returns the column name or alias, in lowercase if unquoted.'''
    
    if column.args.get('alias'):
        quoted = column.args['alias'].args.get('quoted', False)
        name = column.alias_or_name

        return name if quoted else name.lower()

    return normalize_ast_column_real_name(column)

def normalize_ast_column_table(column: exp.Column) -> str | None:
    '''Returns the table name or alias for the column, in lowercase if unquoted.'''
    
    if column.args.get('table'):
        quoted = column.args['table'].quoted
        name = column.table

        return name if quoted else name.lower()
    
    return None

def normalize_ast_table_real_name(table: exp.Table) -> str:
    '''Returns the table real name, in lowercase if unquoted.'''

    quoted = table.this.quoted
    name = table.this.name

    return name if quoted else name.lower()


def normalize_ast_table_name(table: exp.Table) -> str:
    '''Returns the table name or alias, in lowercase if unquoted.'''
    
    if table.args.get('alias'):
        quoted = table.args['alias'].args.get('quoted', False)
        name = table.alias_or_name

        return name if quoted else name.lower()

    return normalize_ast_table_real_name(table)



def normalize_ast_schema_name(table: exp.Table) -> str | None:
    '''Returns the schema name, in lowercase if unquoted.'''
    
    if table.args.get('db'):
        quoted = table.args['db'].quoted
        name = table.db

        return name if quoted else name.lower()
    
    return None

def normalize_ast_subquery_name(subquery: exp.Subquery) -> str:
    '''Returns the subquery name or alias, in lowercase if unquoted.'''
    
    if subquery.args.get('alias'):
        quoted = subquery.args['alias'].this.quoted
        name = subquery.alias_or_name

        return name if quoted else name.lower()

    return subquery.alias_or_name