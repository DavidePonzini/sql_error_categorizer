'''Builds a catalog of existing schemas/tables/columns by executing the provided SQL string in a temporary PostgreSQL database.'''

from .catalog import Catalog, Schema, Table, Column, UniqueConstraintType
import psycopg2
import time
from . import queries

def build_catalog(sql_string: str, *, hostname: str, port: int, user: str, password: str, use_temp_schema: bool = True) -> Catalog:
    '''Builds a catalog by executing the provided SQL string in a temporary PostgreSQL database.'''
    result = Catalog()

    if sql_string.strip() == '':
        return result

    conn = psycopg2.connect(host=hostname, port=port, user=user, password=password)
    cur = conn.cursor()
    
    # Use a temporary schema with a fixed name
    if use_temp_schema:
        schema_name = f'sql_error_categorizer_{time.time_ns()}'
        cur.execute(f'CREATE schema {schema_name};')
        cur.execute(f'SET search_path TO {schema_name};')
    else:
        schema_name = '%'   # TODO: it's a bit hackish, find a more stable solution
    
    # Create the tables
    cur.execute(sql_string)

    from dav_tools import messages
    messages.info(queries.COLUMNS(schema_name))

    # Fetch the catalog information
    cur.execute(queries.COLUMNS(schema_name))
    columns_info = cur.fetchall()

    messages.debug(f'Fetched {len(columns_info)} columns from the database.')

    for column in columns_info:
        messages.debug(f'Processing column: {column}')
        schema_name, table_name, column_name, column_type, numeric_precision, numeric_scale, is_nullable, fk_schema, fk_table, fk_column = column

        result.add_column(schema_name, table_name, column_name,
                          column_type, numeric_precision, numeric_scale,
                          is_nullable,
                          fk_schema, fk_table, fk_column)

    # Fetch unique constraints (including primary keys)
    cur.execute(queries.UNIQUE_COLUMNS(schema_name))
    unique_constraints_info = cur.fetchall()
    for constraint in unique_constraints_info:
        schema_name, table_name, constraint_type, columns = constraint
        columns = set(columns.strip('{}').split(','))  # Postgres returns {col1,col2,...}

        if constraint_type == 'PRIMARY KEY':
            constraint_type = UniqueConstraintType.PRIMARY_KEY
        elif constraint_type == 'UNIQUE':
            constraint_type = UniqueConstraintType.UNIQUE
        else:
            raise ValueError(f'Unknown constraint type: {constraint_type}')

        result[schema_name][table_name].add_unique_constraint(columns, constraint_type)

    # Clean up
    if use_temp_schema:
        cur.execute(f'DROP schema {schema_name} CASCADE;')
    conn.rollback()     # no need to save anything

    return result

def load_json(path: str) -> Catalog:
    '''Loads a catalog from a JSON file.'''
    return Catalog.load_json(path)

