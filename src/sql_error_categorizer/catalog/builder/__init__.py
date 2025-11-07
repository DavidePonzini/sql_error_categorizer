from . import queries
from ..catalog import Catalog
from ..constraint import UniqueConstraintType
import psycopg2
import time

def build_catalog(sql_string: str, *, hostname: str, port: int, user: str, password: str, schema: str | None = None, create_temp_schema: bool = False) -> Catalog:
    '''Builds a catalog by executing the provided SQL string in a temporary PostgreSQL database.'''
    result = Catalog()

    if sql_string.strip() == '':
        return result

    conn = psycopg2.connect(host=hostname, port=port, user=user, password=password)
    cur = conn.cursor()
    
    # Use a temporary schema with a fixed name
    if create_temp_schema:
        if schema is None:
            schema_name = f'sql_error_categorizer_{time.time_ns()}'
        else:
            schema_name = schema
        cur.execute(f'CREATE schema {schema_name};')
        cur.execute(f'SET search_path TO {schema_name};')
    else:
        schema_name = '%' if schema is None else schema
    
    # Create the tables
    cur.execute(sql_string)

    # Fetch the catalog information
    cur.execute(queries.COLUMNS(schema_name))
    columns_info = cur.fetchall()

    for column in columns_info:
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
    if create_temp_schema:
        cur.execute(f'DROP schema {schema_name} CASCADE;')
    conn.rollback()     # no need to save anything

    return result

def load_catalog(path: str) -> Catalog:
    '''Loads a catalog from a JSON file.'''
    return Catalog.load_json(path)