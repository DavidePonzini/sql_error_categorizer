from .catalog import Catalog, UniqueConstraintType
import psycopg2
import time
from . import queries

def build_catalog(sql_string: str, hostname: str, port: int, user: str, password: str, use_temp_schema: bool = True) -> Catalog:
    '''Builds a catalog by executing the provided SQL string in a temporary PostgreSQL database.'''
    result = Catalog()

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
        
        result.get_table(schema_name, table_name).add_unique_constraint(columns, constraint_type)

    # Clean up
    if use_temp_schema:
        cur.execute(f'DROP schema {schema_name} CASCADE;')
    conn.rollback()     # no need to save anything

    return result

# class CatalogBuilder:
#     """Parses CREATE TABLE statements to extract schemas, tables, columns, types, and metadata."""

#     @staticmethod
#     def build_catalog(create_sql: str):
#         catalog = {
#             'schemas': set(),
#             'tables': set(),
#             'columns': set(),
#             'functions': set(),
#             'table_columns': {},
#             'column_metadata': {},  # metadata per table and column
#         }
#         # Split into individual statements
#         statements = sqlparse.split(create_sql)
#         for stmt in statements:
#             stmt_strip = stmt.strip()
#             # Only process CREATE TABLE statements
#             if not stmt_strip.upper().startswith('CREATE TABLE'):
#                 continue
#             # Extract full table name
#             m = re.match(r'CREATE TABLE\s+([^\s(]+)', stmt_strip, re.IGNORECASE)
#             if not m:
#                 continue
#             full_name = m.group(1)
#             # Handle schema.table
#             if '.' in full_name:
#                 schema, table = full_name.split('.', 1)
#                 catalog['schemas'].add(schema)
#             else:
#                 table = full_name
#             catalog['tables'].add(table)
#             # Initialize per-table structures
#             catalog['table_columns'].setdefault(table, set())
#             catalog['column_metadata'].setdefault(table, {})

#             # Extract column block between first parentheses
#             start = stmt_strip.find('(')
#             end = stmt_strip.rfind(')')
#             if start < 0 or end < 0:
#                 continue
#             cols_block = stmt_strip[start+1:end]
#             # Split by commas not within parentheses
#             parts = re.split(r',\s*(?![^()]*\))', cols_block)

#             # Separate column definitions and table constraints
#             col_defs = []
#             constraint_defs = []
#             for part in parts:
#                 part_strip = part.strip()
#                 if re.match(r'^(PRIMARY|FOREIGN|UNIQUE|CONSTRAINT|CHECK|INDEX)\b', part_strip, re.IGNORECASE):
#                     constraint_defs.append(part_strip)
#                 else:
#                     col_defs.append(part_strip)

#             # Process column definitions
#             for part in col_defs:
#                 tokens = part.split()
#                 if not tokens:
#                     continue
#                 # Column name is first token
#                 col = tokens[0].strip('`"')
#                 catalog['columns'].add(col)
#                 catalog['table_columns'][table].add(col)

#                 # Determine type token (with dimensions if any)
#                 col_type = None
#                 if len(tokens) >= 2:
#                     raw_type = tokens[1]
#                     col_type = raw_type

#                 # Initialize metadata defaults
#                 meta = {
#                     'type': col_type,
#                     'nullable': True,
#                     'primary_key': False,
#                     'foreign_key': False,
#                     'unique': False,
#                     'references': None,  # for foreign key: {'table':..., 'column':...}
#                 }
#                 part_upper = part.upper()
#                 # NOT NULL
#                 if re.search(r'\bNOT\s+NULL\b', part_upper):
#                     meta['nullable'] = False
#                 # PRIMARY KEY (inline)
#                 if 'PRIMARY KEY' in part_upper:
#                     meta['primary_key'] = True
#                     meta['nullable'] = False
#                 # UNIQUE (inline, excluding primary key)
#                 if re.search(r'\bUNIQUE\b', part_upper) and 'PRIMARY KEY' not in part_upper:
#                     meta['unique'] = True
#                 # FOREIGN KEY (inline via REFERENCES)
#                 if 'REFERENCES' in part_upper:
#                     fk_match = re.search(r'REFERENCES\s+([^\s(]+)\s*\(\s*([^\s)]+)\s*\)', part, re.IGNORECASE)
#                     if fk_match:
#                         ref_table = fk_match.group(1)
#                         ref_col = fk_match.group(2)
#                         meta['foreign_key'] = True
#                         meta['references'] = {'table': ref_table, 'column': ref_col}
#                 # Save metadata
#                 catalog['column_metadata'][table][col] = meta

#             # Process table-level constraints
#             for part in constraint_defs:
#                 part_upper = part.upper()
#                 # PRIMARY KEY constraint
#                 if 'PRIMARY KEY' in part_upper:
#                     m_pk = re.search(r'\((.*?)\)', part)
#                     if m_pk:
#                         cols = [c.strip('`" ') for c in m_pk.group(1).split(',')]
#                         for c in cols:
#                             if c in catalog['column_metadata'][table]:
#                                 catalog['column_metadata'][table][c]['primary_key'] = True
#                                 catalog['column_metadata'][table][c]['nullable'] = False
#                 # UNIQUE constraint (excluding primary key)
#                 if 'UNIQUE' in part_upper and 'PRIMARY KEY' not in part_upper:
#                     m_uq = re.search(r'\((.*?)\)', part)
#                     if m_uq:
#                         cols = [c.strip('`" ') for c in m_uq.group(1).split(',')]
#                         for c in cols:
#                             if c in catalog['column_metadata'][table]:
#                                 catalog['column_metadata'][table][c]['unique'] = True
#                 # FOREIGN KEY constraint
#                 if 'FOREIGN KEY' in part_upper and 'REFERENCES' in part_upper:
#                     # parse local cols and referenced cols
#                     local_cols = re.search(r'FOREIGN KEY\s*\(([^)]+)\)', part, re.IGNORECASE)
#                     ref = re.search(r'REFERENCES\s+([^\s(]+)\s*\(([^)]+)\)', part, re.IGNORECASE)
#                     if local_cols and ref:
#                         local_list = [c.strip('`" ') for c in local_cols.group(1).split(',')]
#                         ref_table = ref.group(1)
#                         ref_list = [c.strip('`" ') for c in ref.group(2).split(',')]
#                         for lc, rc in zip(local_list, ref_list):
#                             if lc in catalog['column_metadata'][table]:
#                                 catalog['column_metadata'][table][lc]['foreign_key'] = True
#                                 catalog['column_metadata'][table][lc]['references'] = {'table': ref_table, 'column': rc}

#         # Convert sets to lists for JSON serializability
#         catalog['schemas'] = list(catalog['schemas'])
#         catalog['tables'] = list(catalog['tables'])
#         catalog['columns'] = list(catalog['columns'])
#         catalog['functions'] = list(catalog['functions'])
#         catalog['table_columns'] = {tbl: list(cols) for tbl, cols in catalog['table_columns'].items()}
#         return catalog
