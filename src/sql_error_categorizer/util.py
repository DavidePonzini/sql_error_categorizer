from sqlglot import exp

def normalize_identifier_name(identifier: str) -> str:
    '''Normalize an SQL identifier by stripping quotes and converting to lowercase if unquoted.'''
    if identifier.startswith('"') and identifier.endswith('"') and len(identifier) > 1:
        return identifier[1:-1]
    
    return identifier.lower()

def normalize_table_real_name(table: exp.Table) -> str:
    '''Returns the table real name, in lowercase if unquoted.'''

    quoted = table.this.quoted
    name = table.this.name

    return name if quoted else name.lower()


def normalize_table_name(table: exp.Table) -> str:
    '''Returns the table name or alias, in lowercase if unquoted.'''
    
    if table.args.get('alias'):
        quoted = table.args['alias'].args.get('quoted', False)
        name = table.alias_or_name

        return name if quoted else name.lower()

    return normalize_table_real_name(table)



def normalize_schema_name(table: exp.Table) -> str | None:
    '''Returns the schema name, in lowercase if unquoted.'''
    
    if table.args.get('db'):
        quoted = table.args['db'].quoted
        name = table.db

        return name if quoted else name.lower()
    
    return None

def normalize_subquery_name(subquery: exp.Subquery) -> str:
    '''Returns the subquery name or alias, in lowercase if unquoted.'''
    
    if subquery.args.get('alias'):
        quoted = subquery.args['alias'].this.quoted
        name = subquery.alias_or_name

        return name if quoted else name.lower()

    return subquery.alias_or_name