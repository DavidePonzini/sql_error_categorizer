'''Extracts components from list of tokens using `sqlparse`.'''

from sqlglot import expressions as E

import sqlparse
from sqlparse.sql import Function

import copy

def extract_functions(tokens, current_clause: str = 'NONE') -> list[tuple[sqlparse.sql.Function, str]]:
    result: list[tuple[sqlparse.sql.Function, str]] = []

    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword or token.ttype is sqlparse.tokens.DML or token.ttype is sqlparse.tokens.CTE:
            if token.value.upper() in ('WITH', 'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT'):
                current_clause = token.value.upper()
            continue


        if isinstance(token, Function):
            # Include this function
            result.append((token, current_clause))
            # Also search inside for nested function calls
            result.extend(extract_functions(token.tokens, current_clause))
        elif token.is_group:
            result.extend(extract_functions(token.tokens, current_clause))
    return result

def extract_comparisons(tokens, current_clause: str = 'NONE') -> list[tuple[sqlparse.sql.Comparison, str]]:
    result: list[tuple[sqlparse.sql.Comparison, str]] = []
    
    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword or token.ttype is sqlparse.tokens.DML or token.ttype is sqlparse.tokens.CTE:
            if token.value.upper() in ('WITH', 'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT'):
                current_clause = token.value.upper()
            continue

        if isinstance(token, sqlparse.sql.Comparison):
            result.append((token, current_clause))
        elif token.is_group:
            result.extend(extract_comparisons(token.tokens, current_clause))
    return result

def extract_ctes(ast: E.Expression | None) -> list[tuple[str, str]]:
    '''Extracts CTEs from the SQL query and returns them as a list of (cte_name, sql_string) tuples.'''

    if ast is None:
        return []
    
    ctes = ast.args.get('with')
    if ctes is None:
        return []

    result = []
    for cte in ctes.expressions:
        cte_name = cte.alias_or_name
        cte_sql = cte.this.sql()
        result.append((cte_name, cte_sql))

    return result

def remove_ctes(ast: E.Expression | None) -> str:
    '''Removes CTEs from the SQL query and returns the main query as a string.'''

    if ast is None:
        return ''

    ast_copy = copy.deepcopy(ast)

    ast_copy.set('with', None)
    return ast_copy.sql()

def extract_subqueries(ast: E.Expression | None) -> list[E.Subquery]:
    '''Extracts subqueries from the SQL query and returns them as a list of sqlglot Expression objects.'''

    if ast is None:
        return []

    return list(ast.find_all(E.Subquery))