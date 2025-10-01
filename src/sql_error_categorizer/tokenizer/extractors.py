'''Extracts components from list of tokens using `sqlparse`.'''

import sqlparse
from sqlparse.tokens import Whitespace, Newline
from sqlparse.sql import IdentifierList, Identifier, Function

def extract_identifiers(tokens, current_clause: str = 'NONE') -> list[tuple[sqlparse.sql.Identifier, str]]:
    result = []

    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword or token.ttype is sqlparse.tokens.DML or token.ttype is sqlparse.tokens.CTE:
            if token.value.upper() in ('WITH', 'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT'):
                current_clause = token.value.upper()
            continue

        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                result.append((identifier, current_clause))
        elif isinstance(token, Identifier):
            result.append((token, current_clause))
        elif token.is_group:
            sub_identifiers = extract_identifiers(token.tokens, current_clause)
            result.extend(sub_identifiers)
    return result

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
