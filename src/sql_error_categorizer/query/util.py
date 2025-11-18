'''Utility functions for SQL query processing.'''

import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Whitespace, Newline
from sqlglot.optimizer.normalize import normalize
from sqlglot import exp
from copy import deepcopy

# region SQL string
def remove_parentheses(sql: str) -> str:
    '''Remove outer parentheses from a SQL string.'''
    sql = sql.strip()

    # check if the entire string is wrapped in parentheses or if there are inner parentheses
    # i.e., (SELECT 1) UNION (SELECT 2) should not have parentheses removed
    depth = 0
    for idx, char in enumerate(sql):
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
        if depth == 0 and idx < len(sql) - 1:
            return sql  # parentheses are not outermost

    while sql.startswith('(') and sql.endswith(')'):
        sql = sql[1:-1].strip()
    return sql
# endregion

# region tokens
def tokens_to_sql(tokens: list[sqlparse.sql.Token]) -> str:
    '''Convert a list of sqlparse tokens back to a SQL string.'''
    return TokenList(tokens).value.strip()

def is_ws(token: sqlparse.sql.Token) -> bool:
    '''Check if a token is whitespace or newline.'''
    return token.ttype in (Whitespace, Newline)

def strip_ws(tokens: list[sqlparse.sql.Token]) -> list[sqlparse.sql.Token]:
    '''Remove whitespace and newline tokens from a list of tokens.'''
    return [t for t in tokens if not is_ws(t)]
# endregion

# region AST
def extract_DNF(expr) -> list[exp.Expression]:
    '''Given a boolean expression, extract its Disjunctive Normal Form (DNF)'''
    expr = deepcopy(expr)       # Avoid modifying the original expression

    dnf_expr = normalize(expr, dnf=True)

    if not isinstance(dnf_expr, exp.Or):
        return [dnf_expr]
    
    disjuncts = dnf_expr.flatten()  # list Di (A1 OR A2 OR ... OR Dn)
    return list(disjuncts)

def extract_CNF(expr) -> list[exp.Expression]:
    '''Given a boolean expression, extract its Conjunctive Normal Form (CNF)'''
    expr = deepcopy(expr)       # Avoid modifying the original expression

    cnf_expr = normalize(expr, dnf=False)

    if not isinstance(cnf_expr, exp.And):
        return [cnf_expr]
    
    conjuncts = cnf_expr.flatten()  # list Ci (A1 AND A2 AND ... AND Cn)
    return list(conjuncts)

def extract_function_name(func_expr: exp.Func) -> str:
    '''Extract the function name from a function expression.'''
    if isinstance(func_expr, exp.Anonymous):
        return func_expr.name.upper()
    return func_expr.__class__.__name__.lower()
# endregion