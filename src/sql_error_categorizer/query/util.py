'''Utility functions for SQL query processing.'''

import sqlparse
from sqlparse.tokens import Whitespace, Newline
from sqlglot.optimizer.normalize import normalize
from sqlglot import exp
from copy import deepcopy

# region SQL string
def remove_parentheses(sql: str) -> str:
    '''Remove outer parentheses from a SQL string.'''
    sql = sql.strip()
    while sql.startswith('(') and sql.endswith(')'):
        sql = sql[1:-1].strip()
    return sql
# endregion

# region tokens
def strip_ws(tokens: list[sqlparse.sql.Token]) -> list[sqlparse.sql.Token]:
    '''Remove whitespace and newline tokens from a list of tokens.'''
    return [t for t in tokens if t.ttype not in (Whitespace, Newline)]
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