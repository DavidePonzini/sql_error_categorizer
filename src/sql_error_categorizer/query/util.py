import sqlparse
from sqlparse.tokens import Whitespace, Newline
from sqlglot.optimizer.normalize import normalize
from sqlglot import exp

def strip_ws(tokens: list[sqlparse.sql.Token]) -> list[sqlparse.sql.Token]:
    return [t for t in tokens if t.ttype not in (Whitespace, Newline)]

def remove_parentheses(sql: str) -> str:
    sql = sql.strip()
    while sql.startswith('(') and sql.endswith(')'):
        sql = sql[1:-1].strip()
    return sql

def extract_DNF(expr) -> list[exp.Expression]:
    dnf_expr = normalize(expr, dnf=True)

    if not isinstance(dnf_expr, exp.Or):
        return [dnf_expr]
    
    disjuncts = dnf_expr.flatten()  # list of Ci (each an And)
    return list(disjuncts)