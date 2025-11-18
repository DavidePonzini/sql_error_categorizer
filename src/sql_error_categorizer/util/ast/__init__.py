'''Utility functions for processing SQL ASTs made with sqlglot.'''

from .column import *
from .function import *
from .subquery import *
from .table import *

import sqlglot.optimizer.normalize
from sqlglot import exp
from copy import deepcopy

def extract_DNF(expr) -> list[exp.Expression]:
    '''Given a boolean expression, extract its Disjunctive Normal Form (DNF)'''
    expr = deepcopy(expr)       # Avoid modifying the original expression

    dnf_expr = sqlglot.optimizer.normalize.normalize(expr, dnf=True)

    if not isinstance(dnf_expr, exp.Or):
        return [dnf_expr]
    
    disjuncts = dnf_expr.flatten()  # list Di (A1 OR A2 OR ... OR Dn)
    return list(disjuncts)

def extract_CNF(expr) -> list[exp.Expression]:
    '''Given a boolean expression, extract its Conjunctive Normal Form (CNF)'''
    expr = deepcopy(expr)       # Avoid modifying the original expression

    cnf_expr = sqlglot.optimizer.normalize.normalize(expr, dnf=False)

    if not isinstance(cnf_expr, exp.And):
        return [cnf_expr]
    
    conjuncts = cnf_expr.flatten()  # list Ci (A1 AND A2 AND ... AND Cn)
    return list(conjuncts)
