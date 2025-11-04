'''Convert SQL expressions to Z3 expressions for logical reasoning.'''

from typing import Any, Callable
from sqlglot import exp
from z3 import (
    Int, IntVal,
    Real, RealVal,
    Bool, BoolVal,
    String, StringVal,
    And, Or, Not,
    Solver,
    unsat,
    is_expr,
    BoolSort,
    ExprRef
)

from ..catalog import Table

def create_z3_var(variables: dict[str, Any], table_name: str | None, col_name: str, col_type: Callable[[str], ExprRef] | None = None) -> None:
    '''
    Create a Z3 variable for the given column name and type, and add it to the variables dictionary.
    If col_type is None, default to Int.
    '''
    
    if col_type is None:
        col_type = Int  # default type

    # Add both unqualified and qualified names and null flags
    variables[col_name] = col_type(col_name)
    variables[f'{col_name}_isnull'] = Bool(f'{col_name}_isnull')

    if table_name:
        variables[f'{table_name}.{col_name}'] = col_type(f'{table_name}.{col_name}')
        variables[f'{table_name}.{col_name}_isnull'] = Bool(f'{table_name}.{col_name}_isnull')

def catalog_table_to_z3_vars(table: Table) -> dict[str, ExprRef]:
    '''Convert catalog table columns to Z3 variables.'''

    variables = {}
    for column in table.columns:
        col_name = column.name
        col_type = column.column_type.upper()

        if col_type in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT'):
            create_z3_var(variables, table.name, col_name, Int)
        elif col_type in ('FLOAT', 'REAL', 'DOUBLE'):
            create_z3_var(variables, table.name, col_name, Real)
        elif col_type in ('BOOLEAN', 'BOOL'):
            create_z3_var(variables, table.name, col_name, Bool)
        elif col_type in ('VARCHAR', 'CHAR', 'TEXT', 'CHARACTER VARYING'):
            create_z3_var(variables, table.name, col_name, String)
        else:
            create_z3_var(variables, table.name, col_name)

    return variables

def sql_to_z3(expr, variables: dict[str, ExprRef] = {}) -> Any:
    '''Convert a SQLGlot expression to a Z3 expression.'''
    # --- Columns ---
    if isinstance(expr, exp.Column):
        name = expr.name.lower()
        if name not in variables:
            create_z3_var(variables, None, name)
        return variables[name]

    # --- Literals ---
    elif isinstance(expr, exp.Literal):
        val = expr.this
        if expr.is_int:
            return IntVal(int(val))
        elif expr.is_number:
            return RealVal(float(val))
        elif expr.is_string:
            return StringVal(val.strip("'"))
        elif val.upper() in ('TRUE', 'FALSE'):
            return BoolVal(val.upper() == 'TRUE')
        elif val.upper() == 'NULL':
            # Represent NULL as a special None (handled by IS NULL)
            return None
        else:
            raise NotImplementedError(f"Unsupported literal: {val}")

    elif isinstance(expr, exp.Null):
        return None

    # --- Boolean comparisons ---
    elif isinstance(expr, exp.EQ):
        return sql_to_z3(expr.left, variables) == sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.NEQ):
        return sql_to_z3(expr.left, variables) != sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.GT):
        return sql_to_z3(expr.left, variables) > sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.GTE):
        return sql_to_z3(expr.left, variables) >= sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.LT):
        return sql_to_z3(expr.left, variables) < sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.LTE):
        return sql_to_z3(expr.left, variables) <= sql_to_z3(expr.right, variables)

    # --- Logical connectives ---
    elif isinstance(expr, exp.And):
        return And(sql_to_z3(expr.left, variables), sql_to_z3(expr.right, variables))
    elif isinstance(expr, exp.Or):
        return Or(sql_to_z3(expr.left, variables), sql_to_z3(expr.right, variables))
    elif isinstance(expr, exp.Not):
        return Not(sql_to_z3(expr.this, variables))
    elif isinstance(expr, exp.Paren):
        return sql_to_z3(expr.this, variables)

    # --- Arithmetic ---
    elif isinstance(expr, exp.Add):
        return sql_to_z3(expr.left, variables) + sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Sub):
        return sql_to_z3(expr.left, variables) - sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Mul):
        return sql_to_z3(expr.left, variables) * sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Div):
        return sql_to_z3(expr.left, variables) / sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Mod):
        return sql_to_z3(expr.left, variables) % sql_to_z3(expr.right, variables)
    elif isinstance(expr, exp.Pow):
        return sql_to_z3(expr.left, variables) ** sql_to_z3(expr.right, variables)

    # --- BETWEEN a AND b ---String
    elif isinstance(expr, exp.Between):
        target = sql_to_z3(expr.this, variables)
        low = sql_to_z3(expr.args['low'], variables)
        high = sql_to_z3(expr.args['high'], variables)
        return And(target >= low, target <= high)

    # --- IN (list) ---
    elif isinstance(expr, exp.In):
        target = sql_to_z3(expr.this, variables)
        if isinstance(expr.args['expressions'], exp.Subquery):
            # Subquery handling can be complex; skipping for now
            return BoolVal(True)
        
        options = [sql_to_z3(e, variables) for e in expr.expressions]
        return Or(*[target == o for o in options])

    # --- IS / IS NOT ---
    elif isinstance(expr, exp.Is):
        target_expr = expr.this
        right_expr = expr.args.get('expression')

        # handle IS NULL and IS NOT NULL
        if isinstance(right_expr, exp.Null):
            # x IS NULL → x_isnull = True
            if isinstance(target_expr, exp.Column):
                name = target_expr.name.lower()
                flag = variables.setdefault(f'{name}_isnull', Bool(f'{name}_isnull'))
                return flag
            else:
                return BoolVal(False)

        elif isinstance(right_expr, exp.Not) and isinstance(right_expr.this, exp.Null):
            # x IS NOT NULL → ¬x_isnull
            if isinstance(target_expr, exp.Column):
                name = target_expr.name.lower()
                flag = variables.setdefault(f'{name}_isnull', Bool(f'{name}_isnull'))
                return Not(flag)
            else:
                return BoolVal(True)

        else:
            # generic IS (e.g., IS TRUE, IS FALSE)
            return sql_to_z3(target_expr, variables) == sql_to_z3(right_expr, variables)

    # Fallback: skip unsupported expressions
    return BoolVal(True)

def check_formula(expr) -> str:
    '''Check if the given SQLGlot expression is a tautology, contradiction, or contingent.'''
    formula = sql_to_z3(expr, {})
    if formula is None:
        return 'unknown'

    solver = Solver()

    # Check for contradiction
    solver.push()
    solver.add(formula)
    if solver.check() == unsat:
        solver.pop()
        return 'contradiction'
    solver.pop()

    # Check for tautology
    solver.push()
    solver.add(Not(formula))
    if solver.check() == unsat:
        solver.pop()
        return 'tautology'
    solver.pop()

    return 'contingent'

def is_satisfiable(expr_z3) -> bool:
    '''Check if the given Z3 expression is satisfiable.'''
    solver = Solver()
    solver.add(expr_z3)

    return solver.check() != unsat

def is_bool_expr(e) -> bool:
    '''Check if the given Z3 expression is boolean.'''
    return is_expr(e) and e.sort().kind() == BoolSort().kind()
