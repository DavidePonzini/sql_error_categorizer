from sqlglot import exp
from z3 import (
    Int, IntVal, RealVal, BoolVal, Bool, StringVal,
    And, Or, Not, Solver, unsat
)

def sql_to_z3(expr, variables):
    # --- Columns ---
    if isinstance(expr, exp.Column):
        name = expr.name.lower()
        if name not in variables:
            variables[name] = Int(name)  # actual value
            variables[f'{name}_isnull'] = Bool(f'{name}_isnull')  # null flag
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

    # --- BETWEEN a AND b ---
    elif isinstance(expr, exp.Between):
        target = sql_to_z3(expr.this, variables)
        low = sql_to_z3(expr.args['low'], variables)
        high = sql_to_z3(expr.args['high'], variables)
        return And(target >= low, target <= high)

    # --- IN (list) ---
    elif isinstance(expr, exp.In):
        target = sql_to_z3(expr.this, variables)
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

def check_formula(expr):
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


from z3 import Solver, Not, unsat, Or, And, BoolSort, is_expr
import sqlglot
from sqlglot.optimizer.normalize import normalize

def consistency_check(expr_z3):
    solver = Solver()
    solver.add(expr_z3)
    messages.debug(expr_z3)
    return solver.check()

def extract_DNF(expr):
    dnf_expr = normalize(expr, dnf=True)
    disjuncts = dnf_expr.flatten()  # list of Ci (each an And)
    return disjuncts


def is_bool_expr(e):
    return is_expr(e) and e.sort().kind() == BoolSort().kind()

def analyze_error8(expr: exp.Expression):
    variables = {}
    dnf = list(extract_DNF(expr))

    results = {'tautology': False, 'contradiction': False, 'redundant_disjuncts': [], 'redundant_conjuncts': []}

    # (1) whole formula
    whole = Or(*[sql_to_z3(C, variables) for C in dnf])

    if consistency_check(whole) == unsat:
        results['contradiction'] = True
    if consistency_check(Not(whole)) == unsat:
        results['tautology'] = True

    # (2) each Ci redundant?
    for i, Ci in enumerate(dnf):
        messages.debug(f'Analyzing disjunct {i}: {Ci}')        
        Ci_z3 = sql_to_z3(Ci, variables)
        others = Or(*[sql_to_z3(C, variables) for j, C in enumerate(dnf) if j != i])
        if consistency_check(And(Ci_z3, Not(others))) == unsat:
            results['redundant_disjuncts'].append((Ci.sql()))
        
        # (3) each Ai,j redundant?
        conjuncts = list(Ci.flatten())
        for j, Aj in enumerate(conjuncts):
            Aj_z3 = sql_to_z3(Aj, variables)
            if not is_bool_expr(Aj_z3):
                continue
            rest = [sql_to_z3(c, variables) for k, c in enumerate(conjuncts)
                    if k != j and is_bool_expr(sql_to_z3(c, variables))]
            others = Or(*[sql_to_z3(C, variables) for k, C in enumerate(dnf) if k != i])
            formula = And(Not(Aj_z3), *rest, Not(others))
            if consistency_check(formula) == unsat:
                results['redundant_conjuncts'].append((Ci.sql(), Aj.sql()))

    return results
