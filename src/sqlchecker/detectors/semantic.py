'''Detector for semantic errors in SQL queries.'''

import re
from typing import Callable
from sqlglot import exp
from z3 import Not, Or, And
from sqlerrors import SqlErrors
from sqlscope import util
from sqlscope.query import Query, smt

from .base import BaseDetector, DetectedError

class SemanticErrorDetector(BaseDetector):
    '''Detector for semantic errors in SQL queries.'''
    
    def __init__(self,
                 *,
                 query: Query,
                 update_query: Callable[[str, str | None], None],
                 solutions: list[Query] = [],
                ):
        super().__init__(
            query=query,
            solutions=solutions,
            update_query=update_query,
        )

    def run(self) -> list[DetectedError]:
        results: list[DetectedError] = super().run()

        checks = [
            self.detect_40_tautological_or_inconsistent_expression,     # ok
            self.detect_41_distinct_in_sum_or_avg,                      # ok
            self.detect_42_distinct_removing_important_duplicates,      # TODO: implement
            self.detect_45_mixing_comparison_and_null,                  # TODO: refactor/implement
            self.detect_46_null_in_InAnyAll_subquery,                   # TODO: implement
            self.detect_47_join_condition_on_unmatchable_column,        # TODO: implement
            self.detect_49_many_duplicates,                             # TODO: implement
            self.detect_50_constant_column_output,                      # TODO: revise and implement
            self.detect_51_duplicate_column_output,                     # ok
        ]
        
        for chk in checks:
            results.extend(chk())

        return results

    def detect_40_tautological_or_inconsistent_expression(self) -> list[DetectedError]:
        results: list[DetectedError] = []

        for select in self.query.selects:
            where = select.where

            if not where:
                continue

            # Build Z3 variables from catalog
            variables = {}
            for table in select.referenced_tables:
                variables.update(smt.catalog_table_to_z3_vars(table))

            dnf = util.ast.extract_DNF(where)


            # Refer to Brass & Goldberg, 2006 for these checks (error #8)
            # (1) whole formula
            try:
                whole_clauses = [smt.sql_to_z3(C, variables) for C in dnf]
                whole = Or(*whole_clauses)
            except Exception:
                continue  # skip if cannot convert to z3

            if not smt.is_satisfiable(whole):
                results.append(DetectedError(SqlErrors.IMPLIED_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('contradiction',)))
            elif not smt.is_satisfiable(Not(whole)):
                results.append(DetectedError(SqlErrors.IMPLIED_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('tautology',)))
                
            # (2) each Ci redundant?
            for i, Ci in enumerate(dnf):
                    Ci_z3 = smt.sql_to_z3(Ci, variables)
                    others = Or(*[smt.sql_to_z3(C, variables) for j, C in enumerate(dnf) if j != i])
                    if not smt.is_satisfiable(And(Ci_z3, Not(others))):
                        results.append(DetectedError(SqlErrors.IMPLIED_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('redundant_disjunct', Ci.sql())))
                    
                    # (3) each Ai,j redundant?
                    conjuncts = list(Ci.flatten())
                    for j, Aj in enumerate(conjuncts):
                        Aj_z3 = smt.sql_to_z3(Aj, variables)
                        if not smt.is_bool_expr(Aj_z3):
                            continue
                        rest = [smt.sql_to_z3(c, variables) for k, c in enumerate(conjuncts)
                                if k != j and smt.is_bool_expr(smt.sql_to_z3(c, variables))]
                        others = Or(*[smt.sql_to_z3(C, variables) for k, C in enumerate(dnf) if k != i])
                        formula = And(Not(Aj_z3), *rest, Not(others))
                        if not smt.is_satisfiable(formula):
                            results.append(DetectedError(SqlErrors.IMPLIED_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION, ('redundant_conjunct', (Ci.sql(), Aj.sql()))))

        return results

    def detect_41_distinct_in_sum_or_avg(self) -> list[DetectedError]:
        '''
            Detect SUM(DISTINCT ...) or AVG(DISTINCT ...)

            If the correct query uses SUM(DISTINCT ...) or AVG(DISTINCT ...), then
            the user query is unlikely to be incorrect, so we do not flag it.
        '''

        results: list[DetectedError] = []

        # Flags for skipping detection if correct query uses DISTINCT in SUM/AVG
        allow_sum_distinct = False
        allow_avg_distinct = False
        
        # First check the correct solutions
        for solution in self.solutions:
            for select in solution.selects:
                ast = select.ast

                if not ast:
                    continue

                for func in ast.find_all(exp.Sum):
                    if func.this and isinstance(func.this, exp.Distinct):
                        allow_sum_distinct = True

                for func in ast.find_all(exp.Avg):
                    if func.this and isinstance(func.this, exp.Distinct):
                        allow_avg_distinct = True

        # Then check the user query
        for select in self.query.selects:
            ast = select.ast

            if not ast:
                continue

            if not allow_sum_distinct:
                # Solution does not use SUM(DISTINCT ...), so check user query
                for func in ast.find_all(exp.Sum):
                    if func.this and isinstance(func.this, exp.Distinct):
                        results.append(DetectedError(SqlErrors.DISTINCT_IN_SUM_OR_AVG, (func.sql(),)))

            if not allow_avg_distinct:
                # Solution does not use AVG(DISTINCT ...), so check user query
                for func in ast.find_all(exp.Avg):
                    if func.this and isinstance(func.this, exp.Distinct):
                        results.append(DetectedError(SqlErrors.DISTINCT_IN_SUM_OR_AVG, (func.sql(),)))

        return results
    
    def detect_42_distinct_removing_important_duplicates(self) -> list[DetectedError]:
        return []

    def detect_45_mixing_comparison_and_null(self) -> list[DetectedError]: 
        '''Detect mixing of >0 with IS NOT NULL or empty string with IS NULL on the same column'''
        return []

        results = []
        # a > 0 AND a IS NOT NULL
        m = re.search(r"(\w+)\s*>\s*0\s+AND\s+\1\s+IS\s+NOT\s+NULL", self.query, re.IGNORECASE)
        if m:
            results.append((
                SqlErrors.SEM_45_MIXING_A_GREATER_THAN_0_WITH_IS_NOT_NULL,
                m.group(0)
            ))

        # a = '' AND a IS NULL
        m2 = re.search(r"(\w+)\s*=\s*''\s+AND\s+\1\s+IS\s+NULL", self.query, re.IGNORECASE)
        if m2:
            results.append((
                SqlErrors.SEM_45_MIXING_A_GREATER_THAN_0_WITH_IS_NOT_NULL,
                m2.group(0)
            ))

        return results    
    
    def detect_46_null_in_InAnyAll_subquery(self) -> list[DetectedError]:
        '''Detect potential NULL/UNKNOWN in IN/ANY/ALL subqueries when subquery column is nullable.
            heuristically assume that if a column is not declared as NOT NULL, then every typical 
            database state contains at least one row in which it is null. '''
        return []

    def detect_47_join_condition_on_unmatchable_column(self) -> list[DetectedError]:
        '''
        For each JOIN … ON: require at least one “A.col = B.col” in the ON clause.
        For comma-style joins (FROM A, B): require at least one “A.col = B.col” in the WHERE.
        If no such predicate is found for a given join, emit SEM_2_JOIN_ON_INCORRECT_COLUMN.
        If the join operation is a self-join, then skip the check.
        Check based on the content of the catalog column_metadata the compatibility of the columns.
        '''
        return []
    
    def detect_49_many_duplicates(self) -> list[DetectedError]:
        return []
    
    def detect_50_constant_column_output(self) -> list[DetectedError]:
        '''
            Detect if the output of the query contains a constant value.
            Exclude constants that are likely intentional, such as SELECT 1, SELECT 'constant', etc.
            Also exclude aggregation functions that return constants, such as COUNT(*), SUM(*), etc.
        '''

        return []
    
        # NOTE: the following implementatation is incorrect, since it selects only intentional constants

        results: list[DetectedError] = []

        output = self.query.main_query.output

        for col in output.columns:
            if col.is_constant:
                results.append(DetectedError(SqlErrors.CONSTANT_COLUMN_OUTPUT, (col.name,)))

        return results

    def detect_51_duplicate_column_output(self) -> list[DetectedError]:
        '''
        Detects if the same column or expression appears multiple times in the SELECT list.
        Also include columns that are equated.
        '''

        results: list[DetectedError] = []

        projected_columns: set[str] = set()

        # list of equivalence classes of columns that are equated in the WHERE clause (e.g. A.id = B.id means A.id and B.id are equivalent for the purpose of duplicate detection)
        column_equivalences: list[set[str]] = []

        for select in self.query.selects:
            if select.where:
                equalities = util.ast.extract_column_equalities(select.where)

                for left, right in equalities:
                    left_name = util.ast.column.get_real_name(left)
                    left_idx = select.get_table_idx_for_column(left)

                    right_name = util.ast.column.get_real_name(right)
                    right_idx = select.get_table_idx_for_column(right)

                    if left_idx is not None and right_idx is not None:
                        left_full = f'{left_idx}.{left_name}'
                        right_full = f'{right_idx}.{right_name}'

                        # Check if left and right are already in an equivalence class
                        left_class = None
                        right_class = None

                        for eq_class in column_equivalences:
                            if left_full in eq_class:
                                left_class = eq_class
                            if right_full in eq_class:
                                right_class = eq_class

                        if left_class and right_class and left_class != right_class:
                            # Merge the two classes
                            left_class.update(right_class)
                            column_equivalences.remove(right_class)
                        elif left_class and not right_class:
                            left_class.add(right_full)
                        elif right_class and not left_class:
                            right_class.add(left_full)
                        else:
                            column_equivalences.append(set([left_full, right_full]))

            for column in select.output.columns:
                table_idx = column.table_idx

                if table_idx is None:
                    # TODO: handle expressions and constants in the SELECT list
                    continue  # skip if no table reference (e.g. constant or computed column)

                name = f'{column.table_idx}.{column.real_name}'

                equivalent_names = set()
                for eq_class in column_equivalences:
                    if name in eq_class:
                        equivalent_names.update(eq_class)

                if name in projected_columns:
                    results.append(DetectedError(SqlErrors.DUPLICATE_COLUMN_OUTPUT, (select.referenced_tables[table_idx].name, column.real_name)))

                projected_columns.add(name)
                projected_columns.update(equivalent_names)

        return results



