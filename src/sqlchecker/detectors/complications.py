'''Detector for complications in SQL queries.'''

from typing import Callable
from sqlglot import exp
from sqlerrors import SqlErrors
from sqlscope.catalog import ConstraintType, ConstraintColumn
from sqlscope import Query
from sqlscope import util

from .base import BaseDetector, DetectedError

class ComplicationDetector(BaseDetector):
    '''Detector for complications in SQL queries.'''

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
        '''
        Executes all complication checks and returns a list of identified misconceptions.
        '''

        results: list[DetectedError] = super().run()

        checks = [
            self.detect_82_unnecessary_complication,                                # ok
            self.detect_83_unnecessary_distinct_in_select_clause,                   # ok
            self.detect_84_unnecessary_table_reference,                             # TODO: refactor/implement
            self.detect_85_unused_correlation_name,                                 # TODO: implement
            self.detect_86_tables_have_same_data,                                   # TODO: implement
            self.detect_125_correlation_name_identical_to_table_name,               # TODO: implement
            self.detect_87_unnecessary_general_comparison_operator,                 # TODO: implement
            self.detect_88_like_without_wildcards,                                  # ok
            self.detect_89_unnecessarily_complicated_select_in_exists_subquery,     # TODO: implement
            self.detect_90_in_exists_can_be_replaced_by_comparison,                 # TODO: implement
            self.detect_91_unnecessary_aggregate_function,                          # TODO: implement
            self.detect_92_unnecessary_distinct_in_aggregate_function,              # ok
            self.detect_93_unnecessary_argument_of_count,                           # ok
            self.detect_94_unnecessary_group_by_in_exists_subquery,                 # TODO: implement
            self.detect_95_group_by_with_singleton_groups,                          # ok
            self.detect_96_group_by_with_only_a_single_group,                       # TODO: implement
            self.detect_97_group_by_can_be_replaced_by_distinct,                    # ok
            self.detect_98_union_can_be_replaced_by_or,                             # TODO: implement
            self.detect_99_unnecessary_column_in_order_by_clause,                   # TODO: refactor/implement
            self.detect_100_order_by_in_subquery,                                   # TODO: implement
            self.detect_101_inefficient_having,                                     # TODO: implement
            self.detect_102_inefficient_union,                                      # TODO: implement
            self.detect_103_condition_in_the_subquery_can_be_moved_up,              # TODO: implement
            self.detect_104_outer_join_can_be_replaced_by_inner_join,               # TODO: implement
            self.detect_126_unused_cte,                                             #
        ]
        
        for chk in checks:
            results.extend(chk())

        return results

    def detect_82_unnecessary_complication(self) -> list[DetectedError]:
        '''NOTE: this is an umbrella term, so it can't be directly detected.'''
        return []

    def detect_83_unnecessary_distinct_in_select_clause(self) -> list[DetectedError]:
        '''
        Flags a SELECT DISTINCT clause that is unnecessary because the selected
        columns are already unique due to existing constraints.
        '''
        result: list[DetectedError] = []

        for select in self.query.selects:
            if not select.distinct:
                continue

            # Remove DISTINCT constraint
            constraints = [c for c in select.output.unique_constraints if c.constraint_type != ConstraintType.DISTINCT]

            if len(constraints) > 0:
                result.append(DetectedError(SqlErrors.UNNECESSARY_DISTINCT_IN_SELECT_CLAUSE, (select.sql,)))

        return result

    def detect_84_unnecessary_table_reference(self) -> list[DetectedError]:
        '''
        Flags a query that joins to a table not present in the correct solution.
        '''
        return []

        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_tables = self._get_from_tables(self.q_ast)
        s_tables = self._get_from_tables(self.s_ast)

        q_tables_set = {t.lower() for t in q_tables}
        s_tables_set = {t.lower() for t in s_tables}

        extraneous_tables = q_tables_set - s_tables_set

        if extraneous_tables:
            original_q_tables = self._get_from_tables(self.q_ast, with_alias=True)
            for table_name_lower in extraneous_tables:
                # Find the original table name (with alias if it was used) to report back
                original_table_name = next((t for t in original_q_tables if t.lower().startswith(table_name_lower)), table_name_lower)
                results.append((
                    SqlErrors.UNNECESSARY_TABLE_REFERENCE,
                    f"Unnecessary JOIN: The table '{original_table_name}' is not needed to answer the query."
                ))
            
        return results
    
    def detect_85_unused_correlation_name(self) -> list[DetectedError]:
        return []
    
    def detect_86_tables_have_same_data(self) -> list[DetectedError]:
        return []

    def detect_125_correlation_name_identical_to_table_name(self) -> list[DetectedError]:
        return []
    
    def detect_87_unnecessary_general_comparison_operator(self) -> list[DetectedError]:
        return []
    
    def detect_88_like_without_wildcards(self) -> list[DetectedError]:
        '''
        Flags queries where the LIKE operator is used without wildcards ('%' or '_').
        This indicates a potential misunderstanding, where the '=' operator should
        have been used instead.
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            ast = select.ast

            if not ast:
                continue

            for like in ast.find_all(exp.Like):
                pattern_expr = like.args.get('expression')
                
                if not pattern_expr:
                    # Malformed LIKE expression
                    continue
                
                if not isinstance(pattern_expr, exp.Literal):
                    # Some other expression type, e.g., a column reference
                    continue

                pattern_value = pattern_expr.this
                if '%' not in pattern_value and '_' not in pattern_value:
                    full_expression = str(like)

                    results.append(DetectedError(SqlErrors.LIKE_WITHOUT_WILDCARDS, (full_expression,)))

        return results
    
    def detect_89_unnecessarily_complicated_select_in_exists_subquery(self) -> list[DetectedError]:
        return []
    
    def detect_90_in_exists_can_be_replaced_by_comparison(self) -> list[DetectedError]:
        return []
    
    def detect_91_unnecessary_aggregate_function(self) -> list[DetectedError]:
        return []
    
    def detect_92_unnecessary_distinct_in_aggregate_function(self) -> list[DetectedError]:
        '''MIN and MAX never require DISTINCT. For other aggregate functions, DISTINCT is unnecessary if the argument is unique.'''

        results: list[DetectedError] = []

        for select in self.query.selects:
            select = select.strip_subqueries()

            if not select.ast:
                continue

            for agg_func in select.ast.find_all(exp.AggFunc):
                if not isinstance(agg_func.this, exp.Distinct):
                    continue

                if isinstance(agg_func, (exp.Min, exp.Max)):
                    results.append(DetectedError(SqlErrors.UNNECESSARY_DISTINCT_IN_AGGREGATE_FUNCTION, (str(agg_func),)))
                    continue

                arg_expr = agg_func.this.expressions   # `.this` is the DISTINCT, `.expressions` are the arguments
                if not arg_expr:
                    continue

                for expr in arg_expr:
                    # Check if the argument is a constant literal
                    if isinstance(expr, exp.Literal):
                        results.append(DetectedError(SqlErrors.UNNECESSARY_DISTINCT_IN_AGGREGATE_FUNCTION, (str(agg_func),)))
                        continue

                    # Check if the argument is a column
                    if isinstance(expr, exp.Column):
                        column_name = util.ast.column.get_real_name(expr)

                        # Check if the column has a UNIQUE constraint
                        unique_constraints = [c for c in select.all_constraints if c.constraint_type == ConstraintType.UNIQUE]
                        for constraint in unique_constraints:
                            if { ConstraintColumn(column_name, table_idx=select.get_table_idx_for_column(expr)) } == constraint.columns:
                                results.append(DetectedError(SqlErrors.UNNECESSARY_DISTINCT_IN_AGGREGATE_FUNCTION, (str(agg_func),)))
                                break
        return results
    
    def detect_93_unnecessary_argument_of_count(self) -> list[DetectedError]:
        return []
    
    def detect_94_unnecessary_group_by_in_exists_subquery(self) -> list[DetectedError]:
        return []
    
    def detect_95_group_by_with_singleton_groups(self) -> list[DetectedError]:
        '''
        Flags GROUP BY clauses on singleton groups due to the presence
        of UNIQUE constraints on the grouped columns.
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            if not select.group_by:
                continue

            group_by_constraint = next((c for c in select.all_constraints if c.constraint_type == ConstraintType.GROUP_BY), None)
            if not group_by_constraint:
                # No GROUP BY constraint found, meaning GROUP BY clause in invalid. Skip.
                continue

            constraints = [c for c in select.all_constraints if c.constraint_type == ConstraintType.UNIQUE]

            for constraint in constraints:
                if constraint.columns.issubset(group_by_constraint.columns):
                    results.append(DetectedError(SqlErrors.GROUP_BY_WITH_SINGLETON_GROUPS, (group_by_constraint, constraint)))
                    break        

        return results
    
    def detect_96_group_by_with_only_a_single_group(self) -> list[DetectedError]:
        return []
    
    def detect_97_group_by_can_be_replaced_by_distinct(self) -> list[DetectedError]:
        '''
        Flags GROUP BY clauses that can be replaced by SELECT DISTINCT.
        This occurs when all selected columns are included in the GROUP BY clause
        and there are no aggregate functions in the SELECT list.
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            select = select.strip_subqueries()

            if not select.group_by:
                continue

            if not select.ast:
                continue

            has_agg_func = False
            for expression in select.ast.expressions:
                if list(expression.find_all(exp.AggFunc)):
                    has_agg_func = True
                    break

            if has_agg_func:
                continue

            select_columns: list[exp.Column] = []
            for expression in select.ast.expressions:
                columns = list(expression.find_all(exp.Column))
                select_columns.extend(columns)
            
            group_by_columns: list[exp.Column] = []
            for expression in select.group_by:
                columns = list(expression.find_all(exp.Column))
                group_by_columns.extend(columns)

            select_col_names = {(util.ast.column.get_real_name(col), select.get_table_idx_for_column(col)) for col in select_columns}
            group_by_col_names = {(util.ast.column.get_real_name(col), select.get_table_idx_for_column(col)) for col in group_by_columns}

            if select_col_names == group_by_col_names:
                results.append(DetectedError(SqlErrors.GROUP_BY_CAN_BE_REPLACED_WITH_DISTINCT, (select_col_names,)))

        return results
                        
    def detect_98_union_can_be_replaced_by_or(self) -> list[DetectedError]:
        return []
    
    def detect_99_unnecessary_column_in_order_by_clause(self) -> list[DetectedError]:
        '''
        Flags when the ORDER BY clause contains unnecessary columns in addition
        to the required ones.
        '''
        return []
    
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_orderby_cols = self._get_orderby_columns(self.q_ast)
        s_orderby_cols = self._get_orderby_columns(self.s_ast)

        q_cols_set = {col.lower() for col, direction in q_orderby_cols}
        s_cols_set = {col.lower() for col, direction in s_orderby_cols}

        if s_cols_set and s_cols_set.issubset(q_cols_set) and len(q_cols_set) > len(s_cols_set):
            unnecessary_cols = q_cols_set - s_cols_set
            for col_lower in unnecessary_cols:
                original_col = next((col for col, direction in q_orderby_cols if col.lower() == col_lower), col_lower)
                results.append((
                    SqlErrors.COM_99_UNNECESSARY_COLUMN_IN_ORDER_BY_CLAUSE,
                    f"Unnecessary column in ORDER BY clause: '{original_col}' is not needed for sorting."
                ))

        return results
    
    def detect_100_order_by_in_subquery(self) -> list[DetectedError]:
        '''
        Flags when a subquery contains an ORDER BY clause.
        Subqueries both ORDER BY and LIMIT are considered valid.
        '''

        results: list[DetectedError] = []

        # nested subqueries are checked multiple times, so track which have been checked
        checked_subqueries: set[str] = set()

        for select in self.query.selects:
            for subquery, clause, depth in select.subqueries:
                if subquery.sql in checked_subqueries:
                    continue

                checked_subqueries.add(subquery.sql)
                if subquery.order_by and not subquery.limit:
                    results.append(DetectedError(SqlErrors.ORDER_BY_IN_SUBQUERY, (subquery.sql,)))

        return results
    
    def detect_101_inefficient_having(self) -> list[DetectedError]:
        return []
    
    def detect_102_inefficient_union(self) -> list[DetectedError]:
        return []
    
    def detect_103_condition_in_the_subquery_can_be_moved_up(self) -> list[DetectedError]:
        return []
    
    def detect_104_outer_join_can_be_replaced_by_inner_join(self) -> list[DetectedError]:
        return []

    def detect_126_unused_cte(self) -> list[DetectedError]:
        results: list[DetectedError] = []

        if not self.query.ctes:
            return results
        
        used_ctes: dict[int, bool] = {i: False for i in range(len(self.query.ctes))}

        for select in self.query.selects:
            for table in select.referenced_tables:
                if table.cte_idx is not None:
                    used_ctes[table.cte_idx] = True

        for cte_idx, used in used_ctes.items():
            if not used:
                results.append(DetectedError(SqlErrors.UNUSED_CTE, (self.query.ctes[cte_idx].sql,)))

        return results
