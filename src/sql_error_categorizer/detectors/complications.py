'''Detector for complications in SQL queries.'''

import difflib
import re
import sqlparse
import sqlparse.keywords
from typing import Callable
from sqlglot import exp

from .base import BaseDetector, DetectedError
from ..query import Query
from ..sql_errors import SqlErrors
from ..catalog import Catalog


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
            self.com_83_unnecessary_distinct_in_select_clause,
            self.com_84_unnecessary_join,
            self.com_85_unused_correlation_name,
            self.com_86_correlation_names_are_always_identical,
            self.com_87_unnecessary_general_comparison_operator,
            self.com_88_like_without_wildcards,
            self.com_89_unnecessarily_complicated_select_in_exists_subquery,
            self.com_90_in_exists_can_be_replaced_by_comparison,
            self.com_91_unnecessary_aggregate_function,
            self.com_92_unnecessary_distinct_in_aggregate_function,
            self.com_93_unnecessary_argument_of_count,
            self.com_94_unnecessary_group_by_in_exists_subquery,
            self.com_95_group_by_with_singleton_groups,
            self.com_96_group_by_with_only_a_single_group,
            self.com_97_group_by_can_be_replaced_by_distinct,
            self.com_98_union_can_be_replaced_by_or,
            self.com_99_complication_unnecessary_column_in_order_by_clause,
            self.com_100_order_by_in_subquery,
            self.com_101_inefficient_having,
            self.com_102_inefficient_union,
            self.com_103_condition_in_the_subquery_can_be_moved_up,
            self.com_104_condition_on_left_table_in_left_outer_join,
            self.com_105_outer_join_can_be_replaced_by_inner_join,
        ]
        
        for chk in checks:
            results.extend(chk())

        return results

    # TODO: refactor
    def com_83_unnecessary_distinct_in_select_clause(self) -> list[DetectedError]:
        '''
        Flags the unnecessary use of DISTINCT by comparing the proposed query
        against the correct solution.
        '''
        return []

        results = []
        if not self.q_ast or not self.s_ast:
            return results

        # Check if the proposed query has a DISTINCT clause.
        # This can be a boolean `True` or a Dictionary node for `DISTINCT(...)`.
        q_args = self.q_ast.get('args', {})
        q_has_distinct = q_args.get('distinct') not in [None, False]

        # Check if the correct solution has a DISTINCT clause.
        s_args = self.s_ast.get('args', {})
        s_has_distinct = s_args.get('distinct') not in [None, False]

        # If the user's query has DISTINCT but the solution doesn't, it's unnecessary.
        if q_has_distinct and not s_has_distinct:
            results.append((
                SqlErrors.COM_83_UNNECESSARY_DISTINCT_IN_SELECT_CLAUSE,
                "The DISTINCT keyword is used unnecessarily and is not present in the optimal solution."
            ))
            
        return results

    # TODO: refactor
    def com_84_unnecessary_join(self) -> list[DetectedError]:
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
                    SqlErrors.COM_84_UNNECESSARY_JOIN,
                    f"Unnecessary JOIN: The table '{original_table_name}' is not needed to answer the query."
                ))
            
        return results
    
    # TODO: implement
    def com_85_unused_correlation_name(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_86_correlation_names_are_always_identical(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_87_unnecessary_general_comparison_operator(self) -> list[DetectedError]:
        return []
    
    def com_88_like_without_wildcards(self) -> list[DetectedError]:
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

                    results.append(DetectedError(SqlErrors.COM_88_LIKE_WITHOUT_WILDCARDS, (full_expression,)))

        return results
    
    # TODO: implement
    def com_89_unnecessarily_complicated_select_in_exists_subquery(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_90_in_exists_can_be_replaced_by_comparison(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_91_unnecessary_aggregate_function(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_92_unnecessary_distinct_in_aggregate_function(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_93_unnecessary_argument_of_count(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_94_unnecessary_group_by_in_exists_subquery(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_95_group_by_with_singleton_groups(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_96_group_by_with_only_a_single_group(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_97_group_by_can_be_replaced_by_distinct(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_98_union_can_be_replaced_by_or(self) -> list[DetectedError]:
        return []
    
    # TODO: refactor
    def com_99_complication_unnecessary_column_in_order_by_clause(self) -> list[DetectedError]:
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
    
    # TODO: implement
    def com_100_order_by_in_subquery(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_101_inefficient_having(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_102_inefficient_union(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_103_condition_in_the_subquery_can_be_moved_up(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_104_condition_on_left_table_in_left_outer_join(self) -> list[DetectedError]:
        return []
    
    # TODO: implement
    def com_105_outer_join_can_be_replaced_by_inner_join(self) -> list[DetectedError]:
        return []


    #region Utility methods
    def _get_select_columns(self, ast: dict) -> list:
        '''
        Extracts a list of simple column names from a SELECT query's AST.
        '''
        columns = []
        if not ast:
            return columns

        select_expressions = ast.get('args', {}).get('expressions', [])
        
        for expr_node in select_expressions:
            col_name = self._find_underlying_column(expr_node)
            if col_name:
                columns.append(col_name)
        
        return columns
    def _find_underlying_column(self, node: dict):
        '''
        Recursively traverses an expression node to find the underlying column identifier.
        '''
        if not isinstance(node, dict):
            return None
        
        node_class = node.get('class')

        if node_class == 'Paren':
            return self._find_underlying_column(node.get('args', {}).get('this'))

        if node_class == 'Column':
            try:
                return node['args']['expression']['args']['this']
            except (KeyError, TypeError):
                try:
                    return node['args']['this']['args']['this']
                except (KeyError, TypeError):
                    return None

        if node_class == 'Alias':
            return self._find_underlying_column(node.get('args', {}).get('this'))
    def _get_from_tables(self, ast: dict, with_alias=False) -> list:
        '''
        Extracts a list of all table names from the FROM and JOIN clauses of a query's AST.
        '''
        tables = []
        if not ast:
            return tables
        
        args = ast.get('args', {})

        # 1. Process the main table from the 'from' clause
        from_node = args.get('from')
        if from_node:
            # The actual table data is inside the 'this' argument of the 'From' node
            main_table_node = from_node.get('args', {}).get('this')
            if main_table_node:
                self._collect_tables_recursive(main_table_node, tables, with_alias)

        # 2. Process all tables from the 'joins' list
        join_nodes = args.get('joins', [])
        for join_node in join_nodes:
            self._collect_tables_recursive(join_node, tables, with_alias)
                
        return list(set(tables))
    def _collect_tables_recursive(self, node: dict, tables: list, with_alias=False):
        '''
        Recursively traverses a FROM clause node (including joins) to collect table names.
        '''
        if not isinstance(node, dict):
            return

        node_class = node.get('class')

        # This part handles aliased tables (e.g., "customer c") and regular tables
        if node_class == 'Alias':
            underlying_node = node.get('args', {}).get('this')
            # Recurse in case the alias is on a subquery or another join
            self._collect_tables_recursive(underlying_node, tables, with_alias)

        elif node_class == 'Table':
            try:
                # The AST nests identifiers, so we go deep to get the name
                table_name = node['args']['this']['args']['this']
                alias_node = node.get('args', {}).get('alias')
                if with_alias and alias_node:
                    alias_name = alias_node.get('args', {}).get('this', {}).get('args', {}).get('this')
                    tables.append(f"{table_name} AS {alias_name}")
                else:
                    tables.append(table_name)
            except (KeyError, TypeError):
                pass
        
        # This part handles Join nodes found in the 'joins' list
        elif node_class == 'Join':
            # The joined table is in the 'this' argument of the Join node
            self._collect_tables_recursive(node.get('args', {}).get('this'), tables, with_alias)
            # The other side of the join is already handled in the 'from' clause,
            # but we check for 'expression' for other potential join structures.
            if 'expression' in node.get('args', {}):
                self._collect_tables_recursive(node.get('args', {}).get('expression'), tables, with_alias)
    def _get_orderby_columns(self, ast: dict) -> list:
        '''
        Extracts a list of columns and their sort direction from an ORDER BY clause.
        '''
        orderby_terms = []
        if not ast:
            return orderby_terms

        orderby_node = ast.get('args', {}).get('order')
        if not orderby_node:
            return orderby_terms

        try:
            for term_node in orderby_node['args']['expressions']:
                if term_node.get('class') != 'Ordered':
                    continue
                
                column_node = term_node.get('args', {}).get('this')
                
                col_name = self._find_underlying_column(column_node)
                
                if col_name:
                    direction = term_node.get('args', {}).get('direction', 'ASC').upper()
                    orderby_terms.append((col_name, direction))
        except (KeyError, AttributeError):
            return []
            
        return orderby_terms
    #endregion Utility methods