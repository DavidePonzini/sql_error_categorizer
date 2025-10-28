import difflib
import re
import sqlparse
import sqlparse.keywords
from typing import Callable

from .base import BaseDetector, DetectedError
from ..query import Query
from ..sql_errors import SqlErrors
from ..catalog import Catalog


class ComplicationDetector(BaseDetector):
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

        # If correct_solutions are not provided, return an empty list
        if not self.solutions:
            print("Missing correct solutions to analyze logical errors.")
            return results
        
        checks = [
            self.com_1_like_without_wildcards,
            self.com_1_complication_unnecessary_distinct_in_select_clause,
            self.com_1_complication_unnecessary_join,
            self.com_1_complication_unnecessary_column_in_order_by_clause
        ]
        
        for chk in checks:
            results.extend(chk())

        return results

    # TODO: refactor
    def com_1_like_without_wildcards(self) -> list[DetectedError]:
        '''
        Flags queries where the LIKE operator is used without wildcards ('%' or '_').
        This indicates a potential misunderstanding, where the '=' operator should
        have been used instead.
        '''
        return []

        results = []
        like_expressions = re.finditer(
            r"LIKE\s+((['\"]).*?\2|\w+)", 
            self.query.sql, 
            re.IGNORECASE
        )

        for match in like_expressions:
            pattern = match.group(1)
            
            if '%' not in pattern and '_' not in pattern:
                full_expression = match.group(0)
                results.append((
                    SqlErrors.COM_1_COMPLICATION_LIKE_WITHOUT_WILDCARDS,
                    f"LIKE expression without wildcards: {full_expression}",
                ))
        return results

    # TODO: refactor
    def com_1_complication_unnecessary_distinct_in_select_clause(self) -> list[DetectedError]:
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
                SqlErrors.COM_1_COMPLICATION_UNNECESSARY_DISTINCT_IN_SELECT_CLAUSE,
                "The DISTINCT keyword is used unnecessarily and is not present in the optimal solution."
            ))
            
        return results

    # TODO: refactor
    def com_1_complication_unnecessary_join(self) -> list[DetectedError]:
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
                    SqlErrors.COM_1_COMPLICATION_UNNECESSARY_JOIN,
                    f"Unnecessary JOIN: The table '{original_table_name}' is not needed to answer the query."
                ))
            
        return results
    
    # TODO: refactor
    def com_1_complication_unnecessary_column_in_order_by_clause(self) -> list[DetectedError]:
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
                    SqlErrors.COM_1_COMPLICATION_UNNECESSARY_COLUMN_IN_ORDER_BY_CLAUSE,
                    f"Unnecessary column in ORDER BY clause: '{original_col}' is not needed for sorting."
                ))

        return results

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