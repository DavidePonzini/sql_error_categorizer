import difflib
import re
import sqlparse
import sqlparse.keywords
from typing import Callable

from .base import BaseDetector, DetectedError
from ..query import Query
from ..sql_errors import SqlErrors
from ..catalog import Catalog


class LogicalErrorDetector(BaseDetector):
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
        results = super().run()

        # If correct_solutions are not provided, return an empty list
        if not self.solutions:
            print("Missing correct solutions to analyze logical errors.")
            return results
        
        # AST parsing for proposed and correct solutions
        # self.q_ast = get_ast(self.query.sql)
        # self.s_ast = [get_ast(sol) for sol in self.correct_solutions]
        # self.s_ast = self.s_ast[0] # TODO: for now we only support one correct solution

        # if not self.q_ast or not self.s_ast:
        #     return results
        
        checks = [
            # self.log_1_operator_error_or_instead_of_and,  # TODO: refactor
            # self.log_1_operator_error_incorrect_comparison_operator_or_value, # TODO: refactor
            # self.log_4_expression_error_missing_expression,   # TODO: refactor
            # self.log_4_expression_error_expression_on_incorrect_column,   # TODO: refactor
            # self.log_5_projection_error_extraneous_column_in_select,  # TODO: refactor
            # self.log_5_projection_error_missing_column_from_select,   # TODO: refactor
            # self.log_5_projection_error_missing_column_from_order_by, # TODO: refactor
            # self.log_5_projection_error_incorrect_column_in_order_by, # TODO: refactor
            # self.log_5_projection_error_extraneous_order_by_clause,   # TODO: refactor
            # self.log_5_projection_error_incorrect_ordering_of_rows    # TODO: refactor
        ]

        for chk in checks:
            results.extend(chk())

        return results
        
    def log_1_operator_error_or_instead_of_and(self) -> list:
        """
        Detects if OR is used instead of AND in the WHERE or HAVING clauses
        by comparing the query's AST against the correct solution's AST.
        """
        results = []
        clauses_to_check = ['where', 'having']

        for clause_name in clauses_to_check:
            # Safely access the clause (e.g., 'where') from both the proposed (q) and correct (s) solution ASTs.
            q_clause = self.q_ast.get('args', {}).get(clause_name)
            s_clause = self.s_ast.get('args', {}).get(clause_name)

            # If the clause doesn't exist in both queries, skip to the next one.
            if not q_clause or not s_clause:
                continue

            # Extract the top-level operator ('And', 'Or', etc.) from the clause.
            q_operator = q_clause.get('args', {}).get('this', {}).get('class')
            s_operator = s_clause.get('args', {}).get('this', {}).get('class')

            # Check if the proposed query incorrectly uses 'Or' when the correct solution uses 'And'.
            if q_operator == 'Or' and s_operator == 'And':
                results.append((
                    SqlErrors.LOG_1_OPERATOR_ERROR_OR_INSTEAD_OF_AND,
                    f"OR used instead of AND in the {clause_name.upper()} clause"
                ))
                
        return results
    
    def log_1_operator_error_incorrect_comparison_operator_or_value(self) -> list:
        """
        Flags errors in comparison operators or values in WHERE and HAVING clauses.
        
        This function identifies two types of errors:
        1.  An incorrect comparison operator is used (e.g., '<' instead of '>').
        2.  An incorrect literal value is used in a comparison (e.g., 'Morandi' instead of 'Morando').
        """
        results = []

        # 1. Extract all comparison tuples from the proposed and correct queries.
        q_comparisons = []
        s_comparisons = []

        # Extract from WHERE clause
        for ast, comp_list in [(self.q_ast, q_comparisons), (self.s_ast, s_comparisons)]:
            clause_node = ast.get('args', {}).get('where', {}).get('args', {}).get('this')
            if clause_node:
                comp_list.extend(self._get_comparisons(clause_node))
            
            # Extract from HAVING clause
            clause_node = ast.get('args', {}).get('having', {}).get('args', {}).get('this')
            if clause_node:
                comp_list.extend(self._get_comparisons(clause_node))

        # 2. Create a map of the correct comparisons for efficient lookup.
        # The key is the column name, and the value is a (operator, value) tuple.
        s_comp_map = {comp[0]: (comp[1], comp[2]) for comp in s_comparisons}

        # 3. Iterate through the proposed query's comparisons and check for mismatches.
        for q_col, q_op, q_val in q_comparisons:
            # Case-insensitive column lookup
            q_col_lower = q_col.lower()
            s_comp_map_lower = {k.lower(): v for k, v in s_comp_map.items()}
            
            if q_col_lower in s_comp_map_lower:
                s_op, s_val = s_comp_map_lower[q_col_lower]

                # Check for an incorrect comparison operator
                if q_op != s_op:
                    results.append((
                        SqlErrors.LOG_1_OPERATOR_ERROR_INCORRECT_COMPARISON_OPERATOR_OR_VALUE,
                        f"Incorrect operator on column '{q_col}'. Found {q_op} but expected {s_op}."
                    ))

                # Check for an incorrect comparison value (exact comparison for all value types)
                if q_val != s_val:
                    results.append((
                        SqlErrors.LOG_1_OPERATOR_ERROR_INCORRECT_COMPARISON_OPERATOR_OR_VALUE,
                        f"Incorrect value in comparison for column '{q_col}'. Found '{q_val}' but expected '{s_val}'."
                    ))
        return results
    
    def log_4_expression_error_missing_expression(self) -> list:
        """Flags when a required expression is missing from the SELECT clause."""
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        # Get structured expressions from both the proposed and correct queries
        q_exprs = self._get_structured_expressions(self.q_ast)
        s_exprs = self._get_structured_expressions(self.s_ast)

        # Convert to case-insensitive tuples for comparison
        q_expr_set = {(func.lower(), col.lower()) for func, col in q_exprs}
        s_expr_set = {(func.lower(), col.lower()) for func, col in s_exprs}

        missing_expressions = s_expr_set - q_expr_set
        
        for func, col in missing_expressions:
            # Format the expression string for the error message
            expr_str = f"{func.upper()}({col})"
            results.append((
                SqlErrors.LOG_4_EXPRESSION_ERROR_MISSING_EXPRESSION,
                f"The expression '{expr_str}' is missing from the SELECT clause."
            ))
            
        return results

    def log_4_expression_error_expression_on_incorrect_column(self) -> list:
        """Flags when an expression (e.g., AVG) is used on an incorrect column."""
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_exprs = self._get_structured_expressions(self.q_ast)
        s_exprs = self._get_structured_expressions(self.s_ast)

        # Convert to case-insensitive for comparison
        s_expr_set = {(func.lower(), col.lower()) for func, col in s_exprs}
        
        # Create sets of all functions and columns used correctly in the solution's expressions
        s_funcs_present = {func.lower() for func, col in s_exprs}
        s_cols_present_in_exprs = {col.lower() for func, col in s_exprs}

        for q_func, q_col in q_exprs:
            q_func_lower = q_func.lower()
            q_col_lower = q_col.lower()
            
            # An expression is a candidate for this error if it's not in the correct set
            if (q_func_lower, q_col_lower) not in s_expr_set:
                # Check if the function AND column exist separately in the correct solution,
                # which strongly implies they were just paired incorrectly.
                if q_func_lower in s_funcs_present and q_col_lower in s_cols_present_in_exprs:
                    
                    # Find what the column *should* have been for this function
                    correct_col = "unknown"
                    for s_f, s_c in s_exprs:
                        if s_f.lower() == q_func_lower:
                            correct_col = s_c
                            break
                    
                    if correct_col != "unknown" and correct_col.lower() != q_col_lower:
                        results.append((
                            SqlErrors.LOG_4_EXPRESSION_ERROR_EXPRESSION_ON_INCORRECT_COLUMN,
                            f"The function '{q_func}' was applied to the wrong column. Expected {q_func}({correct_col}) but found {q_func}({q_col})."
                        ))
        return results

    def log_4_expression_error_extraneous_error(self) -> list:
        """
        Flags when an extraneous expression is included in the SELECT clause.
        """
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        # Re-use the helper that gets structured representations of expressions like ('AVG', 'Age').
        q_exprs = self._get_structured_expressions(self.q_ast)
        s_exprs = self._get_structured_expressions(self.s_ast)

        # Use sets for an efficient difference operation.
        q_exprs_set = set(q_exprs)
        s_exprs_set = set(s_exprs)

        # Find expressions that are in the user's query but NOT in the correct solution.
        extraneous_expressions = q_exprs_set - s_exprs_set

        for func, col in extraneous_expressions:
            # Format the expression into a user-friendly string.
            expr_str = f"{func}({col})"
            
            results.append((
                SqlErrors.LOG_4_EXPRESSION_ERROR_EXTRANEOUS_ERROR,
                f"The expression '{expr_str}' is extraneous and should be removed from the SELECT clause."
            ))
            
        return results
    
    def log_5_projection_error_extraneous_column_in_select(self) -> list:
        """
        Flags when an extraneous column is included in the SELECT clause,
        with a special check for inappropriate use of 'SELECT *'.
        """
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        # First, check specifically for the misuse of 'SELECT *'
        user_selects_star = self._selects_star(self.q_ast)
        solution_selects_star = self._selects_star(self.s_ast)

        if user_selects_star and not solution_selects_star:
            results.append((
                SqlErrors.LOG_5_PROJECTION_ERROR_EXTRANEOUS_COLUMN_IN_SELECT,
                "Using 'SELECT *' is not correct for this query. Please select only the required columns."
            ))
            return results

        # If the user correctly used 'SELECT *' (because the solution also did),
        # then there's no need to check for individual extraneous columns.
        if user_selects_star:
            return results

        # If 'SELECT *' was not misused, proceed with the original logic for named columns.
        q_cols = self._get_select_columns(self.q_ast)
        s_cols = self._get_select_columns(self.s_ast)

        q_cols_set = {col.lower() for col in q_cols}
        s_cols_set = {col.lower() for col in s_cols}

        extraneous_columns = q_cols_set - s_cols_set

        for col_lower in extraneous_columns:
            # Find the original case from the query for the error message
            original_col = next((col for col in q_cols if col.lower() == col_lower), col_lower)
            results.append((
                SqlErrors.LOG_5_PROJECTION_ERROR_EXTRANEOUS_COLUMN_IN_SELECT,
                f"The column '{original_col}' is extraneous and should be removed from the SELECT clause."
            ))
            
        return results

    def log_5_projection_error_missing_column_from_select(self) -> list:
        """
        Flags when a required column is missing from the SELECT clause,
        correctly handling cases where duplicate columns are required.
        """
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_cols = self._get_select_columns(self.q_ast)
        s_cols = self._get_select_columns(self.s_ast)

        # Use collections.Counter to track the frequency of each column
        q_counts = Counter(col.lower() for col in q_cols)
        s_counts = Counter(col.lower() for col in s_cols)

        # Subtracting counters finds the exact number of missing instances for each column
        missing_counts = s_counts - q_counts

        # Iterate through the elements of the resulting counter to generate a message for each missing column
        for col_lower in missing_counts.elements():
            # Find the original case from the solution for a more user-friendly message
            original_col = next((col for col in s_cols if col.lower() == col_lower), col_lower)
            results.append((
                SqlErrors.LOG_5_PROJECTION_ERROR_MISSING_COLUMN_FROM_SELECT,
                f"A required column '{original_col}' is missing from the SELECT clause."
            ))
                
        return results
    
    def log_5_projection_error_missing_column_from_order_by(self) -> list:
        """Flags when a required column is missing from the ORDER BY clause."""
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_orderby_cols = self._get_orderby_columns(self.q_ast)
        s_orderby_cols = self._get_orderby_columns(self.s_ast)

        # Create sets of column names for easy comparison (case-insensitive)
        q_cols_set = {col.lower() for col, direction in q_orderby_cols}
        s_cols_set = {col.lower() for col, direction in s_orderby_cols}
        
        # Find columns in the solution's ORDER BY that are not in the user's
        missing_cols = s_cols_set - q_cols_set
        for col_lower in missing_cols:
            # Find the original case from the solution
            original_col = next((col for col, direction in s_orderby_cols if col.lower() == col_lower), col_lower)
            results.append((
                SqlErrors.LOG_5_PROJECTION_ERROR_MISSING_COLUMN_FROM_ORDER_BY,
                f"The column '{original_col}' is missing from the ORDER BY clause."
            ))
        return results

    def log_5_projection_error_incorrect_column_in_order_by(self) -> list:
        """Flags when a column is incorrectly included in the ORDER BY clause."""
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_orderby_cols = self._get_orderby_columns(self.q_ast)
        s_orderby_cols = self._get_orderby_columns(self.s_ast)

        # Create sets of column names for easy comparison (case-insensitive)
        q_cols_set = {col.lower() for col, direction in q_orderby_cols}
        s_cols_set = {col.lower() for col, direction in s_orderby_cols}
        
        # Find columns in the user's ORDER BY that are not in the solution's
        incorrect_cols = q_cols_set - s_cols_set
        for col_lower in incorrect_cols:
            # Find the original case from the query
            original_col = next((col for col, direction in q_orderby_cols if col.lower() == col_lower), col_lower)
            results.append((
                SqlErrors.LOG_5_PROJECTION_ERROR_INCORRECT_COLUMN_IN_ORDER_BY,
                f"The column '{original_col}' should not be in the ORDER BY clause."
            ))
        return results

    def log_5_projection_error_extraneous_order_by_clause(self) -> list:
        """Flags when an ORDER BY clause is present but not required."""
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_has_orderby = self.q_ast.get('args', {}).get('order_by') is not None
        s_has_orderby = self.s_ast.get('args', {}).get('order_by') is not None

        if q_has_orderby and not s_has_orderby:
            results.append((
                SqlErrors.LOG_5_PROJECTION_ERROR_EXTRANEOUS_ORDER_BY_CLAUSE,
                "The ORDER BY clause is not required for this query."
            ))
        return results

    def log_5_projection_error_incorrect_ordering_of_rows(self) -> list:
        """Flags when a column in ORDER BY has the wrong sort direction (ASC/DESC)."""
        results = []
        if not self.q_ast or not self.s_ast:
            return results

        q_orderby_cols = self._get_orderby_columns(self.q_ast)
        s_orderby_cols = self._get_orderby_columns(self.s_ast)

        # Use dictionaries for easy lookup of a column's sort direction (case-insensitive keys)
        q_order_map = {col.lower(): (col, direction) for col, direction in q_orderby_cols}
        s_order_map = {col.lower(): (col, direction) for col, direction in s_orderby_cols}

        print(f"q_order_map: {q_order_map}") if self.debug else None
        print(f"s_order_map: {s_order_map}") if self.debug else None

        # Check for columns that are present in both but have different directions
        for col_lower, (q_col_orig, q_dir) in q_order_map.items():
            if col_lower in s_order_map:
                s_col_orig, s_dir = s_order_map[col_lower]
                if q_dir != s_dir:
                    results.append((
                        SqlErrors.LOG_5_PROJECTION_ERROR_INCORRECT_ORDERING_OF_ROWS,
                        f"Incorrect sort direction for column '{q_col_orig}'. Expected {s_dir} but found {q_dir}."
                    ))
        return results
    
    #region Utility methods
    def _get_comparisons(self, node: dict) -> list:
        """
        Recursively traverses an AST node to find all comparison expressions.
        
        Args:
            node: The AST node to start traversal from.
            
        Returns:
            A list of tuples, where each tuple represents a comparison in the
            form (column_name, operator_class, literal_value).
        """
        if not node or not isinstance(node, dict):
            return []

        node_class = node.get('class')
        args = node.get('args', {})

        # Base case: The node is a comparison operator (e.g., EQ, LT, GT).
        comparison_operators = {'EQ', 'NE', 'GT', 'GTE', 'LT', 'LTE'}
        if node_class in comparison_operators:
            left_operand = args.get('this', {})
            right_operand = args.get('expression', {})

            # We only evaluate simple "Column <operator> Literal" expressions.
            if left_operand.get('class') == 'Column' and right_operand.get('class') == 'Literal':
                try:
                    column_name = left_operand['args']['this']['args']['this']
                    literal_value = right_operand['args']['this']
                    return [(column_name, node_class, literal_value)]
                except KeyError:
                    return [] # AST structure is not as expected.
            return []

        # Recursive step: The node is a logical combiner (AND, OR).
        logical_operators = {'And', 'Or'}
        if node_class in logical_operators:
            left_results = self._get_comparisons(args.get('this'))
            right_results = self._get_comparisons(args.get('expression'))
            return left_results + right_results
        
        return []
    
    def _get_structured_expressions(self, ast: dict) -> list:
        """
        Extracts a list of structured representations of aggregate/function expressions
        from a SELECT query's AST.

        Args:
            ast: The Abstract Syntax Tree of the query.

        Returns:
            A list of tuples, e.g., [('AVG', 'Age'), ('COUNT', '*')].
        """
        structured_exprs = []
        if not ast:
            return structured_exprs

        # Navigate to the list of expressions in the SELECT clause
        select_expressions = ast.get('args', {}).get('expressions', [])
        
        for expr_node in select_expressions:
            node_class = expr_node.get('class')
            
            # Check for common aggregate functions
            if node_class in {'Avg', 'Sum', 'Count', 'Min', 'Max'}:
                target_node = expr_node.get('args', {}).get('this', {})
                
                # Handle the case of COUNT(*)
                if target_node.get('class') == 'Star':
                    structured_exprs.append((node_class, '*'))
                # Handle functions on a specific column, e.g., AVG(Age)
                elif target_node.get('class') == 'Column':
                    try:
                        col_name = target_node['args']['this']['args']['this']
                        structured_exprs.append((node_class, col_name))
                    except KeyError:
                        # Could not parse column name, so skip this expression
                        continue
        return structured_exprs
    
    def _get_select_columns(self, ast: dict) -> list:
        """
        Extracts a list of simple column names from a SELECT query's AST.
        This version handles simple columns, qualified columns (table.col), and aliased columns.
        """
        columns = []
        if not ast:
            return columns

        select_expressions = ast.get('args', {}).get('expressions', [])
        
        for expr_node in select_expressions:
            # This recursive helper will dive into aliases to find the base column.
            col_name = self._find_underlying_column(expr_node)
            if col_name:
                # Normalize to lowercase for case-insensitive comparison
                columns.append(col_name.lower())
        
        return columns

    def _find_underlying_column(self, node: dict):
        """
        Recursively traverses an expression node to find the underlying column identifier.
        """
        if not isinstance(node, dict):
            return None
        
        node_class = node.get('class')

        # Base case: We found a column. Handle both qualified and simple names.
        if node_class == 'Column':
            try:
                # Qualified column name, e.g., c1.cID -> 'cID'
                return node['args']['expression']['args']['this']
            except (KeyError, TypeError):
                try:
                    # Simple column name, e.g., cID -> 'cID'
                    return node['args']['this']['args']['this']
                except (KeyError, TypeError):
                    return None

        # Recursive step: The node is an alias, so check the aliased expression.
        if node_class == 'Alias':
            return self._find_underlying_column(node.get('args', {}).get('this'))
        
        # Return None if it's another type of expression (e.g., a function or literal)
        return None
    
    def _selects_star(self, ast: dict) -> bool:
        """
        Checks if a 'SELECT *' is used in the query by looking for a 'Star'
        node in the AST's expression list.

        Args:
            ast: The Abstract Syntax Tree of the query.

        Returns:
            True if 'SELECT *' is found, otherwise False.
        """
        if not ast:
            return False
        try:
            select_expressions = ast['args']['expressions']
            for expr_node in select_expressions:
                if expr_node.get('class') == 'Star':
                    return True
        except (KeyError, TypeError):
            # Handles cases where the AST structure is unexpected
            return False
        return False
    
    def _get_orderby_columns(self, ast: dict) -> list:
        """
        Extracts a list of columns and their sort direction from an ORDER BY clause.

        Args:
            ast: The Abstract Syntax Tree of the query.

        Returns:
            A list of tuples, e.g., [('col_name', 'ASC'), ('col_name2', 'DESC')].
        """
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
                    # Check for the 'desc' boolean flag in the term's arguments.
                    is_desc = term_node.get('args', {}).get('desc', False)
                    direction = 'DESC' if is_desc else 'ASC'
                    orderby_terms.append((col_name, direction))
        except (KeyError, AttributeError):
            return []
            
        return orderby_terms
    #endregion Utility methods