from sql_error_categorizer.query.util import remove_parentheses
from .set_operation import SetOperation
from ..tokenized_sql import TokenizedSQL
from .. import extractors
from ...catalog import Catalog, Table, UniqueConstraintType, Column
from ...catalog.catalog import UniqueConstraint
from ...util import *
from ..util import extract_function_name
from sql_error_categorizer.query.typechecking import get_type, to_res_type
from copy import deepcopy

from copy import deepcopy


import sqlglot
import sqlglot.errors
from sqlglot import exp
import re


class Select(SetOperation, TokenizedSQL):
    '''Represents a single SQL SELECT statement.'''

    def __init__(self,
                 query: str,
                 *,
                 catalog: Catalog = Catalog(),
                 search_path: str = 'public',
                 subquery_level: int = 0,
        ) -> None:
        '''
        Initializes a SelectStatement object.

        Args:
            query (str): The SQL query string to tokenize.
            catalog_db (Catalog): The database catalog for resolving table and column names.
            catalog_query (Catalog): The query-specific catalog for resolving table and column names.
            search_path (str): The search path for schema resolution.
            parent (TokenizedSQL | None): The parent TokenizedSQL object if this is a subquery.
        '''

        SetOperation.__init__(self, query, subquery_level=subquery_level)
        TokenizedSQL.__init__(self, query)

        self.catalog = catalog
        '''Catalog representing tables that can be referenced in this query.'''
        
        self.search_path = search_path

        self._subqueries = None
        self._referenced_tables = None
        '''Catalog representing tables that are read by this query.'''

        try:
            self.ast = sqlglot.parse_one(self.sql)
        except sqlglot.errors.ParseError:
            self.ast = None  # Empty expression on parse error        
        
    # region Auxiliary
    def _get_referenced_tables(self) -> list[Table]:
        '''Extracts referenced tables from the SQL query and returns them as a Catalog object.'''

        result: list[Table] = []

        if not self.ast:
            return result

        def add_tables_from_expression(expr: exp.Expression) -> None:
            '''Recursively adds tables from an expression to the result catalog.'''
            # Subquery: get its output columns
            if isinstance(expr, exp.Subquery):
                table_name_out = normalize_ast_subquery_name(expr)

                subquery_sql = expr.this.sql()
                subquery = Select(subquery_sql, catalog=self.catalog, search_path=self.search_path)

                result.append(subquery.output)
            
            # Table: look it up in the IN catalog
            elif isinstance(expr, exp.Table):
                # schema name
                schema_name = normalize_ast_schema_name(expr)
                table_name_in = normalize_ast_table_real_name(expr)
                table_name_out = normalize_ast_table_name(expr)

                if schema_name is None:
                    # If no schema is specified, try to find the table in the CTEs
                    if self.catalog.has_table(schema_name='', table_name=table_name_in):
                        schema_name = ''
                    # If not found in CTEs, use the search path
                    else:
                        schema_name = self.search_path 

                # check if the table exists in the catalog
                if self.catalog.has_table(schema_name=schema_name, table_name=table_name_in):
                    # Table exists
                    table_in = self.catalog[schema_name][table_name_in]

                    # Create a copy of the table with the output name
                    table = deepcopy(table_in)
                    table.name = table_name_out
                    result.append(table)
                else:
                    # Table does not exist, add as empty table
                    result.append(Table(name=table_name_out))
        
        from_expr = self.ast.args.get('from')
        

        if from_expr:
            add_tables_from_expression(from_expr.this)
            
        for join in self.ast.args.get('joins', []):
            add_tables_from_expression(join.this)

        return result

    # endregion

    def strip_subqueries(self, replacement: str = 'NULL') -> 'Select':
        '''Returns the SQL query with all subqueries removed (replaced by a context-aware placeholder).'''

        stripped_sql = self.sql

        subquery_sqls = extractors.extract_subqueries_tokens(self.sql)

        counter = 1
        for subquery_sql, clause in subquery_sqls:
            repl = replacement  # default safe fallback

            clause_upper = (clause or '').upper()

            if clause_upper in ('FROM', 'JOIN'):
                repl = f'__subq{counter}'
                counter += 1
            elif clause_upper in ('WHERE', 'HAVING', 'ON', 'SELECT', 'COMPARISON'):
                repl = replacement
            elif clause_upper in ('IN', 'EXISTS'):
                repl = f'({replacement})'

            escaped = re.escape(subquery_sql)
            pattern = rf'\(\s*{escaped}\s*\)'

            # Replace the parentheses and enclosed subquery entirely
            stripped_sql, n = re.subn(pattern, repl, stripped_sql, count=1)

            # Fallback: if not found with parentheses, remove raw subquery text
            if n == 0:
                stripped_sql = re.sub(escaped, repl, stripped_sql, count=1)

        return Select(stripped_sql, catalog=self.catalog, search_path=self.search_path)

    # region Properties
    @property
    def subqueries(self) -> list[tuple['Select', str]]:
        '''
            Returns a list of subqueries as TokenizedSQL objects.
        
            Returns:
                list[tuple[Select, str]]: A list of tuples containing subquery Select objects and their associated clause.
        '''
        if self._subqueries is None:
            self._subqueries = []
            # try to find subqueries via sqlglot AST, since it's more reliable
            # if self.ast:
            #     subquery_asts = extractors.extract_subqueries_ast(self.ast)
            #     for subquery_ast in subquery_asts:
            #         while not isinstance(subquery_ast, exp.Select):
            #             subquery_ast = subquery_ast.this
            #         subquery = Select(subquery_ast.sql(), catalog=self.catalog, search_path=self.search_path)
            #         self._subqueries.append(subquery)
            # else:
                # fallback: AST cannot be constructed, try to find subqueries via sqlparse
            subquery_sqls = extractors.extract_subqueries_tokens(self.sql)

            for subquery_sql, clause in subquery_sqls:
                subquery = Select(subquery_sql, catalog=self.catalog, search_path=self.search_path)
                self._subqueries.append((subquery, clause))
    
        return self._subqueries

    @property
    def distinct(self) -> bool:
        '''Returns True if the main query has a DISTINCT clause.'''
        if self.ast and self.ast.args.get('distinct', False):
            return True
        return False
    
    @property
    def referenced_tables(self) -> list[Table]:
        '''Returns a list of tables that are referenced in the SQL query.'''

        if self._referenced_tables is None:
            self._referenced_tables = self._get_referenced_tables()
        
        return self._referenced_tables
    

    @property
    def output(self) -> Table:
        '''
        Returns a Table object representing the output of this SELECT query.
        It includes inferred columns (name, type, nullability, constancy)
        and merged unique constraints from referenced tables.
        '''
        result = super().output
        if not self.ast:
            return result

        anonymous_counter = 1

        # ----------------------------------------------------------------------
        # Helper functions
        # ----------------------------------------------------------------------

        def get_anonymous_column_name() -> str:
            '''Generates a unique anonymous column name.'''
            nonlocal anonymous_counter
            name = f'?column_{anonymous_counter}?'
            anonymous_counter += 1
            return name

        def add_star() -> None:
            '''Expand SELECT * by adding all columns from all referenced tables.'''
            for idx, table in enumerate(self.referenced_tables):
                for col in table.columns:
                    result.add_column(
                        name=col.name,
                        table_idx=idx,
                        column_type=to_res_type(col.column_type).value,
                        is_nullable=col.is_nullable,
                        is_constant=col.is_constant
                    )

        def add_alias(column: exp.Alias) -> None:
            '''Add an expression with an explicit alias (e.g. SELECT expr AS alias).'''
            alias = column.args['alias']
            name = alias.this if alias.quoted else alias.this.lower()
            res_type = get_type(column.this, self.referenced_tables)[0]

            result.add_column(
                name=name,
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def add_table_star(column: exp.Column) -> None:
            '''Add all columns from a specific table (SELECT table.*).'''
            table_name = normalize_ast_column_table(column)
            table = next((t for t in self.referenced_tables if t.name == table_name), None)
            if table:
                for col in table.columns:
                    res_type = get_type(col, self.referenced_tables)[0]
                    result.add_column(
                        name=col.name,
                        table_idx=self.referenced_tables.index(table),
                        column_type=res_type.type.value,
                        is_nullable=res_type.nullable,
                        is_constant=res_type.constant
                    )

        def add_column(column: exp.Column) -> None:
            '''Add a column reference (SELECT column or table.column).'''
            table_name = normalize_ast_column_table(column)
            col_name = column.alias_or_name
            name = col_name if column.this.quoted else col_name.lower()

            # Resolve which table this column belongs to
            if table_name:
                table_idx = next((i for i, t in enumerate(self.referenced_tables) if t.name == table_name), None)
            else:
                table_idx = next((i for i, t in enumerate(self.referenced_tables)
                                if any(c.name == name for c in t.columns)), None)

            res_type = get_type(column, self.referenced_tables)[0]
            
            result.add_column(
                name=name,
                table_idx=table_idx,
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def add_subquery(column: exp.Subquery) -> None:
            '''Add a column derived from a subquery expression (SELECT (SELECT ...)).'''
            subq = Select(remove_parentheses(column.sql()), catalog=self.catalog, search_path=self.search_path)
            
            # Add the first column of the subquery's output
            if subq.output.columns:
                first_col = subq.output.columns[0]
                res_type = get_type(first_col, self.referenced_tables)[0]
            
                result.add_column(
                    name=first_col.name,
                    column_type=res_type.type.value,
                    is_nullable=res_type.nullable,
                    is_constant=res_type.constant
                )
            else:
                result.add_column(name=get_anonymous_column_name(), column_type='None')

        def add_literal(column: exp.Literal | exp.Expression) -> None:
            '''Add a literal or computed expression as a pseudo-column (e.g. SELECT 1, SELECT a+b).'''
            res_type = get_type(column, self.referenced_tables)[0]

            result.add_column(
                name=get_anonymous_column_name(),
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def add_function(column: exp.Func) -> None:
            '''Add a function output column (e.g. SELECT MAX(col)).'''
            res_type = get_type(column, self.referenced_tables)[0]

            result.add_column(
                name=get_anonymous_column_name(),
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def merge_unique_constraints() -> None:
            '''
            Merge unique constraints from all referenced tables.
            When multiple tables are joined, constraints are combined
            by unioning column sets across participating tables.
            '''
            tables = self.referenced_tables
            if not tables:
                return

            all_constraints = [deepcopy(t.unique_constraints) for t in tables]
            final_constraints = all_constraints[0]

            # Assign base table index
            for constraint in final_constraints:
                for constraint_column in constraint.columns:
                    constraint_column.table_idx = 0

            # Combine constraints across tables (Cartesian merge)
            for table_idx, constraints in enumerate(all_constraints[1:], start=1):
                merged: list[UniqueConstraint] = []
                for c1 in final_constraints:
                    for c2 in constraints:
                        for constraint_column in c2.columns:
                            constraint_column.table_idx = table_idx
                        merged.append(
                            UniqueConstraint(
                                c1.columns.union(c2.columns),
                                UniqueConstraintType.UNIQUE
                            )
                        )
                final_constraints = merged

            # Keep only constraints for which all columns appear in the output
            for unique_constraint in final_constraints:
                valid = all(
                    any(output_column.table_idx == constraint_column.table_idx and output_column.name == constraint_column.name
                        for output_column in result.columns if output_column.table_idx is not None)
                    for constraint_column in unique_constraint.columns
                )
                if valid:
                    result.unique_constraints.append(unique_constraint)

        # ----------------------------------------------------------------------
        # Main column extraction loop
        # ----------------------------------------------------------------------

        for expr in self.ast.expressions:
            if isinstance(expr, exp.Star):
                add_star()
            elif isinstance(expr, exp.Alias):
                add_alias(expr)
            elif isinstance(expr, exp.Column):
                # Handle SELECT table.* separately
                if isinstance(expr.this, exp.Star):
                    add_table_star(expr)
                else:
                    add_column(expr)
            elif isinstance(expr, exp.Subquery):
                add_subquery(expr)
            elif isinstance(expr, exp.Literal):
                add_literal(expr)
            elif isinstance(expr, exp.Func):
                add_function(expr)
            else:
                add_literal(expr)  # fallback for other expressions

        # ----------------------------------------------------------------------
        # Merge and attach unique constraints
        # ----------------------------------------------------------------------
        merge_unique_constraints()

        return result

    @property
    def where(self) -> exp.Expression | None:
        if not self.ast:
            return None
        where = self.ast.args.get('where')
        if not where:
            return None
        
        return where.this

    @property
    def group_by(self) -> list[exp.Expression]:
        if not self.ast:
            return []
        group = self.ast.args.get('group')
        if not group:
            return []
        
        return group.expressions

    @property
    def having(self) -> exp.Expression | None:
        if not self.ast:
            return None
        having = self.ast.args.get('having')
        if not having:
            return None
        
        return having.this
    
    @property
    def order_by(self) -> list[exp.Expression]:
        if not self.ast:
            return []
        order = self.ast.args.get('order')
        if not order:
            return []

        return order.expressions


    @property
    def limit(self) -> int | None:
        if not self.ast:
            return None
        limit_exp = self.ast.args.get('limit')
        if not limit_exp:
            return None
        try:
            return int(limit_exp.expression.this)
        except ValueError:
            return None
        
    @property
    def offset(self) -> int | None:
        if not self.ast:
            return None
        offset_exp = self.ast.args.get('offset')
        if not offset_exp:
            return None
        try:
            return int(offset_exp.expression.this)
        except ValueError:
            return None

    # def get_output_types(self, catalog: Catalog = Catalog()) -> list[str]:
    #     '''
    #     Returns a list of output column types from the main query.
        
    #     Returns:
    #         list[str]: A list of output column types.
    #     '''
    #     columns = self.ast.expressions if self.ast else []

    #     types = []
    #     for col in columns:

    # endregion


    def print_tree(self, pre: str = '') -> None:
        if self.parsed:
            self.parsed._pprint_tree(_pre=pre)

    def __repr__(self, pre: str = '') -> str:
        return f'{pre}{self.__class__.__name__}(SQL="{self.sql.splitlines()[0]}{"..." if len(self.sql.splitlines()) > 1 else ""}")'
    
    @property
    def selects(self) -> list['Select']:
        return [self] + [subquery for subquery, _ in self.subqueries]

