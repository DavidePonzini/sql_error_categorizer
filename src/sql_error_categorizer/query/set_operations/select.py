from sql_error_categorizer.query.util import remove_parentheses
from .set_operation import SetOperation
from ..tokenized_sql import TokenizedSQL
from .. import extractors
from ...catalog import Catalog, Table
from ...util import *
from sql_error_categorizer.query.typechecking import get_type, rewrite_expression

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

    def strip_subqueries(self) -> 'Select':
        '''Returns the SQL query with all subqueries removed (replaced by a context-aware placeholder).'''

        stripped_sql = self.sql

        subquery_sqls = extractors.extract_subqueries_tokens(self.sql)

        counter = 1
        for subquery_sql, clause in subquery_sqls:
            replacement = 'NULL'  # default safe fallback

            clause_upper = (clause or '').upper()

            if clause_upper in ('FROM', 'JOIN'):
                replacement = f'__subq{counter}'
                counter += 1
            elif clause_upper in ('WHERE', 'HAVING', 'ON', 'SELECT', 'COMPARISON'):
                replacement = 'NULL'
            elif clause_upper in ('IN', 'EXISTS'):
                replacement = '(NULL)'

            escaped = re.escape(subquery_sql)
            pattern = rf'\(\s*{escaped}\s*\)'

            # Replace the parentheses and enclosed subquery entirely
            stripped_sql, n = re.subn(pattern, replacement, stripped_sql, count=1)

            # Fallback: if not found with parentheses, remove raw subquery text
            if n == 0:
                stripped_sql = re.sub(escaped, replacement, stripped_sql, count=1)

        return Select(stripped_sql, catalog=self.catalog, search_path=self.search_path)

    # region Properties
    @property
    def subqueries(self) -> list[tuple['Select', str]]:
        '''Returns a list of subqueries as TokenizedSQL objects.'''
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
        Returns a list of output column names from the main query, properly normalized.
        
        Returns:
            list[str]: A list of output column names.
        '''

        result = super().output

        typed_ast = rewrite_expression(self.ast, self.referenced_tables)

        columns = typed_ast.expressions if typed_ast else []
        for col in columns:
            if isinstance(col, exp.Star):
                # Expand star to all columns from all referenced tables
                for table in self.referenced_tables:
                    for column in table.columns:
                        result.add_column(name=column.name, column_type=column.column_type, is_nullable=column.is_nullable)
            elif isinstance(col, exp.Alias):
                alias = col.args['alias']
                quoted = alias.quoted
                col_name = alias.this

                res_type = get_type(col.this)
                result.add_column(name=col_name if quoted else col_name.lower(), column_type=res_type.data_type, is_nullable=res_type.nullable)

            elif isinstance(col, exp.Column):

                # Handle table.* case
                if isinstance(col.this, exp.Star):
                    table_name = normalize_ast_column_table(col)
                    table = next((t for t in self.referenced_tables if t.name == table_name), None)
                    if table:
                        for column in table.columns:
                            result.add_column(name=column.name, column_type=column.column_type, is_nullable=column.is_nullable)
                else:
                    col_name = col.alias_or_name
                    name = col_name if col.this.quoted else col_name.lower()

                    res_type = get_type(col)
                    result.add_column(name=name, column_type=res_type.data_type, is_nullable=res_type.nullable)

            elif isinstance(col, exp.Subquery):
                subquery = Select(remove_parentheses(col.sql()), catalog=self.catalog, search_path=self.search_path)

                # Add the first column of the subquery's output
                if subquery.output.columns:
                    subquery_col = subquery.output.columns[0]
                    res_type = get_type(subquery_col)
                    result.add_column(name=subquery_col.name, column_type=res_type.data_type, is_nullable=res_type.nullable)
                else:
                    result.add_column(name='', column_type='None')

            else:
                # mostly unrecognized expressions (e.g. functions, literals, operations), that result in a column without a specific name
                res_type = get_type(col)
                result.add_column(name='', column_type=res_type.data_type, is_nullable=res_type.nullable)

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
    
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Select):
            return False
        
        if self.ast and value.ast:
            return self.ast.sql() == value.ast.sql()
        
        return self.tokens == value.tokens
    
    @property
    def selects(self) -> list['Select']:
        return [self] + [subquery for subquery, _ in self.subqueries]

