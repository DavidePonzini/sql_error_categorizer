from sql_error_categorizer.query.util import remove_parentheses
from .set_operation import SetOperation
from ..tokenized_sql import TokenizedSQL
from .. import extractors
from ...catalog import Catalog, Table
from ...util import *

from copy import deepcopy


import sqlglot
import sqlglot.errors
from sqlglot import exp


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

    # region Properties
    @property
    def subqueries(self) -> list['Select']:
        '''Returns a list of subqueries as TokenizedSQL objects.'''
        if self._subqueries is None:
            self._subqueries = []
            if self.ast:
                subquery_asts = extractors.extract_subqueries(self.ast)
                for subquery_ast in subquery_asts:
                    subquery_sql = remove_parentheses(subquery_ast.sql())
                    subquery = Select(subquery_sql, catalog=self.catalog, search_path=self.search_path)
                    self._subqueries.append(subquery)
        
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

        columns = self.ast.expressions if self.ast else []
        for col in columns:
            if isinstance(col, exp.Star):
                # Expand star to all columns from all referenced tables
                for table in self.referenced_tables:
                    for column in table.columns:
                        result.add_column(name=column.name, column_type='TODO')
            elif isinstance(col, exp.Alias):
                alias = col.args['alias']
                quoted = alias.quoted
                col_name = alias.this

                result.add_column(name=col_name if quoted else col_name.lower(), column_type='TODO')
            elif isinstance(col, exp.Column):

                # Handle table.* case
                if isinstance(col.this, exp.Star):
                    table_name = normalize_ast_column_table(col)
                    table = next((t for t in self.referenced_tables if t.name == table_name), None)
                    if table:
                        for column in table.columns:
                            result.add_column(name=column.name, column_type='TODO')
                else:
                    col_name = col.alias_or_name
                    name = col_name if col.this.quoted else col_name.lower()

                    result.add_column(name=name, column_type='TODO')

            else:
                # mostly unrecognized expressions (e.g. functions, literals, operations), that result in a column without a specific name
                result.add_column(name='', column_type='TODO')

        return result
    
    @property
    def order_by(self) -> list[OrderByColumn]:
        if not self.ast:
            return []
        order = self.ast.args.get('order')
        if not order:
            return []
        
        # TODO: handle table.col and indexes
        result: list[OrderByColumn] = []
        for order_exp in order.expressions:
            if isinstance(order_exp, exp.Ordered):
                col_exp = order_exp.this
                if isinstance(col_exp, exp.Column):
                    name = normalize_ast_column_name(col_exp)
                    descending = order_exp.args.get('desc', False)
                    result.append(OrderByColumn(column=name, table=col_exp.table, ascending=not descending))

        return result
    
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
        return self.subqueries + [self]

