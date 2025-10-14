'''Tokeninzes a SQL query into a list of tokens using `sqlparse`.'''

from copy import deepcopy
import sqlparse
from sqlparse.tokens import Whitespace, Newline
from . import extractors
from ..catalog.catalog import Catalog, Table
import sqlglot
import sqlglot.errors
from sqlglot import exp
from ..util import *

class TokenizedSQL:
    def __init__(self,
                 query: str,
                 *,
                 catalog: Catalog = Catalog(),
                 search_path: str = 'public',
                 is_subquery: bool = False) -> None:
        '''
        Initializes a TokenizedSQL object.

        Args:
            query (str): The SQL query string to tokenize.
            catalog_db (Catalog): The database catalog for resolving table and column names.
            catalog_query (Catalog): The query-specific catalog for resolving table and column names.
            search_path (str): The search path for schema resolution.
            parent (TokenizedSQL | None): The parent TokenizedSQL object if this is a subquery.
        '''
        self.sql = query
        self.is_subquery = is_subquery

        self.catalog = deepcopy(catalog)
        '''Catalog representing tables that can be referenced in this query.'''
        
        self.search_path = search_path

        parsed_statements = sqlparse.parse(self.sql)
        if not parsed_statements:
            self.all_statements: list[sqlparse.sql.Statement] = []
            self.parsed = sqlparse.sql.Statement()
        else:
            self.all_statements = list(parsed_statements)
            self.parsed = parsed_statements[0]

        # Lazy properties
        self._tokens = None
        self._identifiers = None
        self._functions = None
        self._comparisons = None

        # NOTE: main_query must be lazy, to prevent infinite recursion
        self._main_query = None
        
        try:
            self.ast = sqlglot.parse_one(self.sql)
        except sqlglot.errors.ParseError:
            self.ast = None  # Empty expression on parse error

        # Extract CTEs
        self.ctes: list[tuple[str, TokenizedSQL]] = []
        for cte_name, cte_sql in extractors.extract_ctes(self.ast):
            cte = TokenizedSQL(cte_sql, catalog=self.catalog)

            self.ctes.append((cte_name, cte))

            # Add CTE output columns to catalog
            output = cte.get_output()
            output.name = cte_name
            self.catalog[''][cte_name] = output

        # Extract referenced tables
        self._referenced_tables = None
        '''Catalog representing tables that are read by this query.'''

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
                table_name_out = normalize_subquery_name(expr)

                subquery_sql = expr.this.sql()
                subquery = TokenizedSQL(subquery_sql, catalog=self.catalog, search_path=self.search_path, is_subquery=True)

                table = subquery.get_output()
                result.append(table)
            
            # Table: look it up in the IN catalog
            elif isinstance(expr, exp.Table):
                # schema name
                schema_name = normalize_schema_name(expr)
                table_name_in = normalize_table_real_name(expr)
                table_name_out = normalize_table_name(expr)

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

    def _flatten(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Flattens the parsed SQL statement into a list of (ttype, value) tuples. Ignores whitespace and newlines.'''

        if not self.parsed:
            return []

        # Flatten tokens into (ttype, value)
        return [
            (tok.ttype, tok.value) for tok in self.parsed.flatten()
            if tok.ttype not in (Whitespace, Newline)
        ]
    
    # endregion

    # region Properties
    @property
    def main_query(self) -> 'TokenizedSQL':
        '''Returns the main query without CTEs as a TokenizedSQL object.'''
        if self._main_query is None:
            if self.ast:
                main_sql = extractors.remove_ctes(self.ast)
                self._main_query = TokenizedSQL(main_sql, catalog=self.catalog)
            else:
                self._main_query = self
        
        return self._main_query
    
    @property
    def distinct(self) -> bool:
        '''Returns True if the main query has a DISTINCT clause.'''
        if self.ast and self.ast.args.get('distinct', False):
            return True
        return False
    
    @property
    def tokens(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Returns a flattened list of tokens as (ttype, value) tuples, excluding whitespace and newlines.'''
        if not self._tokens:
            self._tokens = self._flatten()
        return self._tokens

    @property
    def identifiers(self) -> list[tuple[sqlparse.sql.Identifier, str]]:
        '''Returns a list of (identifier, clause) tuples found in the SQL query.'''

        if not self._identifiers:
            self._identifiers = extractors.extract_identifiers(self.parsed.tokens)
        return self._identifiers
    
    @property
    def functions(self) -> list[tuple[sqlparse.sql.Function, str]]:
        '''Returns a list of (function, clause) tuples found in the SQL query.'''

        if self._functions is None:
            self._functions = extractors.extract_functions(self.parsed.tokens)
        return self._functions
    
    @property
    def comparisons(self) -> list[tuple[sqlparse.sql.Comparison, str]]:
        '''Returns a list of (comparison, clause) tuples found in the SQL query.'''
        if self._comparisons is None:
            self._comparisons = extractors.extract_comparisons(self.parsed.tokens)
        return self._comparisons    
    
    @property
    def referenced_tables(self) -> list[Table]:
        '''Returns a list of tables that are referenced in the SQL query.'''

        if self._referenced_tables is None:
            self._referenced_tables = self._get_referenced_tables()
        
        return self._referenced_tables

    # endregion


    # region Output
    def get_ctes_output(self, cte_names: list[str] = []) -> Catalog:
        '''
        Returns a dictionary mapping CTE names to their output columns.

        Args:
            cte_names (list[str]): List of CTE names to get output columns for. If empty, returns for all CTEs.

        Returns:
            dict[str, list[str]]: A dictionary mapping CTE names to their output columns.
        '''

        result = Catalog()
        for cte_name, cte in self.ctes:
            if not cte_names or cte_name in cte_names:  # check if we want this CTE
                output_table = cte.get_output()
                
                output_table.name = cte_name
                result[''][cte_name] = output_table

        return result

    def get_output(self) -> Table:
        '''
        Returns a list of output column names from the main query, properly normalized.
        
        Returns:
            list[str]: A list of output column names.
        '''

        result = Table('')

        columns = self.ast.expressions if self.ast else []
        for col in columns:
            if isinstance(col, exp.Star):
                ...
            elif isinstance(col, exp.Alias):
                alias = col.args['alias']
                quoted = alias.quoted
                col_name = alias.this

                result.add_column(name=col_name if quoted else col_name.lower(), column_type='TODO')
            elif isinstance(col, exp.Column):
                col_name = col.alias_or_name
                quoted =  col.this.quoted

                result.add_column(name=col_name if quoted else col_name.lower(), column_type='TODO')
            else:
                # mostly unrecognized expressions (e.g. functions, literals, operations), that result in a column without a specific name
                result.add_column(name='', column_type='TODO')

        return result

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


    # region Utilities
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, TokenizedSQL):
            return False
        return self.sql == value.sql

    def __repr__(self) -> str:
        return f'TokenizedSQL("{self.sql}")'
    
    def copy(self) -> 'TokenizedSQL':
        return deepcopy(self)
    
    def print_tree(self) -> None:
        if self.parsed:
            self.parsed._pprint_tree()
    # endregion