from . import extractors
from ..catalog import Catalog, Table, Column
from ..util import *

from abc import ABC, abstractmethod
from copy import deepcopy

import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Whitespace, Newline, Keyword

import sqlglot
import sqlglot.errors
from sqlglot import exp


class SetOperation(ABC):
    def __init__(self, sql: str, subquery_level: int = 0):
        self.sql = sql
        '''The SQL string representing the operation.'''
        
        self.subquery_level = subquery_level
        '''The level of subquery nesting.'''

    @property
    @abstractmethod
    def output(self) -> Table:
        '''Returns the output table schema of the set operation.'''
        return Table('')
    
    def __repr__(self, pre: str = '') -> str:
        return f'{pre}{self.__class__.__name__}'

    
    @abstractmethod
    def print_tree(self, pre: str = '') -> None:
        pass

    @property
    @abstractmethod
    def limit(self) -> int | None:
        return None
    
    @property
    @abstractmethod
    def offset(self) -> int | None:
        return None
    
    @property
    @abstractmethod
    def order_by(self) -> list[Column]:
        return []

class BinarySetOperation(SetOperation, ABC):
    '''Represents a binary set operation (e.g., UNION, INTERSECT, EXCEPT).'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool = False):
        super().__init__(sql)
        self.left = left
        self.right = right
        self.all = all
        '''Indicates whether the operation is ALL (duplicates allowed) or DISTINCT (duplicates removed).'''

    def __repr__(self, pre: str = '') -> str:
        result = f'{pre}{self.__class__.__name__}(ALL={self.all})\n'
        result +=  self.left.__repr__(pre + '|- ') + '\n'
        result += self.right.__repr__(pre + '`- ')

        return result

    @property
    def output(self) -> Table:
        # Assuming both sides have the same schema for simplicity
        return self.left.output
    
    def print_tree(self, pre: str = '') -> None:
        print(f'{pre}{self.__class__.__name__} (ALL={self.all})')
        print(                      f'{pre}|- Left:')
        self.left.print_tree(pre=   f'{pre}|  ')
        print(                      f'{pre}`- Right:')
        self.right.print_tree(pre=  f'{pre}   ')

    # TODO: Implement
    @property
    def limit(self) -> int | None:
        return None
    
    # TODO: Implement
    @property
    def offset(self) -> int | None:
        return None

    # TODO: Implement
    @property
    def order_by(self) -> list[Column]:
        return []

class Union(BinarySetOperation):
    '''Represents a SQL UNION operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool = False):
        super().__init__(sql, left, right, all=all)

class Intersect(BinarySetOperation):
    '''Represents a SQL INTERSECT operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool = False):
        super().__init__(sql, left, right, all=all)

class Except(BinarySetOperation):
    '''Represents a SQL EXCEPT operation.'''
    def __init__(self, sql: str, left: SetOperation, right: SetOperation, all: bool = False):
        super().__init__(sql, left, right, all=all)

class Select(SetOperation):
    '''Represents a single SQL SELECT statement.'''

    def __init__(self,
                 query: str,
                 *,
                 catalog: Catalog = Catalog(),
                 search_path: str = 'public'
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
        self.sql = query

        self.catalog = catalog
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
        self._functions = None
        self._comparisons = None
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
    def subqueries(self) -> list['Select']:
        '''Returns a list of subqueries as TokenizedSQL objects.'''
        if self._subqueries is None:
            self._subqueries = []
            if self.ast:
                subquery_asts = extractors.extract_subqueries(self.ast)
                for subquery_ast in subquery_asts:
                    subquery_sql = subquery_ast.sql()
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
    def tokens(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Returns a flattened list of tokens as (ttype, value) tuples, excluding whitespace and newlines.'''
        if not self._tokens:
            self._tokens = self._flatten()
        return self._tokens

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
    
    # TODO: Implement
    @property
    def order_by(self) -> list[Column]:
        return []
    
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
        return f'{pre}{self.__class__.__name__}(SQL="{self.sql.splitlines()[0]}...")'
    
def create_set_operation_tree(sql: str, catalog: Catalog = Catalog(), search_path: str = 'public') -> SetOperation:
    '''
    Parses a SQL string and constructs a tree of SetOperation objects representing the query structure using sqlparse.

    Args:
        sql (str): The SQL query string to parse.
        catalog (Catalog): The database catalog for resolving table and column names.
        search_path (str): The search path for schema resolution.

    Returns:
        SetOperation: The root of the SetOperation tree representing the query.
    '''

    def find_set_operation(tokens):
        # Returns (operation, left_sql, right_sql, all) or None
        op_keywords = {'UNION', 'INTERSECT', 'EXCEPT'}
        all = False
        op = None
        left_tokens = []
        right_tokens = []
        found_op = False
        for i, tok in enumerate(tokens):
            if tok.ttype is Keyword and tok.value.upper() in op_keywords:
                op = tok.value.upper()
                found_op = True
                # Check for ALL/DISTINCT
                next_tok = tokens[i+1] if i+1 < len(tokens) else None
                if next_tok and next_tok.ttype is Keyword and next_tok.value.upper() == 'ALL':
                    all = True
                elif next_tok and next_tok.ttype is Keyword and next_tok.value.upper() == 'DISTINCT':
                    all = False
                left_tokens = tokens[:i]
                right_tokens = tokens[i+1:]
                break
        if not found_op:
            return None
        # Remove ALL/DISTINCT from right_tokens if present
        if right_tokens and right_tokens[0].ttype is Keyword and right_tokens[0].value.upper() in ('ALL', 'DISTINCT'):
            right_tokens = right_tokens[1:]
        left_sql = TokenList(left_tokens).value.strip()
        right_sql = TokenList(right_tokens).value.strip()
        return (op, left_sql, right_sql, all)

    parsed = sqlparse.parse(sql)
    if not parsed:
        return Select(sql, catalog=catalog, search_path=search_path)
    statement = parsed[0]

    result = find_set_operation(statement.tokens)
    if result:
        op, left_sql, right_sql, all = result
        left_op = create_set_operation_tree(left_sql, catalog=catalog, search_path=search_path)
        right_op = create_set_operation_tree(right_sql, catalog=catalog, search_path=search_path)
        if op == 'UNION':
            return Union(sql, left_op, right_op, all=all)
        elif op == 'INTERSECT':
            return Intersect(sql, left_op, right_op, all=all)
        elif op == 'EXCEPT':
            return Except(sql, left_op, right_op, all=all)
    # Not a set operation; return as a Select
    return Select(sql, catalog=catalog, search_path=search_path)

