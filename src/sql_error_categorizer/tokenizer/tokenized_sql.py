'''Tokeninzes a SQL query into a list of tokens using `sqlparse`.'''

from copy import deepcopy
from typing import Self
import sqlparse
from sqlparse.tokens import Whitespace, Newline
from . import extractors
import sqlglot
import sqlglot.errors

class TokenizedSQL:
    def __init__(self, query: str, parent: Self | None = None) -> None:
        '''
        Initializes a TokenizedSQL object.

        Args:
            query (str): The SQL query string to tokenize.
            parent (TokenizedSQL | None): The parent TokenizedSQL object if this is a subquery.
        '''
        self.sql = query
        self.parent = parent

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
        self._ctes = None
        
        try:
            self.ast = sqlglot.parse_one(self.sql)
        except sqlglot.errors.ParseError:
            self.ast = None

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, TokenizedSQL):
            return False
        return self.sql == value.sql

    def _flatten(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Flattens the parsed SQL statement into a list of (ttype, value) tuples. Ignores whitespace and newlines.'''

        if not self.parsed:
            return []

        # Flatten tokens into (ttype, value)
        return [
            (tok.ttype, tok.value) for tok in self.parsed.flatten()
            if tok.ttype not in (Whitespace, Newline)
        ]
    

    # region Properties
    
    @property
    def main_query(self) -> 'TokenizedSQL':
        '''Returns the main query without CTEs as a TokenizedSQL object.'''
        if self._main_query is None:
            if self.ast:
                main_sql = extractors.remove_ctes(self.ast)
                self._main_query = TokenizedSQL(main_sql)
            else:
                self._main_query = TokenizedSQL('')
        
        return self._main_query
    
    @property
    def ctes(self) -> list[tuple[str, 'TokenizedSQL']]:
        '''Returns a list of (cte_name, TokenizedSQL) tuples representing the CTEs in the query.'''

        if self._ctes is None:
            self._ctes = []

            for cte_name, cte_sql in extractors.extract_ctes(self.ast):
                self._ctes.append((cte_name, TokenizedSQL(cte_sql)))

        return self._ctes


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
    # endregion



    # region Utilities
    def __repr__(self) -> str:
        return f'TokenizedSQL("{self.sql!r}")'
    
    def copy(self) -> 'TokenizedSQL':
        return deepcopy(self)
    
    def print_tree(self) -> None:
        if self.parsed:
            self.parsed._pprint_tree()
    # endregion