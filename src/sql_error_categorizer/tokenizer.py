'''Tokeninzes a SQL query into a list of tokens using `sqlparse`.'''

from copy import deepcopy
import sqlparse
from sqlparse.tokens import Whitespace, Newline
from sqlparse.sql import IdentifierList, Identifier, Function

class TokenizedSQL:
    def __init__(self, query: str):
        self.sql = query
        self.parsed = self._parse()

        # Lazy properties
        self._tokens = None
        self._identifiers = None
        self._functions = None

    @property
    def tokens(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Returns a flattened list of tokens as (ttype, value) tuples, excluding whitespace and newlines.'''
        if not self._tokens:
            self._tokens = self._tokenize()
        return self._tokens

    @property
    def identifiers(self) -> list[tuple[sqlparse.sql.Identifier, str]]:
        '''
            Returns a list of tuples (identifier, alias, clause) where:
                - identifier: The name of the identifier (table or column)
                - alias: The alias if present, else None
                - clause: The SQL clause where the identifier appears (e.g., SELECT, FROM, WHERE)
        '''
        if not self._identifiers:
            self._identifiers = self._extract_identifiers()
        return self._identifiers
    
    @property
    def functions(self) -> list[sqlparse.sql.Function]:
        if self._functions is None:
            self._functions = self._extract_functions()
        return self._functions

    def _parse(self) -> sqlparse.sql.Statement | None:
        parsed = sqlparse.parse(self.sql)
        
        if not parsed:
            return None
        return parsed[0]

    def _tokenize(self) -> list[tuple[sqlparse.tokens._TokenType, str]]:
        '''Tokenizes a SQL query into a list of tokens using sqlparse, preserving context for syntax analysis.'''
        # Parse the SQL statement
        if not self.parsed:
            return []
        # Flatten tokens into (ttype, value)
        return [
            (tok.ttype, tok.value) for tok in self.parsed.flatten()
            if tok.ttype not in (Whitespace, Newline)
        ]

    # region Identifiers
    def _extract_identifiers(self) -> list[tuple[sqlparse.sql.Identifier, str]]:
        if not self.parsed:
            return []

        return self._extract_identifiers_rec(self.parsed.tokens)
    
    @staticmethod
    def _extract_identifiers_rec(tokens, current_clause: str = 'NONE') -> list[tuple[sqlparse.sql.Identifier, str]]:
        identifiers = []

        for token in tokens:
            if token.ttype is sqlparse.tokens.Keyword or token.ttype is sqlparse.tokens.DML or token.ttype is sqlparse.tokens.CTE:
                if token.value.upper() in ('WITH', 'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT'):
                    current_clause = token.value.upper()
                continue

            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    identifiers.append((identifier, current_clause))
            elif isinstance(token, Identifier):
                identifiers.append((token, current_clause))
            elif token.is_group:
                sub_identifiers = TokenizedSQL._extract_identifiers_rec(token.tokens, current_clause)
                identifiers.extend(sub_identifiers)
        return identifiers
    # endregion

    # region Functions
    def _extract_functions(self) -> list[sqlparse.sql.Function]:
        if not self.parsed:
            return []
        return self._extract_functions_rec(self.parsed.tokens)

    @staticmethod
    def _extract_functions_rec(tokens) -> list[sqlparse.sql.Function]:
        funcs: list[sqlparse.sql.Function] = []
        for tok in tokens:
            if isinstance(tok, Function):
                # Include this function
                funcs.append(tok)
                # Also search inside for nested function calls
                funcs.extend(TokenizedSQL._extract_functions_rec(tok.tokens))
            elif tok.is_group:
                funcs.extend(TokenizedSQL._extract_functions_rec(tok.tokens))
        return funcs
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