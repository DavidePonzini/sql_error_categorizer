'''Tokeninzes a SQL query into a list of tokens using `sqlparse`.'''

import sqlparse
from sqlparse.tokens import Whitespace, Newline

class TokenizedSQL:
    def __init__(self, query: str):
        self.sql = query
        self.parsed = self._parse()
        self.tokens = self._tokenize()

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

    def __repr__(self) -> str:
        return f'TokenizedSQL("{self.sql!r}")'