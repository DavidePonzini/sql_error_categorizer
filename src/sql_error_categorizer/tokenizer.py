import sqlparse
from sqlparse.tokens import Whitespace, Newline

def tokenize(query: str):
    """Tokenizes a SQL query into a list of tokens using sqlparse, preserving context for syntax analysis."""
    # Parse the SQL statement
    parsed = sqlparse.parse(query)
    if not parsed:
        return []
    # Flatten tokens into (ttype, value)
    return [
        (tok.ttype, tok.value) for tok in parsed[0].flatten()
        if tok.ttype not in (Whitespace, Newline)
    ]
