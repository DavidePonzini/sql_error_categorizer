import sqlparse
from sqlparse.tokens import Whitespace, Newline

def strip_ws(tokens: list[sqlparse.sql.Token]) -> list[sqlparse.sql.Token]:
    return [t for t in tokens if t.ttype not in (Whitespace, Newline)]

