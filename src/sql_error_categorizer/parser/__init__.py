'''Parses (possibly incorrect) SQL queries to extract CTEs, subqueries, and main query components.'''

from dataclasses import dataclass, field
import re
from typing import Any

import sqlglot
import sqlglot.errors

from .ctes import extract_ctes, CTEMap, create_cte_catalog, CTECatalog
from .subqueries import extract_subqueries, SubqueryMap
from .query import parse_query, QueryMap


@dataclass
class ParseResult:
    cte_map: CTEMap = field(default_factory=CTEMap)
    subquery_map: SubqueryMap = field(default_factory=SubqueryMap)
    query_map: QueryMap = field(default_factory=QueryMap)
    ctes: list[str] = field(default_factory=list)
    main_query: str = field(default='')


def parse(query: str) -> ParseResult:
    cte_map, stripped_query, ctes = extract_ctes(query)
    subquery_map, main_query = extract_subqueries(stripped_query)
    query_map = parse_query(main_query)

    return ParseResult(cte_map=cte_map, query_map=query_map, subquery_map=subquery_map, ctes=ctes, main_query=main_query)

def get_clause_content(clause, query):
    '''
        The query_parser does not extract WHERE, HAVING, or ON clauses, so we extract them manually.
        Works only from the WHERE clause to the end of the query.
    '''
    pattern = rf'\b{clause}\b(.*?)(?:\bGROUP BY|\bHAVING|\bORDER BY|\bLIMIT|\bUNION\b|\bINTERSECT\b|\bEXCEPT\b|$)'
    match = re.search(pattern, query, re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else ""

def get_ast(query: str) -> Any:
    """
    Generates a serializable dictionary representation of the AST for the
    given SQL query using sqlglot.

    This method uses sqlglot's standard parser and is ideal for converting
    the AST to JSON or for other serialization purposes.

    Args:
        query: The SQL query string.

    Returns:
        A dictionary representing the entire AST, or None if parsing fails.
    """
    if not query:
        return None
    try:
        # Parse the query into a sqlglot expression object
        expression = sqlglot.parse_one(query)
        # Dump the expression object into a serializable dictionary
        if expression:
            return expression.dump()
        return None
    except sqlglot.errors.ParseError:
        # If parsing fails completely, return None
        return None
    

    
    


