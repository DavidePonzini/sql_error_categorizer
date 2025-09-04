from dataclasses import dataclass
import re
from typing import List, Dict, Any
# from sqlglot.expression import Expression


from .util import normalize_query


SQL_KEYWORDS = frozenset({
    'SELECT', 'FROM',
    'JOIN', 'NATURAL JOIN', 'INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN',
    'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL JOIN', 'FULL OUTER JOIN',
    'ON', 'WHERE', 'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
    'AS', 'UNION', 'INTERSECT', 'EXCEPT', 'DISTINCT', 'TOP', 'ALL', 'ANY', 'EXISTS',
})


@dataclass
class QueryMap:
    select_value: List[str]
    from_value: str
    join_value: List[str]
    alias_mapping: Dict[str, List[str]]
    where: bool
    distinct: bool
    order_by: List[str]
    top_limit_first: bool
    set_op: bool
    set_op_values: Dict[str, List[str]]
    group_by_values: List[str]
    having: bool
    exists: bool
    all_any_condition: bool

    def __repr__(self) -> str:
        return (f'QueryMap('
                f'\n    select_value={self.select_value},'
                f'\n    from_value={self.from_value},'
                f'\n    join_value={self.join_value},'
                f'\n    alias_mapping={self.alias_mapping},'
                f'\n    where={self.where},'
                f'\n    distinct={self.distinct},'
                f'\n    order_by={self.order_by},'
                f'\n    top_limit_first={self.top_limit_first},'
                f'\n    set_op={self.set_op},'
                f'\n    set_op_values={self.set_op_values},'
                f'\n    group_by_values={self.group_by_values},'
                f'\n    having={self.having},'
                f'\n    exists={self.exists},'
                f'\n    all_any_condition={self.all_any_condition})'
                '\n')

    

# region Mapping methods

def extract_aliases(query: str, in_cte: bool = False, in_subquery: bool = False) -> Dict[str, List[str]]:
    alias_map: Dict[str, List[str]] = {}

    def add_alias(alias: str, base: str):
        if alias.upper() in SQL_KEYWORDS:
            return
        
        alias_map.setdefault(alias, [])
        if base not in alias_map[alias]:
            alias_map[alias].append(base)

    # Extend FROM clause to include JOIN to avoid cutting early
    from_match = re.search(
        r'\bFROM\b\s+(.*?)(\bWHERE\b|\bGROUP\b|\bHAVING\b|\bORDER\b|\bUNION\b|$)',
        query, re.IGNORECASE | re.DOTALL
    )
    if from_match:
        tables_section = from_match.group(1)

        # Only process from clause up to the start of the ON clause
        tables_only_section = re.split(r'\bON\b', tables_section, maxsplit=1, flags=re.IGNORECASE)[0]

        table_aliases = re.finditer(
            r'\b([a-zA-Z_][\w]*)\b\s+(?:AS\s+)?\b([a-zA-Z_][\w]*)\b',
            tables_only_section,
            re.IGNORECASE
        )
        for match in table_aliases:
            base, alias = match.groups()
            if base.upper() not in SQL_KEYWORDS and alias.upper() not in SQL_KEYWORDS:
                add_alias(alias, base)

    # JOIN clauses (separate for safety)
    # Handle all JOIN types, including NATURAL JOIN, LEFT JOIN, etc.
    join_patterns = re.finditer(
        r'\b(?:NATURAL\s+)?(?:LEFT\s+OUTER\s+|LEFT\s+|RIGHT\s+OUTER\s+|RIGHT\s+|FULL\s+OUTER\s+|FULL\s+|INNER\s+)?JOIN\b\s+([a-zA-Z_][\w]*)\s*(?:AS\s+)?([a-zA-Z_][\w]*)?',
        query, re.IGNORECASE
    )
    for match in join_patterns:
        table = match.group(1)
        alias = match.group(2)
        if alias:
            add_alias(alias, table)

    # SELECT clause for CTEs and subqueries
    if in_cte or in_subquery:
        clause = re.sub(r'\bTOP\s*\(\s*\d+\s*\)', '', query, flags=re.IGNORECASE)
        clause = re.sub(r'\bDISTINCT\b', '', query, flags=re.IGNORECASE)
        select_match = re.search(r'\bSELECT\b\s+(.*?)\bFROM\b', clause, re.IGNORECASE)
        if select_match:
            fields = select_match.group(1)
            columns = [c.strip() for c in re.split(r',(?![^(]*\))', fields)]
            for col in columns:
                # Match expressions like: SUM(price) AS total or user.id ID
                as_match = re.match(r'(.+?)\s+(?:AS\s+)?(\w+)$', col, re.IGNORECASE)
                if as_match:
                    expr, alias = as_match.groups()
                    add_alias(alias.strip(), expr.strip())

    return alias_map

def extract_keywords(query: str) -> Dict[str, Any]:
    """Extract basic boolean keyword presence flags."""
    return {
        'where': bool(re.search(r'\bWHERE\b', query, re.IGNORECASE)),
        'distinct': bool(re.search(r'\bDISTINCT\b', query, re.IGNORECASE)),
        'top_limit_first': bool(re.search(r'\bTOP\b|\bLIMIT\b|\bFIRST\b', query, re.IGNORECASE)),
        'set_op': bool(re.search(r'\bUNION\b|\bINTERSECT\b|\bEXCEPT\b', query, re.IGNORECASE)),
        'having': bool(re.search(r'\bHAVING\b', query, re.IGNORECASE)),
        'exists': bool(re.search(r'\bEXISTS\b', query, re.IGNORECASE)),
        'all_any_condition': bool(re.search(r'[\=\>\<]\s*(ALL|ANY)\b', query, re.IGNORECASE))
    }

def _clean_select_clause(clause: str) -> List[str]:
    """
    Cleans SELECT clause by removing TOP, DISTINCT, and aliases (with or without AS).
    Returns a list of raw expressions (e.g. table.column, SUM(x), etc).
    """
    # Remove TOP(n) and DISTINCT
    clause = re.sub(r'\bTOP\s*\(\s*\d+\s*\)', '', clause, flags=re.IGNORECASE)
    clause = re.sub(r'\bDISTINCT\b', '', clause, flags=re.IGNORECASE)

    # Split respecting parentheses
    columns = [c.strip() for c in re.split(r',(?![^(]*\))', clause) if c.strip()]
    cleaned = []

    for col in columns:
        # Remove alias: with or without AS
        match = re.match(r'(.+?)\s+(?:AS\s+)?\w+$', col, flags=re.IGNORECASE)
        if match:
            cleaned.append(match.group(1).strip())
        else:
            cleaned.append(col)

    return cleaned

def extract_select_fields(query: str) -> List[str]:
    match = re.search(r'\bSELECT\b\s+(.*?)(\bFROM\b|\bWHERE\b|\bGROUP\b|\bORDER\b|$)', query, re.IGNORECASE)
    return _clean_select_clause(match.group(1)) if match else []

def extract_from_table(query: str) -> str:
    match = re.search(r'\bFROM\b\s+([a-zA-Z_][\w]*)', query, re.IGNORECASE)
    return match.group(1) if match else ""

def extract_joined_tables(query: str) -> List[str]:
    joined = re.findall(r'\bJOIN\b\s+([a-zA-Z_][\w]*)', query, re.IGNORECASE)
    from_section = re.search(r'\bFROM\b\s+(.*?)(\bWHERE\b|\bORDER\b|\bGROUP\b|\bHAVING\b|\bUNION\b|$)', query, re.IGNORECASE)
    if from_section:
        tables = from_section.group(1).split(',')
        for table in tables[1:]:  # skip the first (main FROM)
            name = table.strip().split()[0]
            if name.upper() != "JOIN":
                joined.append(name)
    return [t for t in joined if t]

def extract_order_by(query: str) -> List[str]:
    match = re.search(r'\bORDER BY\b\s+(.*?)(\bUNION\b|\bLIMIT\b|\bOFFSET\b|$)', query, re.IGNORECASE)
    if not match:
        return []
    clause = match.group(1)
    return [s.strip() for s in clause.split(',') if s.strip()]

def extract_group_by(query: str) -> List[str]:
    match = re.search(r'\bGROUP BY\b\s+(.*?)(\bHAVING\b|\bORDER BY\b|$)', query, re.IGNORECASE)
    if not match:
        return []
    return [s.strip() for s in match.group(1).split(',') if s.strip()]

def extract_set_op_values(query: str) -> Dict[str, List[str]]:
    set_queries = re.split(r'\bUNION\b|\bEXCEPT\b|\bINTERSECT\b', query, flags=re.IGNORECASE)
    result = {}
    for i, q in enumerate(set_queries):
        sel = extract_select_fields(q)
        if sel:
            result[f'query{i + 1}'] = sel
    return result

def parse_query(query: str, in_cte: bool = False, in_subquery: bool = False) -> QueryMap:
    query = normalize_query(query)
    alias_map = extract_aliases(query, in_cte=in_cte, in_subquery=in_subquery)
    keyword_flags = extract_keywords(query)

    return QueryMap(
        select_value=extract_select_fields(query),
        from_value=extract_from_table(query),
        join_value=extract_joined_tables(query),
        alias_mapping=alias_map,
        where=keyword_flags['where'],
        distinct=keyword_flags['distinct'],
        order_by=extract_order_by(query),
        top_limit_first=keyword_flags['top_limit_first'],
        set_op=keyword_flags['set_op'],
        set_op_values=extract_set_op_values(query) if keyword_flags['set_op'] else {},
        group_by_values=extract_group_by(query),
        having=keyword_flags['having'],
        exists=keyword_flags['exists'],
        all_any_condition=keyword_flags['all_any_condition']
    )



