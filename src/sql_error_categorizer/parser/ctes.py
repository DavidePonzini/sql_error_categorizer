from .util import normalize_query
from .query import parse_query, QueryMap
import re

CTEMap = dict[str, QueryMap]

def extract_ctes(query: str) -> tuple[CTEMap, str, list[str]]:
    '''Extract and parse CTE blocks from the beginning of the query.'''
    # self.IS_CTE = True
    query = normalize_query(query)

    if not re.match(r'^\s*WITH\b', query, re.IGNORECASE):
        # self.IS_CTE = False
        return CTEMap(), query, []

    cte_map = CTEMap()

    # Find the end of the CTE block, which is before the last SELECT statement
    # that is not enclosed in parentheses.
    
    # Remove the WITH keyword to start processing
    query_after_with = query.lstrip().split(' ', 1)[1]

    depth = 0
    last_cte_end_index = -1
    
    # Find the start of the main query by looking for a SELECT not in parentheses
    # starting from what we think is the end of CTEs
    
    # A simple heuristic: find the last closing parenthesis of a CTE definition
    # then find the next SELECT. This is brittle.
    # A better way is to parse the CTEs definitions.
    
    cte_defs_str = ''
    remaining_query = ''

    paren_depth = 0
    in_string = False
    last_comma_index = -1
    
    # Let's find the real end of the CTE section
    # The CTE section ends when we have a SELECT statement at parenthesis depth 0
    
    temp_query = query_after_with.lstrip()

    # Find the start of the main query (a SELECT not inside parentheses)
    # The CTE definitions are before it.
    
    search_start = 0
    while True:
        match = re.search(r'\(|\)|\bSELECT\b', temp_query[search_start:], re.IGNORECASE)
        if not match:
            # No SELECT found, something is wrong with the query.
            return CTEMap(), query, []

        if match.group(0).upper() == 'SELECT' and depth == 0:
            # This is the main query's SELECT
            cte_section = temp_query[:search_start + match.start()]
            remaining_query = temp_query[search_start + match.start():]
            break
        elif match.group(0) == '(':
            depth += 1
        elif match.group(0) == ')':
            depth -= 1
        
        search_start += match.end()


    # Split multiple CTEs
    cte_defs = []
    depth = 0
    current = ''
    for ch in cte_section:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        
        if ch == ',' and depth == 0:
            cte_defs.append(current.strip())
            current = ''
        else:
            current += ch
    
    if current.strip():
        cte_defs.append(current.strip())

    ctes = []
    for cte_def in cte_defs:
        # The regex needs to handle CTE names that might be keywords if not quoted
        # And it needs to handle optional column lists like `cte(c1, c2) AS ...`
        name_match = re.match(r'([a-zA-Z_][\w]*)\s*(?:\([^)]+\))?\s*AS\s*\((.+)\)', cte_def, re.IGNORECASE | re.DOTALL)
        if name_match:
            cte_name, cte_query = name_match.groups()
            cte_query = cte_query.strip()
            ctes.append(cte_query)
            # The query is already without the outer parentheses from the regex
            cte_map[cte_name.strip()] = parse_query(cte_query, in_cte=True)

    return cte_map, remaining_query, ctes

class CTECatalog:
    def __init__(self):
        # k: cte name, v: column names
        self.cte_tables: dict[str, list[str]] = {}

    def add_cte(self, cte_name: str, columns: list[str]):
        self.cte_tables[cte_name] = columns

    @property
    def tables(self) -> set[str]:
        return set(self.cte_tables.keys())
    
    def get_columns(self, cte_name: str) -> list[str]:
        return self.cte_tables.get(cte_name, [])
    
    def __repr__(self) -> str:
        return f'CTECatalog(cte_tables={self.cte_tables.__repr__()})'


def create_cte_catalog(cte_map: CTEMap) -> CTECatalog:
    '''
    Creates a catalog of CTEs, mapping CTE names to their column names.
    It resolves column aliases from the CTE's SELECT statement.
    '''
    cte_catalog = CTECatalog()
    for cte_name, cte_data in cte_map.items():
        columns = []
        select_values = cte_data.select_value
        alias_mapping = cte_data.alias_mapping

        # Create a reverse map from expression to alias for easier lookup
        expr_to_alias = {v[0]: k for k, v in alias_mapping.items() if v}

        for expr in select_values:
            # If the expression has an alias, use the alias as the column name.
            # Otherwise, use the expression itself (e.g., for columns like 'table.id').
            column_name = expr_to_alias.get(expr, expr)
            
            # If the column name is in 'table.column' format, take only the column part.
            if '.' in column_name:
                column_name = column_name.split('.')[-1]
                
            columns.append(column_name)
        
        cte_catalog.add_cte(cte_name, columns)
        
    return cte_catalog
