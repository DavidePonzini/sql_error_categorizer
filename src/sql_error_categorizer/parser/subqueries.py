from .query import parse_query, QueryMap
from .util import normalize_query
import re

SubqueryMap = dict[str, QueryMap]

def extract_subqueries(query: str) -> tuple[SubqueryMap, str]:
    subquery_map = SubqueryMap()
    normalized_query = normalize_query(query)
    replacements = []

    # pattern = r'([\w\.]+\s*(?:=|<>|<|>|<=|>=|IN|NOT IN|>|<|=|!=)\s*(?:ALL|ANY|SOME)?\s*\(\s*(SELECT\b(?:[^()]+|\([^()]*\))*?)\s*\))'
    pattern = r'([\w\.]+\s*(?:=|<>|<|>|<=|>=|IN|NOT IN|!=)?\s*(?:ALL|ANY|SOME)?\s*\(\s*(SELECT\b(?:[^()]+|\((?:[^()]+|\([^()]*\))*\))*?)\s*\))'

    matches = re.finditer(pattern, normalized_query, re.IGNORECASE | re.DOTALL)
    for match in matches:
        condition = match.group(1).strip()
        subquery_sql = match.group(2).strip()
        subquery_map[condition] = parse_query(subquery_sql, in_subquery=True)
        replacements.append((match.start(), match.end(), match.group(1).split('(')[0].strip() + ' (...)'))

    exists_pattern = r'((?:NOT\s+)?EXISTS\s*\(\s*SELECT\b(?:[^()]*|\([^()]*\))*?\))'
    exists_matches = re.finditer(exists_pattern, normalized_query, re.IGNORECASE | re.DOTALL)
    for match in exists_matches:
        condition = match.group(1).strip()
        subquery_sql_match = re.search(r'\(\s*(SELECT\b.+)\)$', condition, re.IGNORECASE | re.DOTALL)
        if subquery_sql_match:
            subquery_clean = subquery_sql_match.group(1).strip()
            subquery_map[condition] = parse_query(subquery_clean, in_subquery=True)
            replacements.append((match.start(), match.end(), 'EXISTS (...)'))

    cleaned_query = normalized_query
    for start, end, replacement in sorted(replacements, key=lambda x: x[0], reverse=True):
        cleaned_query = cleaned_query[:start] + replacement + cleaned_query[end:]

    return subquery_map, cleaned_query