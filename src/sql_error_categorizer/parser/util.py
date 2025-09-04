import re

def normalize_query(query: str) -> str:
    '''Clean and normalize the SQL query for parsing.'''
    query = query.strip().rstrip(';')
    return re.sub(r'\s+', ' ', query.strip().replace('\n', ' '))

