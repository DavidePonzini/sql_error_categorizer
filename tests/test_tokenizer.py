from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

from sql_error_categorizer.tokenizer.tokenized_sql import TokenizedSQL

def test_tokenized_sql_main_query_no_cte():
    query = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = TokenizedSQL(query)

    assert tokenized.main_query.sql == query

def test_tokenized_sql_ctes_no_cte():
    query = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = TokenizedSQL(query)

    assert len(tokenized.ctes) == 0

def test_tokenized_sql_main_query_with_cte():
    query_cte = 'WITH cte AS (SELECT id, name FROM users)'

    query_main = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'
    tokenized = TokenizedSQL(f'{query_cte} {query_main}')

    assert tokenized.main_query.sql == query_main

def test_tokenized_sql_ctes_with_cte():
    query_cte = 'SELECT id, name FROM users'

    query = f'WITH cte AS ({query_cte}) SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = TokenizedSQL(query)

    assert len(tokenized.ctes) == 1
    assert tokenized.ctes[0][0] == 'cte'
    assert tokenized.ctes[0][1].sql == query_cte

