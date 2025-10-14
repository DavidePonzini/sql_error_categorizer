from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

from sql_error_categorizer.tokenizer.tokenized_sql import TokenizedSQL
from sql_error_categorizer import catalog

def test_main_query_no_cte():
    query = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = TokenizedSQL(query)

    assert tokenized.main_query.sql == query

def test_ctes_no_cte():
    query = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = TokenizedSQL(query)

    assert len(tokenized.ctes) == 0

def test_main_query_with_cte():
    query_cte = 'WITH cte AS (SELECT id, name FROM users)'

    query_main = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'
    tokenized = TokenizedSQL(f'{query_cte} {query_main}')

    assert tokenized.main_query.sql == query_main

def test_ctes_with_cte():
    query_cte = 'SELECT id, name FROM users'

    query = f'WITH cte AS ({query_cte}) SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = TokenizedSQL(query)

    assert len(tokenized.ctes) == 1
    assert tokenized.ctes[0][0] == 'cte'
    assert tokenized.ctes[0][1].sql == query_cte

def test_distinct_true():
    query = 'SELECT DISTINCT id, name FROM users'

    tokenized = TokenizedSQL(query)

    assert tokenized.distinct is True

def test_distinct_false():
    query = 'SELECT id, name FROM users'

    tokenized = TokenizedSQL(query)

    assert tokenized.distinct is False

def test_select_star():

    # setup
    db = 'miedema'
    catalog_db = catalog.load_json("tests/datasets/cat_miedema.json")
    table = 'store'

    query = f'SELECT * FROM {table}'

    tokenized = TokenizedSQL(query=query, catalog=catalog_db, search_path=db)

    assert len(tokenized.get_output().columns) == len(tokenized.catalog[db][table].columns)

def test_select_multiple_stars():

    # setup
    db = 'miedema'
    catalog_db = catalog.load_json("tests/datasets/cat_miedema.json")
    table = 'store'

    query = f'SELECT *,* FROM {table}'

    tokenized = TokenizedSQL(query=query, catalog=catalog_db, search_path=db)

    assert len(tokenized.get_output().columns) == len(tokenized.catalog[db][table].columns) * 2

def test_select_star_on_a_cte():

    # setup
    db = 'miedema'
    catalog_db = catalog.load_json("tests/datasets/cat_miedema.json")
    table = 'store'
    cte_name = 'cte_store'

    query = f'WITH {cte_name} AS (SELECT sid, sname FROM {table}) SELECT sid,* FROM {cte_name}'

    tokenized = TokenizedSQL(query=query, catalog=catalog_db, search_path=db)

    assert len(tokenized.get_output().columns) == 3  # sid + all columns from cte_store (sid, sname)