from tests import run_test, SyntaxErrorDetector, SqlErrors, has_error

from sql_error_categorizer.query import Query
from sql_error_categorizer.query.set_operations import *
from sql_error_categorizer import catalog

def test_main_query_no_cte():
    query = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = Query(query)

    assert tokenized.main_query.sql == query

def test_ctes_no_cte():
    query = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = Query(query)

    assert len(tokenized.ctes) == 0

def test_main_query_with_cte():
    query_cte = 'WITH cte AS (SELECT id, name FROM users)'

    query_main = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'
    tokenized = Query(f'{query_cte} {query_main}')

    assert tokenized.main_query.sql == query_main

def test_ctes_with_cte():
    query_cte = 'SELECT id, name FROM users'

    query = f'WITH cte AS ({query_cte}) SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    tokenized = Query(query)

    assert len(tokenized.ctes) == 1
    assert tokenized.ctes[0].sql == query_cte

def test_distinct_true():
    query = 'SELECT DISTINCT id, name FROM users'

    tokenized = Query(query)

    assert isinstance(tokenized.main_query, Select)
    assert tokenized.main_query.distinct is True

def test_distinct_false():
    query = 'SELECT id, name FROM users'

    tokenized = Query(query)

    assert isinstance(tokenized.main_query, Select)
    assert tokenized.main_query.distinct is False

def test_select_star():
    db = 'miedema'
    catalog_db = catalog.load_json("tests/datasets/cat_miedema.json")
    table = 'store'

    query = f'SELECT * FROM {table}'

    tokenized = Query(query, catalog=catalog_db, search_path=db)

    assert len(tokenized.main_query.output.columns) == len(tokenized.catalog[db][table].columns)

def test_select_multiple_stars():
    db = 'miedema'
    catalog_db = catalog.load_json("tests/datasets/cat_miedema.json")
    table = 'store'

    query = f'SELECT *,* FROM {table}'

    tokenized = Query(query, catalog=catalog_db, search_path=db)

    assert len(tokenized.main_query.output.columns) == len(tokenized.catalog[db][table].columns) * 2

def test_select_star_on_a_cte():
    db = 'miedema'
    catalog_db = catalog.load_json("tests/datasets/cat_miedema.json")
    table = 'store'
    cte_name = 'cte_store'

    query = f'WITH {cte_name} AS (SELECT sid, sname FROM {table}) SELECT sid,* FROM {cte_name}'

    tokenized = Query(query, catalog=catalog_db, search_path=db)

    assert len(tokenized.main_query.output.columns) == 3  # sid + all columns from cte_store (sid, sname)