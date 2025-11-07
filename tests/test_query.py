import pytest
from sql_error_categorizer.query import *
from sql_error_categorizer import load_catalog

# region CTEs
def test_main_query_no_cte():
    sql = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    query = Query(sql)

    assert query.main_query.sql == sql

def test_ctes_no_cte():
    sql = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    query = Query(sql)

    assert len(query.ctes) == 0

def test_main_query_with_cte():
    sql_cte = 'WITH cte AS (SELECT id, name FROM users)'

    sql_main = 'SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'
    query = Query(f'{sql_cte} {sql_main}')

    assert query.main_query.sql == sql_main

def test_ctes_with_cte():
    sql_cte = 'SELECT id, name FROM users'

    sql = f'WITH cte AS ({sql_cte}) SELECT cte.id, orders.amount FROM cte JOIN orders ON cte.id = orders.user_id WHERE orders.amount > 100'

    query = Query(sql)

    assert len(query.ctes) == 1
    assert query.ctes[0].sql == sql_cte

# endregion

# region Properties
def test_distinct_true():
    sql = 'SELECT DISTINCT id, name FROM users'

    query = Query(sql)

    assert isinstance(query.main_query, Select)
    assert query.main_query.distinct is True

def test_distinct_false():
    sql = 'SELECT id, name FROM users'

    query = Query(sql)

    assert isinstance(query.main_query, Select)
    assert query.main_query.distinct is False

def test_select_star():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'

    sql = f'SELECT * FROM {table}'

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == len(query.catalog[db][table].columns)

def test_select_multiple_stars():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'

    sql = f'SELECT *,* FROM {table}'

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == len(query.catalog[db][table].columns) * 2

def test_select_star_on_a_cte():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'
    cte_name = 'cte_store'

    sql = f'WITH {cte_name} AS (SELECT sid, sname FROM {table}) SELECT sid,* FROM {cte_name}'

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == 3  # sid + all columns from cte_store (sid, sname)

def test_select_star_on_a_table():
    db = 'miedema'
    catalog_db = load_catalog("datasets/catalogs/miedema.json")
    table = 'store'
    join = 'transaction'

    sql = f"SELECT {table}.*, {join}.date FROM {table} JOIN {join} ON {table}.sid = {join}.sid;"

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert len(query.main_query.output.columns) == len(catalog_db[db][table].columns) + 1  # sid + all columns from store

# region set_operations

@pytest.mark.skip(reason="Not implemented yet")
def test_set_operation_order_by_limit_offset_left():
    db = 'miedema'
    catalog_db = load_catalog(f"datasets/catalogs/{db}.json")
    sql = "(SELECT sid,sname FROM store WHERE city = 'Breda' ORDER BY sname LIMIT 3 OFFSET 1) EXCEPT SELECT sid, sname FROM store WHERE city = 'Amsterdam';"

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert isinstance(query.main_query, BinarySetOperation)
    assert query.main_query.left.limit == 3
    assert query.main_query.left.offset == 1
    assert query.main_query.left.order_by == [('sname', 'ASC')]

@pytest.mark.skip(reason="Not implemented yet")
def test_set_operation_order_by_limit_offset_right():
    db = 'miedema'
    catalog_db = load_catalog(f"datasets/catalogs/{db}.json")
    sql = "SELECT sid,sname FROM store WHERE city = 'Breda' EXCEPT SELECT sid, sname FROM store WHERE city = 'Amsterdam' ORDER BY sname LIMIT 3 OFFSET 1;"

    query = Query(sql, catalog=catalog_db, search_path=db)

    assert isinstance(query.main_query, BinarySetOperation)
    assert query.main_query.limit == 3
    assert query.main_query.offset == 1
    assert query.main_query.order_by == [('sname', 'ASC')]

def test_set_operation_intersect_precedence1():
    sql = "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"

    query = Query(sql)

    assert isinstance(query.main_query, Union)
    assert query.main_query.left.sql == "SELECT 1"
    assert query.main_query.right.sql == "SELECT 2 INTERSECT SELECT 3"

    assert isinstance(query.main_query.right, Intersect)
    assert query.main_query.right.left.sql == "SELECT 2"
    assert query.main_query.right.right.sql == "SELECT 3"

def test_set_operation_intersect_precedence2():
    sql = "SELECT 1 INTERSECT SELECT 2 UNION SELECT 3"

    query = Query(sql)

    assert isinstance(query.main_query, Union)
    assert query.main_query.left.sql == "SELECT 1 INTERSECT SELECT 2"
    assert query.main_query.right.sql == "SELECT 3"

    assert isinstance(query.main_query.left, Intersect)
    assert query.main_query.left.left.sql == "SELECT 1"
    assert query.main_query.left.right.sql == "SELECT 2"

def test_selects_single():
    sql = "SELECT 1 FROM t1"

    query = Query(sql)

    assert len(query.selects) == 1
    assert query.selects[0].sql == "SELECT 1 FROM t1"

def test_selects_multiple():
    sql = "SELECT 1 FROM t1 UNION ALL SELECT id FROM t2 INTERSECT SELECT name, last_name FROM users"

    query = Query(sql)

    assert len(query.selects) == 3
    assert query.selects[0].sql == "SELECT 1 FROM t1"
    assert query.selects[1].sql == "SELECT id FROM t2"
    assert query.selects[2].sql == "SELECT name, last_name FROM users"