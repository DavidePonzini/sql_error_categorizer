import pytest
from sql_error_categorizer.query.typechecking import get_type

def test_primitive_types(make_query):
    sql = "SELECT 'hello' AS str_col, 123 AS num_col, TRUE AS bool_col, NULL AS null_col, DATE '2020-01-01' AS date_col;"
    query = make_query(sql)
    result = []
    for exp in query.main_query.ast.expressions:
        col_type = get_type(exp, query.main_query.referenced_tables)[0]
        result.append(col_type.type.value)
    assert result == ["string", "number", "boolean", "null", "date"]

def test_type_columns(make_query):
    sql = "SELECT * FROM store;"
    query = make_query(sql)
    result = []
    for col in query.main_query.output.columns:
        result.append(col.column_type)
    
    assert result == ['number', 'string', 'string', 'string']


@pytest.mark.parametrize('sql, expected_types', [
    ("SELECT 1 + (2 - '4') AS sum_col;", [('number', True, False)]),
    ("SELECT s.sid FROM store s WHERE s.sid > '3';", [('number', False, False), ('boolean', True, False)]),
    ("SELECT sname FROM transaction,store WHERE date > '11-05-2020' AND price < (1-0.5) AND store.sid = transaction.sid;", [('string', False, False), ('boolean', True, False)])

])
def test_expression_types(sql, expected_types, make_query):
    query = make_query(sql)
    result = []
    for exp in query.main_query.ast.expressions:
        col_type = get_type(exp, query.main_query.referenced_tables)[0]
        result.append((col_type.type.value, col_type.constant, col_type.nullable))
    if query.main_query.where:
        where_type = get_type(query.main_query.where, query.main_query.referenced_tables)[0]
        result.append((where_type.type.value, where_type.constant, where_type.nullable))
    assert result == expected_types

# functions
def test_function_types(make_query):
    sql = "SELECT COUNT(DISTINCT sname), AVG(sid), SUM(sid), MIN(sname), MAX(sid), CONCAT(NULL,NULL,1), CONCAT(NULL) FROM store;"
    query = make_query(sql)

    result = []
    for col in query.main_query.output.columns:
        result.append(col.column_type)

    assert result == ['number', 'number', 'number', 'string', 'number', 'string', 'null']

# logical operators
@pytest.mark.parametrize('sql, expected_types', [
    ("SELECT sname FROM store WHERE sname LIKE 'C%';", 'boolean'),
    ("SELECT sname FROM store WHERE (sname LIKE 'C%') IS FALSE;", 'boolean'),
    ("SELECT sname FROM store WHERE sid IS NOT NULL;", 'boolean'),
    ("SELECT sname FROM store WHERE sid BETWEEN 1 AND 10;", 'boolean')
])
def test_logical_operator(sql, expected_types, make_query):
    query = make_query(sql)
    result = None
    if query.main_query.where:
        where_type = get_type(query.main_query.where, query.main_query.referenced_tables)[0]
        result = where_type.type.value
    assert result == expected_types
