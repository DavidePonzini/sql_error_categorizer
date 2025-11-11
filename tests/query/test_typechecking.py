import pytest
from sql_error_categorizer.query.typechecking import collect_errors, get_type, rewrite_expression

def test_primitive_types(make_query):
    sql = "SELECT 'hello' AS str_col, 123 AS num_col, TRUE AS bool_col, NULL AS null_col, DATE '2020-01-01' AS date_col;"
    query = make_query(sql)
    result = []
    for col in query.main_query.output.columns:
        result.append(col.column_type.value.lower())
    assert result == ["varchar", "int", "boolean", "null", "date"]

def test_type_columns(make_query):
    sql = "SELECT * FROM store;"
    query = make_query(sql)
    result = []
    for col in query.main_query.output.columns:
        result.append(col.column_type.value.lower())
    
    assert result == ['decimal', 'varchar', 'varchar', 'varchar']

def test_wrong_column_reference(make_query):
    sql = "SELECT unknown_col FROM store;"
    query = make_query(sql)

    messages = collect_errors(query.main_query.typed_ast, query.catalog, query.search_path)
    assert messages == [("Unknown column type.", "unknown_col")]

@pytest.mark.parametrize('sql, expected_types', [
    ("SELECT 1 + (2 - '4') AS sum_col;", []),
    ("SELECT sid FROM store WHERE sid > '3';", []),
    ("SELECT sname FROM transaction,store WHERE date > '11-05-2020' AND price < (1-0.5) AND store.sid = transaction.sid;", [])

])
def test_expression_types(sql, expected_types, make_query):
    query = make_query(sql)
    messages = collect_errors(query.main_query.typed_ast, query.catalog, query.search_path)
    assert messages == expected_types

@pytest.mark.parametrize('sql, expected_error', [
    ("SELECT 1 + TRUE AS invalid_sum;", ["Invalid right operand type. Expected type numeric, but found type boolean."]),
    ("SELECT sname FROM store WHERE sname > 5;", ['Type mismatch in comparison.']),
    ("SELECT MIN(TRUE) FROM store;", ["Invalid MIN operand type."]),
    ("SELECT MAX(sname > 'A') FROM store;", ["Invalid MAX operand type."])
])
def test_expression_type_errors(sql, expected_error, make_query):
    query = make_query(sql)
    errors = collect_errors(query.main_query.typed_ast, query.catalog, query.search_path)
    collected_messages = [msg for msg, _ in errors]
    assert collected_messages == expected_error

# functions
def test_function_types(make_query):
    sql = "SELECT COUNT(DISTINCT sname), AVG(sid), SUM(sid), MIN(sname), MAX(sid), CONCAT(NULL,NULL,1), CONCAT(NULL) FROM store;"
    query = make_query(sql)

    result = []
    for col in query.main_query.output.columns:
        result.append(col.column_type.value.lower())

    assert result == ['bigint', 'double', 'decimal', 'varchar', 'decimal', 'varchar', 'null']

# logical operators
@pytest.mark.parametrize('sql, expected_types', [
    ("SELECT sname FROM store WHERE sname LIKE 'C%';", []),
    ("SELECT sname FROM store WHERE (sname LIKE 'C%') IS FALSE;", []),
    ("SELECT sname FROM store WHERE sid IS NOT NULL;", []),
    ("SELECT sname FROM store WHERE sid BETWEEN 1 AND 10;", []),
    ("SELECT sname FROM store WHERE (sname, sid) BETWEEN ('A', 5) AND ('B',7);", [])
])
def test_logical_operator(sql, expected_types, make_query):
    query = make_query(sql)
    typed_ast_where = query.main_query.typed_ast.args.get('where').this
    result = None
    if typed_ast_where:
        where_type = get_type(typed_ast_where, query.catalog, query.search_path)
        result = where_type.messages
    assert result == expected_types

@pytest.mark.parametrize('sql, expected_errors', [
    ("SELECT sname FROM store WHERE sname LIKE 5;", ["Invalid right operand type. Expected type string, but found type int."]),
    ("SELECT sname FROM store WHERE sid IS TRUE;", ["Invalid left operand type. Expected type boolean, but found type decimal."]),
    ("SELECT sname FROM store WHERE sid BETWEEN 'A' AND TRUE;", ["Invalid low bound type. Expected type decimal, but found type varchar.","Invalid high bound type. Expected type decimal, but found type boolean."])
])
def test_logical_operator_errors(sql, expected_errors, make_query):
    query = make_query(sql)
    typed_ast_where = query.main_query.typed_ast.args.get('where').this
    result = collect_errors(typed_ast_where, query.catalog, query.search_path)
    found_messages = [msg for msg, _ in result]
    assert found_messages == expected_errors


@pytest.mark.parametrize('sql, expected_errors', [
    ("SELECT sname FROM store WHERE sid IN ('A', 'B', 2);", ["Invalid item type. Expected type decimal, but found type varchar."]*2),
    ("SELECT sname FROM store WHERE sid IN (1,2,3);", []),
    ("SELECT sname FROM store WHERE sid IN (SELECT 'a');", ["Invalid right operand type. Expected type decimal, but found type varchar."]),
    ("SELECT sname FROM store WHERE sid IN (SELECT 1);", []),
    ("SELECT sname FROM store WHERE sid IN (SELECT 1,2);", ["Invalid right operand type. Expected type decimal, but found type list."]),
    ("SELECT sid FROM store WHERE sid IN (SELECT sid FROM transaction);", []),
    ("SELECT sid FROM store WHERE sid IN (SELECT sname FROM transaction);", ["Invalid right operand type. Expected type decimal, but found type varchar."])
])
def test_in_operator_errors(sql, expected_errors, make_query):
    query = make_query(sql)
    typed_ast_where = query.main_query.typed_ast.args.get('where').this
    result = collect_errors(typed_ast_where, query.catalog, query.search_path)
    found_messages = [msg for msg, _ in result]
    assert found_messages == expected_errors

# @pytest.mark.parametrize('sql, expected_errors', [
#     ("WITH t AS (SELECT * FROM store) SELECT sname FROM t WHERE sname LIKE 5;", ["Invalid right operand type. Expected type string, but found type int."])
# ])
# def test_complex_typechecking(sql, expected_errors, make_query):
#     query = make_query(sql)
#     result = collect_errors(query.main_query.typed_ast, query.catalog, query.search_path)
#     found_messages = [msg for msg, _ in result]
#     assert found_messages == expected_errors