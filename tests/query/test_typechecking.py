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

    typed_ast = rewrite_expression(query.main_query.ast, query.main_query.referenced_tables)
    messages = collect_errors(typed_ast)
    assert messages == [("Unknown column type", "unknown_col")]

# TODO: refactor above tests to comply with the new structure
# @pytest.mark.parametrize('sql, expected_types', [
#     ("SELECT 1 + (2 - '4') AS sum_col;", [('number', True, False)]),
#     ("SELECT s.sid FROM store s WHERE s.sid > '3';", [('number', False, False), ('boolean', True, False)]),
#     ("SELECT sname FROM transaction,store WHERE date > '11-05-2020' AND price < (1-0.5) AND store.sid = transaction.sid;", [('string', False, False), ('boolean', True, False)])

# ])
# def test_expression_types(sql, expected_types, make_query):
#     query = make_query(sql)
#     result = []
#     for exp in query.main_query.ast.expressions:
#         col_type = get_type(exp, query.main_query.referenced_tables)
#         result.append((col_type.name, col_type.constant, col_type.nullable))
#     if query.main_query.where:
#         where_type = get_type(query.main_query.where, query.main_query.referenced_tables)
#         result.append((where_type.name, where_type.constant, where_type.nullable))
#     assert result == expected_types

# @pytest.mark.parametrize('sql, expected_error', [
#     ("SELECT 1 + TRUE AS invalid_sum;", "1 + TRUE"),
#     ("SELECT sname FROM store WHERE sname > 5;", "sname > 5"),
#     ("SELECT MIN(TRUE) FROM store;", "MIN(TRUE)"),
#     ("SELECT MAX(sname > 'A') FROM store;", "MAX(sname > 'A')")
# ])
# def test_expression_type_errors(sql, expected_error, make_query):
#     query = make_query(sql)
#     error_message = ""
#     for exp in query.main_query.ast.expressions:
#         col_type = get_type(exp, query.main_query.referenced_tables)
#         if col_type == ErrorType:
#             error_message += col_type.message.split(': ').pop()
#     if query.main_query.where:
#         where_type = get_type(query.main_query.where, query.main_query.referenced_tables)
#         if where_type == ErrorType:
#             error_message += where_type.message.split(': ').pop()
#     assert error_message == expected_error

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
    typed_ast_where = rewrite_expression(query.main_query.ast, query.main_query.referenced_tables).args.get('where').this
    result = None
    if typed_ast_where:
        where_type = get_type(typed_ast_where)
        result = where_type.messages
    assert result == expected_types

@pytest.mark.parametrize('sql, expected_errors', [
    ("SELECT sname FROM store WHERE sname LIKE 5;", ["Invalid right operand type on LIKE operation"]),
    ("SELECT sname FROM store WHERE sid IS TRUE;", ["Invalid left operand type on IS operation with BOOLEAN"]),
    ("SELECT sname FROM store WHERE sid BETWEEN 'A' AND 'Z';", ["Invalid low bound type on BETWEEN operation","Invalid high bound type on BETWEEN operation"])
])
def test_logical_operator_errors(sql, expected_errors, make_query):
    query = make_query(sql)
    typed_ast_where = rewrite_expression(query.main_query.ast, query.main_query.referenced_tables).args.get('where').this
    result = collect_errors(typed_ast_where)
    found_messages = [msg for msg, _ in result]
    assert found_messages == expected_errors


@pytest.mark.parametrize('sql, expected_errors', [
    ("SELECT sname FROM store WHERE sid IN ('A', 'B', 2);", ["Invalid IN list item type"]*2),
    ("SELECT sname FROM store WHERE sid IN (1,2,3);", []),
    ("SELECT sname FROM store WHERE sid IN (SELECT 'a');", ["The argument type of the IN subquery must match the target type"]),
    ("SELECT sname FROM store WHERE sid IN (SELECT 1);", []),
    ("SELECT sname FROM store WHERE sid IN (SELECT 1,2);", ["The argument type of the IN subquery must match the target type"])
])
def test_in_operator_errors(sql, expected_errors, make_query):
    query = make_query(sql)
    typed_ast_where = rewrite_expression(query.main_query.ast, query.main_query.referenced_tables).args.get('where').this
    result = collect_errors(typed_ast_where)
    found_messages = [msg for msg, _ in result]
    assert found_messages == expected_errors
