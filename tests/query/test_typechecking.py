import pytest
from sql_error_categorizer.query.typechecking import collect_errors, determinate_type

def test_primitive_types(make_query):
    sql = "SELECT 'hello' AS str_col, 123 AS num_col, TRUE AS bool_col, NULL AS null_col, DATE '2020-01-01' AS date_col;"
    query = make_query(sql)
    result = []
    for exp in query.main_query.ast.expressions:
        col_type = determinate_type(exp, query.main_query.referenced_tables)
        result.append(col_type.data_type.value.lower())
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
    messages = collect_errors(query.main_query.ast.expressions[0], query.main_query.referenced_tables)
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

# # functions
# def test_function_types(make_query):
#     sql = "SELECT COUNT(DISTINCT sname), AVG(sid), SUM(sid), MIN(sname), MAX(sid), CONCAT(NULL,NULL,1), CONCAT(NULL) FROM store;"
#     query = make_query(sql)

#     result = []
#     for col in query.main_query.output.columns:
#         result.append(col.column_type)

#     assert result == ['number', 'number', 'number', 'string', 'number', 'string', 'null']

# # logical operators
# @pytest.mark.parametrize('sql, expected_types', [
#     ("SELECT sname FROM store WHERE sname LIKE 'C%';", 'boolean'),
#     ("SELECT sname FROM store WHERE (sname LIKE 'C%') IS FALSE;", 'boolean'),
#     ("SELECT sname FROM store WHERE sid IS NOT NULL;", 'boolean'),
#     ("SELECT sname FROM store WHERE sid BETWEEN 1 AND 10;", 'boolean'),
#     ("SELECT sname FROM store WHERE (sname, sid) BETWEEN ('A', 5) AND ('B',7);", 'boolean')
# ])
# def test_logical_operator(sql, expected_types, make_query):
#     query = make_query(sql)
#     result = None
#     if query.main_query.where:
#         where_type = get_type(query.main_query.where, query.main_query.referenced_tables)
#         result = where_type.name
#     assert result == expected_types

# @pytest.mark.parametrize('sql, expected_error', [
#     ("SELECT sname FROM store WHERE sname LIKE 5;", "sname LIKE 5"),
#     ("SELECT sname FROM store WHERE sid IS TRUE;", "sid IS TRUE"),
#     ("SELECT sname FROM store WHERE sid BETWEEN 'A' AND 'Z';", "sid BETWEEN 'A' AND 'Z'")
# ])
# def test_logical_operator_errors(sql, expected_error, make_query):
#     query = make_query(sql)
#     error_message = ""
#     if query.main_query.where:
#         where_type = get_type(query.main_query.where, query.main_query.referenced_tables)
#         if where_type == ErrorType:
#             error_message += where_type.message.split(': ').pop()
#     assert error_message == expected_error
