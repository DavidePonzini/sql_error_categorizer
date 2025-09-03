# automatic_categorisation_of_sql_errors
Master's Thesis 2024/25

# Create the virtual environment
```bash
python -m venv venv
```

# Activate the python venv
```bash
venv\Scripts\Activate.ps1
```

# Install the required packages
```bash
pip install -r requirements.txt
```

# Initialize the database from YAML files

```bash
python main.py your_file.yaml --init-db
```

# Run the tool to analyze SQL queries

```bash
python .\main.py queries/ur_file.yaml
```

# SQL Misconceptions TODO List

## SYN - Syntax Errors

- [ ] SYN_1_AMBIGUOUS_DATABASE_OBJECT_OMITTING_CORRELATION_NAMES (ID: 1)
- [ ] SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN (ID: 2)
- [ ] SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_FUNCTION (ID: 3)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_COLUMN (ID: 4)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_FUNCTION (ID: 5)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_PARAMETER (ID: 6)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT (ID: 7)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_INVALID_SCHEMA_NAME (ID: 8)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_MISSPELLINGS (ID: 9)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_SYNONYMS (ID: 10)
- [ ] SYN_2_UNDEFINED_DATABASE_OBJECT_OMITTING_QUOTES_AROUND_CHARACTER_DATA(ID: 11)
- [ ] SYN_3_DATA_TYPE_MISMATCH_FAILURE_TO_SPECIFY_COLUMN_NAME_TWICE (ID: 12)
- [ ] SYN_3_DATA_TYPE_MISMATCH (ID: 13)
- [ ] SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING (ID: 14)
- [ ] SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED (ID: 15)
- [ ] SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_GROUPING_ERROR_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN (ID: 16)
- [ ] SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY (ID: 17)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_FUNCTION_WITH_FUNCTION_PARAMETER (ID: 18)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_USING_WHERE_TWICE (ID: 19)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_FROM_CLAUSE (ID: 20)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_COMPARISON_WITH_NULL (ID: 21)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON (ID: 22)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_DATE_TIME_FIELD_OVERFLOW (ID: 23)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE (ID: 24)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_USING_AN_UNDEFINED_CORRELATION_NAME (ID: 25)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_TOO_MANY_COLUMNS_IN_SUBQUERY (ID: 26)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_TABLE_NAMES_WITH_COLUMN_NAMES (ID: 27)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_RESTRICTION_IN_SELECT_CLAUSE (ID: 28)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_PROJECTION_IN_WHERE_CLAUSE (ID: 29)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_ORDER_OF_KEYWORDS (ID: 30)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_LOGIC_OF_KEYWORDS (ID: 31)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_SYNTAX_OF_KEYWORDS (ID: 32)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_OMITTING_COMMAS (ID: 33)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_CURLY_SQUARE_OR_UNMATCHED_BRACKETS (ID: 34)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_IS_WHERE_NOT_APPLICABLE (ID: 35)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_NONSTANDARD_KEYWORDS_OR_STANDARD_KEYWORDS_IN_WRONG_CONTEXT (ID: 36)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_NONSTANDARD_OPERATORS (ID: 37)
- [ ] SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON (ID: 38)

## SEM - Semantic Errors

- [ ] SEM_1_INCONSISTENT_EXPRESSION_AND_INSTEAD_OF_OR (ID: 39)
- [ ] SEM_1_INCONSISTENT_EXPRESSION_TAUTOLOGICAL_OR_INCONSISTENT_EXPRESSION (ID: 40)
- [ ] SEM_1_INCONSISTENT_EXPRESSION_DISTINCT_IN_SUM_OR_AVG (ID: 41)
- [ ] SEM_1_INCONSISTENT_EXPRESSION_DISTINCT_THAT_MIGHT_REMOVE_IMPORTANT_DUPLICATES (ID: 42)
- [ ] SEM_1_INCONSISTENT_EXPRESSION_WILDCARDS_WITHOUT_LIKE (ID: 43)
- [ ] SEM_1_INCONSISTENT_EXPRESSION_INCORRECT_WILDCARD_USING_UNDERSCORE_INSTEAD_OF_PERCENT (ID: 44)
- [ ] SEM_1_INCONSISTENT_EXPRESSION_MIXING_A_GREATER_THAN_0_WITH_IS_NOT_NULL (ID: 45)
- [ ] SEM_2_INCONSISTENT_JOIN_NULL_IN_IN_ANY_ALL_SUBQUERY (ID: 46)
- [ ] SEM_2_INCONSISTENT_JOIN_JOIN_ON_INCORRECT_COLUMN (ID: 47)
- [ ] SEM_3_MISSING_JOIN_OMITTING_A_JOIN (ID: 48)
- [ ] SEM_4_DUPLICATE_ROWS_MANY_DUPLICATES (ID: 49)
- [ ] SEM_5_REDUNDANT_COLUMN_OUTPUT_CONSTANT_COLUMN_OUTPUT (ID: 50)
- [ ] SEM_5_REDUNDANT_COLUMN_OUTPUT_DUPLICATE_COLUMN_OUTPUT (ID: 51)

## LOG - Logic Errors

- [ ] LOG_1_OPERATOR_ERROR_OR_INSTEAD_OF_AND (ID: 52)
- [ ] LOG_1_OPERATOR_ERROR_EXTRANEOUS_NOT_OPERATOR (ID: 53)
- [ ] LOG_1_OPERATOR_ERROR_MISSING_NOT_OPERATOR (ID: 54)
- [ ] LOG_1_OPERATOR_ERROR_SUBSTITUTING_EXISTENCE_NEGATION_WITH_NOT_EQUAL_TO (ID: 55)
- [ ] LOG_1_OPERATOR_ERROR_PUTTING_NOT_IN_FRONT_OF_INCORRECT_IN_OR_EXISTS (ID: 56)
- [ ] LOG_1_OPERATOR_ERROR_INCORRECT_COMPARISON_OPERATOR_OR_VALUE (ID: 57)
- [ ] LOG_2_JOIN_ERROR_JOIN_ON_INCORRECT_TABLE (ID: 58)
- [ ] LOG_2_JOIN_ERROR_JOIN_WHEN_JOIN_NEEDS_TO_BE_OMITTED (ID: 59)
- [ ] LOG_2_JOIN_ERROR_JOIN_ON_INCORRECT_COLUMN_MATCHES_POSSIBLE (ID: 60)
- [ ] LOG_2_JOIN_ERROR_JOIN_WITH_INCORRECT_COMPARISON_OPERATOR (ID: 61)
- [ ] LOG_2_JOIN_ERROR_MISSING_JOIN (ID: 62)
- [ ] LOG_3_NESTING_ERROR_IMPROPER_NESTING_OF_EXPRESSIONS (ID: 63)
- [ ] LOG_3_NESTING_ERROR_IMPROPER_NESTING_OF_SUBQUERIES (ID: 64)
- [ ] LOG_4_EXPRESSION_ERROR_EXTRANEOUS_QUOTES (ID: 65)
- [ ] LOG_4_EXPRESSION_ERROR_MISSING_EXPRESSION (ID: 66)
- [ ] LOG_4_EXPRESSION_ERROR_EXPRESSION_ON_INCORRECT_COLUMN (ID: 67)
- [ ] LOG_4_EXPRESSION_ERROR_EXTRANEOUS_EXPRESSION (ID: 68)
- [ ] LOG_4_EXPRESSION_ERROR_EXPRESSION_IN_INCORRECT_CLAUSE (ID: 69)
- [ ] LOG_5_PROJECTION_ERROR_EXTRANEOUS_COLUMN_IN_SELECT (ID: 70)
- [ ] LOG_5_PROJECTION_ERROR_MISSING_COLUMN_FROM_SELECT (ID: 71)
- [ ] LOG_5_PROJECTION_ERROR_MISSING_DISTINCT_FROM_SELECT (ID: 72)
- [ ] LOG_5_PROJECTION_ERROR_MISSING_AS_FROM_SELECT (ID: 73)
- [ ] LOG_5_PROJECTION_ERROR_MISSING_COLUMN_FROM_ORDER_BY (ID: 74)
- [ ] LOG_5_PROJECTION_ERROR_INCORRECT_COLUMN_IN_ORDER_BY (ID: 75)
- [ ] LOG_5_PROJECTION_ERROR_EXTRANEOUS_ORDER_BY_CLAUSE (ID: 76)
- [ ] LOG_5_PROJECTION_ERROR_INCORRECT_ORDERING_OF_ROWS (ID: 77)
- [ ] LOG_6_FUNCTION_ERROR_DISTINCT_AS_FUNCTION_PARAMETER_WHERE_NOT_APPLICABLE (ID: 78)
- [ ] LOG_6_FUNCTION_ERROR_MISSING_DISTINCT_FROM_FUNCTION_PARAMETER (ID: 79)
- [ ] LOG_6_FUNCTION_ERROR_INCORRECT_FUNCTION (ID: 80)
- [ ] LOG_6_FUNCTION_ERROR_INCORRECT_COLUMN_AS_FUNCTION_PARAMETER (ID: 81)

## COM - Complication Errors

- [ ] COM_1_COMPLICATION_UNNECESSARY_COMPLICATION (ID: 82)
- [ ] COM_1_COMPLICATION_UNNECESSARY_DISTINCT_IN_SELECT_CLAUSE (ID: 83)
- [ ] COM_1_COMPLICATION_UNNECESSARY_JOIN (ID: 84)
- [ ] COM_1_COMPLICATION_UNUSED_CORRELATION_NAME (ID: 85)
- [ ] COM_1_COMPLICATION_CORRELATION_NAMES_ARE_ALWAYS_IDENTICAL (ID: 86)
- [ ] COM_1_COMPLICATION_UNNECESSARILY_GENERAL_COMPARISON_OPERATOR (ID: 87)
- [ ] COM_1_COMPLICATION_LIKE_WITHOUT_WILDCARDS (ID: 88)
- [ ] COM_1_COMPLICATION_UNNECESSARILY_COMPLICATED_SELECT_IN_EXISTS_SUBQUERY (ID: 89)
- [ ] COM_1_COMPLICATION_IN_EXISTS_CAN_BE_REPLACED_BY_COMPARISON (ID: 90)
- [ ] COM_1_COMPLICATION_UNNECESSARY_AGGREGATE_FUNCTION (ID: 91)
- [ ] COM_1_COMPLICATION_UNNECESSARY_DISTINCT_IN_AGGREGATE_FUNCTION (ID: 92)
- [ ] COM_1_COMPLICATION_UNNECESSARY_ARGUMENT_OF_COUNT (ID: 93)
- [ ] COM_1_COMPLICATION_UNNECESSARY_GROUP_BY_IN_EXISTS_SUBQUERY (ID: 94)
- [ ] COM_1_COMPLICATION_GROUP_BY_WITH_SINGLETON_GROUPS (ID: 95)
- [ ] COM_1_COMPLICATION_GROUP_BY_WITH_ONLY_A_SINGLE_GROUP (ID: 96)
- [ ] COM_1_COMPLICATION_GROUP_BY_CAN_BE_REPLACED_WITH_DISTINCT (ID: 97)
- [ ] COM_1_COMPLICATION_UNION_CAN_BE_REPLACED_BY_OR (ID: 98)
- [ ] COM_1_COMPLICATION_UNNECESSARY_COLUMN_IN_ORDER_BY_CLAUSE (ID: 99)
- [ ] COM_1_COMPLICATION_ORDER_BY_IN_SUBQUERY (ID: 100)
- [ ] COM_1_COMPLICATION_INEFFICIENT_HAVING (ID: 101)
- [ ] COM_1_COMPLICATION_INEFFICIENT_UNION (ID: 102)
- [ ] COM_1_COMPLICATION_CONDITION_IN_SUBQUERY_CAN_BE_MOVED_UP (ID: 103)
- [ ] COM_1_COMPLICATION_CONDITION_ON_LEFT_TABLE_IN_LEFT_OUTER_JOIN (ID: 104)
- [ ] COM_1_COMPLICATION_OUTER_JOIN_CAN_BE_REPLACED_BY_INNER_JOIN (ID: 105)
- [ ] COM_X_COMPLICATION_JOIN_CONDITION_IN_WHERE_CLAUSE (ID: 106)


# SEMANTIC ERRORs
## SEM-1 Inconsistent expression
- AND instead of OR (empty result table) 
- implied, tautological or inconsistent expression -> The WHERE-condition is unnecessarily complicated if a subcondition (some node in the operator tree) can be replaced by TRUE or FALSE and the condition is still equivalent. E.g. it happens sometimes that a condition is tested under WHERE that is actually a constraint on the relation.
	This condition becomes even more strict if it is applied not to the given formula, but to the DNF of the formula. Then the check for unnecessary logical complications can be easily reduced to a series of consistency
	tests. Let the DNF of the WHERE condition be C1 v ... v Cm with Ci = (Ai,1 ^ ... ^ Ai,ni).
	There are really six cases to consider:
	1. The entire formula C1 v ... v Cm can be replaced by FALSE, i.e. it is inconsistent.
	2. The entire formula C1 v ... v Cm can be replaced by TRUE, , i.e. it is a tautology.
	3. One of the Ci can be replaced by FALSE, i.e. removed from the disjunction. For example, SAL > 500 OR SAL > 700 is equivalent to SAL > 500.
	4. One of the Ci can be replaced by TRUE. In this case, the entire disjunction can be replaced by TRUE
	5. One of the Ai,j can be replaced by FALSE. In this case, the entire conjunction Ci = Ai,1 ^ ... ^ Ai,ni can be replace by FALSE.
	6. One of the Ai,j can be replaced by TRUE, i.e. it can be removed from the conjunction. For instance, consider (SAL < 500 AND COMM > 1000) OR SAL >= 500. This can be simplified to ‘‘COMM > 1000 OR SAL >= 500’’, i.e. the underlined condition can be replaced by TRUE.
	
- DISTINCT in SUM or AVG
- DISTINCT that might remove important duplicates -> Conversely, applying DISTINCT or GROUP BY sometimes removes important duplicates. Consider the following query:  SELECT DISTINCT M.ENAME FROM EMP M, EMP E WHERE E.MGR = M.EMPNO AND E.JOB = 'ANALYST'; The purpose of this query seems to find employees who have an analyst in their team. If it should ever happen that two different employees with the same name satisfy this condition, only one name is printed. Since the intention is to find employee objects, this result is at least misleading. We suggest to give a warning whenever a soft key of a tuple variable appears under SELECT DISTINCT or GROUP BY, but not the corresponding real key. This could also be seen as a violation of a standard pattern.
- wildcards without LIKE -> When ‘‘=’’ is used with a comparison string that contains ‘‘%’’, probably ‘‘LIKE’’ was meant. For the other wildcard, ‘‘_’’, it is not that clear, because it might more often appear in normal strings.
- incorrect wildcard: using _ instead of % or using, e.g., *
- mixing a >0 with IS NOT NULL or empty string with NULL


## SEM-2 Inconsistent join
- NULL in IN/ANY/ALL subquery -> IN conditions and quantified comparisons with subqueries (ANY, ALL) can have a possibly surprising behaviour if the subquery returns a null value (among other values). An IN and ANY condition is then null/ unknown, when it otherwise would be false, and an ALL condition is null/unknown, when it otherwise would be true (ALL is treated like a large conjunction, and IN/ ANY like a large disjunction). For instance, the result of the following query is always empty as there exists one employee who is not a subordinate (MGR IS NULL for the president of the company): SELECT X.EMPNO, X.ENAME, X.JOB FROM EMP X WHERE X.EMPNO NOT IN (SELECT Y.MGR FROM EMP Y)
This is a trap that many SQL programmers are not aware of. Indeed, it is quite common to assume that the above query is equivalent to SELECT X.EMPNO, X.ENAME, X.JOB FROM EMP X WHERE NOT EXISTS (SELECT * FROM EMP Y WHERE Y.MGR = X.EMPNO)
This equivalence does not hold: The NOT EXISTS-query would work as intended. A warning should be printed whenever there is a database state in which the result of such a subquery includes a null value. We could also heuristically assume that if a column is not declared as NOT NULL, then every typical database state contains at least one row in which it is null. Now a query would be strange if it returns an empty result for all typical database states. (Of course, it might be that the programmer searches for untypical states, but then he/she could use a comment to switch off the warning).

- join on incorrect column (matches impossible) -> SELECT in subquery uses no tuple variable of subquery or Comparison between different domains,  If there is no domain information, one could analyze an example database state for columns that are nearly disjoint. If that is not possible, at least comparisons between strings and numbers of different size (e.g., a VARCHAR(10) column and a VARCHAR(20) column) are suspicious.


## SEM-3 Missing join
- omitting a join -> to test it, First, the conditions is converted to DNF, and the test is done for each conjunction separately. One creates a graph with the tuple variables X as nodes. Edges are drawn between tuple variables for which a foreign key is equated to a key, except in the case of self-joins, where any equation suffices. The graph then should be connected, with the possible exception of nodes X such that there is a condition X.A = c with a key attribute A and a constant c. Another exception are tuple variables over relations that can contain only a single tuple. Unfortunately, standard SQL does not permit to declare this constraint (it would be a key with 0 columns). Note that in some cases, joins can also be done via subqueries. Thus, tuple variables in subqueries should be added to the graph.


## SEM-4 Duplicate rows
- many duplicates -> run the query on an example database state. If it produces a lot of duplicates, we could give a warning.

## SEM-5 Redundant column output
- constant column output -> The conjunctive normal form of the WHERE clause contains A = c with an attribute A and a constant c as one part of the conjunction, and A also appears as a term in the SELECT-list. Of course, if A = B appears in addition to A = c, also the value of B is obvious, and using B in the SELECT-list should generate a warning. And so on.
- duplicate column output -> An output column is also unnecessary if it is always identical to another output column.