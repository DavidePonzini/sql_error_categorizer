.. dav-tools documentation master file, created by
   sphinx-quickstart on Sun Jul 16 15:00:51 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to sql_error_categorizer's documentation!
=====================================
This project analyses SQL statements to highlight possible **misconceptions**
(common mistakes or misunderstandings). The detection engine tokenises the input
query and applies a set of rules. When a rule matches, it reports the type of
misconception together with the token or fragment that triggered it.

The logic is implemented in
`sql_query_analyzer/utils/misconception_detector.py` and the available
misconception identifiers are listed in
`sql_query_analyzer/utils/misconceptions.py`.

Below you will find a short explanation that anyone can follow, followed by a
section with technical details for developers.

Contents
========

.. toctree::
   :maxdepth: 4


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Installation
============
``$ pip install sql_error_categorizer``

What is checked right now?
==========================

At the moment the tool recognises the following issues:

- Missing `FROM` clause in a `SELECT` statement.
    - `SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_FROM_CLAUSE`
- Comparing a value with `NULL` using operators like `=` or `!=`.
    - `SYN_6_COMMON_SYNTAX_ERROR_COMPARISON_WITH_NULL`
- Omitting the final semicolon or adding extra semicolons.
    - `SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON`
    - `SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON`
- Ambiguous column references when multiple tables share the same column name.
    - `SYN_1_AMBIGUOUS_DATABASE_OBJECT_OMITTING_CORRELATION_NAMES`
- Duplicate `WHERE` clauses or repeated `FROM`, `GROUP`, `HAVING` or `ORDER` clauses.
    - `SYN_6_COMMON_SYNTAX_ERROR_USING_WHERE_TWICE`
    - `SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE`
- Using a column alias that has not been defined.
    - `SYN_6_COMMON_SYNTAX_ERROR_USING_AN_UNDEFINED_CORRELATION_NAME`
- Leaving out commas between expressions in the `SELECT` list.
    - `SYN_6_COMMON_SYNTAX_ERROR_OMITTING_COMMAS`
- Mismatched parentheses, brackets or braces.
    - `SYN_6_COMMON_SYNTAX_ERROR_CURLY_SQUARE_OR_UNMATCHED_BRACKETS`
- Non-standard operators such as `==`, `&&` or `||`.
    - `SYN_6_COMMON_SYNTAX_ERROR_NONSTANDARD_OPERATORS`
- Unqualified column names that exist in more than one table.
    - `SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN`
- References to tables or columns that do not exist in the provided catalogue.
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_COLUMN`
- Invalid schema names.
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_INVALID_SCHEMA_NAME`
- Text values without single quotes or with double quotes instead.
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_OMITTING_QUOTES_AROUND_CHARACTER_DATA`
- Unknown functions, parameters or general identifiers.
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_FUNCTION`
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_PARAMETER`
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT`
- Misspellings or use of synonyms for table/column names.
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_MISSPELLINGS`
    - `SYN_2_UNDEFINED_DATABASE_OBJECT_SYNONYMS`

Other misconception categories defined in `misconceptions.py` are placeholders
for future development.

Technical details
=================

Each detection rule scans the tokenised query and returns a list of
`(Misconceptions, token)` tuples. The list below summarises how each rule works
and where it is implemented.

- SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_FROM_CLAUSE
    Lines [81-84] of `misconception_detector.py` check if a SELECT statement lacks a
    FROM clause. If so, the token `'FROM'` is reported.

- SYN_6_COMMON_SYNTAX_ERROR_COMPARISON_WITH_NULL
    Lines [85-89] scan tokens for comparison operators followed by `NULL`. If found,
    the token `'NULL'` is returned.

- SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON
    Lines [91-93] verify that the original query text ends with a semicolon. Missing
    termination results in this misconception.

- SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON
    Lines [95-97] detect more than one semicolon in the query, flagging an
    additional semicolon error.

- SYN_1_AMBIGUOUS_DATABASE_OBJECT_OMITTING_CORRELATION_NAMES
    Lines [99-109] evaluate column names appearing in multiple tables without a
    correlation alias. The offending column name is reported when ambiguity arises.

- SYN_6_COMMON_SYNTAX_ERROR_USING_WHERE_TWICE
    Lines [112-130] count top-level WHERE clauses. If more than one occurs, the
    misconception is raised with the token `'WHERE'`.

- SYN_6_COMMON_SYNTAX_ERROR_DUPLICATE_CLAUSE
    The same loop also counts FROM, GROUP, HAVING, and ORDER clauses. When a clause
    appears twice, its name is used as the token (lines 112‑130).

- SYN_6_COMMON_SYNTAX_ERROR_USING_AN_UNDEFINED_CORRELATION_NAME
    Lines [132-136] check for `alias.column` references where the alias was never
    introduced. The unknown alias is returned.

- SYN_6_COMMON_SYNTAX_ERROR_OMITTING_COMMAS
    Lines [138-155] track the SELECT list, counting identifiers and commas. If more
    than one column appears without a comma, the comma token is reported.

- SYN_6_COMMON_SYNTAX_ERROR_CURLY_SQUARE_OR_UNMATCHED_BRACKETS
    Lines [157-172] parse the raw query string for mismatched parentheses or square/
    curly brackets. The unmatched character is returned.

- SYN_6_COMMON_SYNTAX_ERROR_NONSTANDARD_OPERATORS
    Lines [174-178] search for operators `==`, `&&` or `||` in the tokenised query.
    The offending operator is returned when found.

- SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN
    Lines [181-195] look for duplicate column names in the SELECT clause where no
    qualifier follows. The repeated column name triggers this misconception.

- SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_COLUMN
    Lines [198-211] validate CTE and subquery aliases against cataloged columns.
    Unknown columns referenced through an alias produce this error. The same rule
    is reused later for general identifiers (lines 237‑248).

- SYN_2_UNDEFINED_DATABASE_OBJECT_INVALID_SCHEMA_NAME
    Lines [213-219] identify identifiers containing a schema prefix. If the schema
    is not listed in the catalog, it is reported as invalid.

- SYN_2_UNDEFINED_DATABASE_OBJECT_OMITTING_QUOTES_AROUND_CHARACTER_DATA
    Lines [230-235] look for double-quoted identifiers that are not table or column
    names. Additionally, lines 249‑251 flag bare alphabetic tokens treated as string
    literals without quotes.

- SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_FUNCTION
    Lines [237-242] detect alphabetic tokens followed by `(` that are not known
    objects. These are assumed to be undefined functions.

- SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_PARAMETER
    Lines [243-244] catch identifiers starting with `:` `@` or `?`, reporting them as
    undefined parameters.

- SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT
    Lines [249-252] handle remaining unknown identifiers that do not match previous
    cases, labelling them as undefined objects.

- SYN_2_UNDEFINED_DATABASE_OBJECT_MISSPELLINGS
    Lines [254-264] compare every alphabetic token not found in the catalog against
    known names using `difflib.get_close_matches`. If a near match exists, the token
    is flagged as a misspelling.

- SYN_2_UNDEFINED_DATABASE_OBJECT_SYNONYMS
    Lines [266-269] check tokens against a `synonyms` list in the catalog and report
    usage when present.


Other misconception types listed in `misconceptions.py` have no detection logic yet and are reserved for future work.

