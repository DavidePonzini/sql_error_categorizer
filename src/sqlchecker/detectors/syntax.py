'''Detector for syntax errors in SQL queries.'''

from dataclasses import dataclass
import difflib
import re
import sqlparse
from sqlglot import exp
from typing import Callable
from copy import deepcopy
from sqlerrors import SqlErrors
from sqlscope import Query, catalog
from sqlscope.query.set_operations.set_operation import SetOperation
from sqlscope.query.typechecking import get_type, collect_errors
from sqlscope import util

from .base import BaseDetector, DetectedError


class SyntaxErrorDetector(BaseDetector):
    '''Detector for syntax errors in SQL queries.'''

    def __init__(self,
                 *,
                 query: Query,
                 update_query: Callable[[str, str | None], None],
                 solutions: list[Query] = [],
                ):
        super().__init__(
            query=query,
            solutions=solutions,
            update_query=update_query,
        )

    def run(self) -> list[DetectedError]:
        '''Run the detector and return a list of detected errors with their descriptions'''
        results: list[DetectedError] = super().run()

        # 1) fix stray semicolons (to allow ast building for subsequent checks)
        checks = [self.detect_22_38_additional_omitted_semicolons]

        for check in checks:
            check_result, fixed_query_str = check()
            results.extend(check_result)

            if fixed_query_str != self.query.sql:
                self.update_query(fixed_query_str, check.__name__)

        # 2) detect unexisting objects (before corrections, to avoid false positives)
        unexisting_checks = [
            self.detect_2_4_undefined_columns_ambiguous_columns,    # ok
            self.detect_2_ambiguous_function,                       # TODO: implement
            self.detect_5_undefined_functions,                      # ok
            self.detect_6_undefined_function_parameters,            # ok
            self.detect_7_8_undefined_tables,                       # ok
            self.detect_25_using_an_undefined_correlation_name,     # TODO: implement
        ]

        for check in unexisting_checks:
            check_result = check()
            results.extend(check_result)

        # 3.1) detect fixable errors and apply corrections for improved subsequent checks
        # NOTE: leave in this order!
        misspelling_checks = [
            self.detect_33_omitting_commas,                         # TODO: implement/refactor
            self.detect_27_confusing_table_names_with_column_names, # TODO: implement
            self.detect_36_nonstandard_operators,                   # ok
            self.detect_9_misspellings_schemas_tables,              # ok
            self.detect_9_misspellings_columns,                     # ok
            self.detect_10_synonyms,                                # TODO: implement
            self.detect_11_omitted_quotes,                          # TODO: implement/refactor
        ]

        # 3.2) apply corrections and re-parse query
        corrected_sql = self.query.sql
        for check in misspelling_checks:
            for error in check():
                results.append(error)
                pattern = r'\b' + re.escape(error.data[0]) + r'\b'
                corrected_sql = re.sub(
                    pattern,
                    error.data[1],
                    corrected_sql,
                    # flags=re.IGNORECASE
                )

                # Use the corrected query from here on (across all detectors)
                if corrected_sql != self.query.sql:
                    self.update_query(corrected_sql, check.__name__)
            
        # Proceed with all other checks
        checks = [
            self.detect_12_failure_to_specify_column_name_twice,                        # TODO: implement
            self.detect_13_data_type_mismatch,                                          # ok
            self.detect_14_aggregate_function_outside_select_or_having,                 # ok
            self.detect_15_aggregate_functions_cannot_be_nested,                        # ok
            self.detect_16_extraneous_or_omitted_grouping_column,                       # ok
            self.detect_17_having_without_group_by,                                     # ok
            self.detect_106_missing_quantifier,                                         #TODO: implement
            self.detect_18_confusing_function_with_function_parameter,                  # TODO: implement
            self.detect_19_using_where_twice,                                           # ok
            self.detect_20_omitted_from_clause,                                         # ok
            self.detect_21_comparison_with_null,                                        # ok
            self.detect_23_date_time_field_overflow,                                    # TODO: implement, needs AST
            self.detect_24_duplicate_clause,                                            # ok
            self.detect_26_too_many_columns_in_subquery,                                # ok
            self.detect_30_confused_order_of_keywords,                                  # ok
            self.detect_32_confused_syntax_of_keywords,                                 # TODO: check and refactor
            self.detect_107_108_curly_square_or_unmatched_brackets,                     # ok
            self.detect_35_is_where_not_applicable,                                     # ok
            self.detect_36_nonstandard_keywords_or_standard_keywords_in_wrong_context,  #TODO: implement
            self.detect_109_different_tuples_in_set_operation,                          #TODO: implement
        ]
    
        for check in checks:
            results.extend(check())
        return results

    # region 1) Semicolons
    def detect_22_38_additional_omitted_semicolons(self) -> tuple[list[DetectedError], str]:
        '''
        Flags queries that omit the semicolon at the end or have multiple semicolons.

        Returns:
        - List of DetectedError instances for semicolon issues.
        - The cleaned query string with extra semicolons removed. If the query contains multiple nested queries, only the last semicolon is kept. If it contains multiple separate queries, only the first statement is kept.
        '''

        results: list[DetectedError] = []

        def is_semicolon(token):
            return token.ttype == sqlparse.tokens.Punctuation and token.value == ';'

        def is_ws(token):
            return token.ttype in (sqlparse.tokens.Whitespace, sqlparse.tokens.Newline)

        all_tokens = []
        for statement in self.query.all_statements:
            all_tokens.extend(list(statement.flatten()))
        
        good_tokens = []
        trailing_semicolon_found = False
        non_whitespace_found = False

        depth = 0
        
        for token in reversed(all_tokens):  # start from end to preserve only the last semicolon
            # check for whitespace/newline
            if is_ws(token):
                # keep as is and continue
                good_tokens.append(token)
                continue

            # compute query depth
            if token.ttype == sqlparse.tokens.Punctuation and token.value in ('(', ')'):
                if token.value == '(':
                    depth -= 1
                else:
                    depth += 1

            # check for semicolons: the first one before any non-whitespace is kept, others are flagged
            if is_semicolon(token):
                if non_whitespace_found:
                    # we encountered a semicolon in the middle of the query!
                    # we don't care if this is the first one we encounter, it's surely not supposed to be here
                    results.append(DetectedError(SqlErrors.ADDITIONAL_SEMICOLON))

                    if depth == 0:
                        # possibly multiple statements in the same query, keep the ; to be safe
                        good_tokens.append(token)

                    continue
                
                if not trailing_semicolon_found:
                    # we encountered the trailing semicolon for the first time
                    # it's good, keep it
                    good_tokens.append(token)
                    trailing_semicolon_found = True
                    continue

                # else, we have already found the trailing semicolon, so this is an extra one at the end
                results.append(DetectedError(SqlErrors.ADDITIONAL_SEMICOLON))
                continue
            
            # any other token
            non_whitespace_found = True
            good_tokens.append(token)
                
        if not trailing_semicolon_found:
            results.append(DetectedError(SqlErrors.OMITTED_SEMICOLON))

        # if there are semicolons at the start of the query, before any actual tokens, we can safely discard them
        while(len(good_tokens) > 0 and is_semicolon(good_tokens[-1]) or is_ws(good_tokens[-1])):
            good_tokens.pop()

        return (results, ''.join(reversed([token.value for token in good_tokens])))
    # endregion

    # region 2) Pre-fixing
    def detect_2_ambiguous_function(self) -> list[DetectedError]:
        return []

    def detect_7_8_undefined_tables(self) -> list[DetectedError]:
        '''
        Checks for undefined tables in the FROM clause
        '''
        
        results: list[DetectedError] = []

        for select in self.query.selects:
            select_stripped = select.strip_subqueries()

            if select_stripped.ast is None:
                continue

            for table in select_stripped.ast.find_all(exp.Table):
                table_name = util.ast.table.get_real_name(table)
                schema_name = util.ast.table.get_schema(table)

                if table_name.startswith('__subq'):  # skip subqueries, they are not real tables
                    continue

                if schema_name:
                    # Fully qualified table (schema.table)
                    if not select.catalog.lookup_schema(schema_name):
                        results.append(DetectedError(SqlErrors.INVALID_SCHEMA_NAME, (table.sql(),)))
                        continue

                    if not select.catalog.has_table(schema_name, table_name):
                        results.append(DetectedError(SqlErrors.UNDEFINED_OBJECT, (table.sql(),)))
                        continue
                else:
                    # Unqualified table (table)
                    # Check if table is a CTE
                    if select.catalog.has_table('', table_name):
                        continue

                    # Check if table is in the current schema
                    if select.catalog.has_table(select.search_path, table_name):
                        continue

                    results.append(DetectedError(SqlErrors.UNDEFINED_OBJECT, (table.sql(),)))

        return results

    def detect_2_4_undefined_columns_ambiguous_columns(self) -> list[DetectedError]:
        '''
        Checks for undefined and ambiguous columns.
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:
            select_stripped = select.strip_subqueries()

            if select_stripped.ast is None:
                continue

            natural_join_equalities = select.get_natural_join_equalities()

            for column in select_stripped.ast.find_all(exp.Column):
                # skip `table.*` syntax, we only want to check actual column references
                if isinstance(column.this, exp.Star):
                    continue

                column_name = util.ast.column.get_name(column)
                table_name = util.ast.column.get_table(column)

                # Determine the parent clause of the column (FROM, ORDER BY, etc.) to handle special cases
                parent_clause = None
                aux = column
                while aux.parent:
                    if isinstance(aux.parent, exp.Ordered):
                        parent_clause = 'ORDER BY'
                        break
                    if isinstance(aux.parent, exp.Join):
                        parent_clause = 'JOIN'
                        break
                    aux = aux.parent

                possible_matches: list[str] = []

                if table_name:
                    # Qualified column (table.column)
                    for table in select.referenced_tables:
                        if table.name != table_name:
                            continue

                        for possible_match in table.columns:
                            if possible_match.name == column_name:
                                possible_matches.append(f'{table_name}.{column_name}')
                else:
                    # Unqualified column (column)
                    for table in select.referenced_tables:
                        for possible_match in table.columns:
                            if possible_match.name == column_name:
                                possible_matches.append(f'{table.name}.{column_name}')

                # If the column has no table name and is part of ORDER BY and it appears in SELECT clause,
                #   we can only check for ambiguity in the SELECT clause
                # If it doesn't appear in SELECT clause, we keep checking for ambiguity in the whole query
                if parent_clause == 'ORDER BY' and table_name is None:
                    select_columns = [col.name for col in select.output.columns]

                    if column_name in select_columns:
                        possible_matches = [m for m in select_columns if m == column_name]

                if len(possible_matches) == 0:
                    results.append(DetectedError(SqlErrors.UNDEFINED_COLUMN, (column.sql(),)))
                    continue

                # If the select is a subquery, we can discard columns defined in the parent, since the subquery tables have precedence over them
                if select_stripped.parent_query is not None and table_name is None:
                    # unqualified match in a subquery, we can ignore matches that belong to the parent query, since subquery tables have precedence over them
                    parent_columns = set()
                    for parent_select in select_stripped.parent_query.strip(select.sql, ' __subq ').selects:
                        for parent_table in parent_select.referenced_tables:
                            for parent_column in parent_table.columns:
                                parent_columns.add(f'{parent_table.name}.{parent_column.name}')

                    possible_matches = [m for m in possible_matches if m not in parent_columns]

                # If the column is part of a JOIN clause, we can discard matches that belong to tables that are not yet joined, since they are not visible in the current context
                if parent_clause == 'JOIN':
                    # compute this again to avoid potentially unbound warnings
                    aux = column
                    join_left_tables: list[catalog.Table] = []
                    while aux.parent:
                        if isinstance(aux.parent, exp.Join):
                            join_table_name = util.ast.table.get_name(aux.parent.this)
                            join_left_tables = select.get_left_tables(join_table_name)
                            break
                        aux = aux.parent

                    # Remove tables not yet joined
                    possible_matches = [m for m in possible_matches if any(m.startswith(f'{t.name}.') for t in join_left_tables)]

                # Remove equal matches from NATURAL JOINs, since they are not ambiguous
                if column_name in natural_join_equalities:
                    table_names = [select.referenced_tables[i].name for i in natural_join_equalities[column_name]]

                    if len(table_names) > 1:
                        # only keep matches that are not part of the natural join, since those are not ambiguous
                        possible_matches = [m for m in possible_matches if not any(m.startswith(f'{t}.') for t in table_names)]

                        # add a dummy match for the natural join, to make the comparison below work correctly
                        possible_matches.append(f'NATURAL JOIN({",".join(table_names)}).{column_name}')

                if len(possible_matches) > 1:
                    results.append(DetectedError(SqlErrors.AMBIGUOUS_COLUMN, (column.sql(), possible_matches)))

        return results

    def detect_5_undefined_functions(self) -> list[DetectedError]:
        '''Checks for undefined functions (i.e. invalid names followed by parentheses).'''

        results: list[DetectedError] = []

        # standard_functions = {
        known_aggregate_functions = {
            'SUM', 'AVG', 'COUNT', 'MIN', 'MAX',
            'IN', 'EXISTS', 'ANY', 'ALL',
            'FILTER', 'EXTRACT', 'DATE_TRUNC', 'DATE_PART',
            'COALESCE', 'NULLIF', 'CAST', 'CONVERT',
            'UPPER', 'LOWER', 'LENGTH', 'SUBSTRING',
            'NOW', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
        }
        user_defined_functions = set() # TODO: self.catalog.functions

        all_functions = known_aggregate_functions.union(user_defined_functions)

        for func, clause in self.query.functions:
            func_name = func.get_name()
            
            if func_name is None:
                continue
            
            if func_name.upper() not in all_functions:
                results.append(DetectedError(SqlErrors.UNDEFINED_FUNCTION, (func_name, clause)))

        return results

    def detect_6_undefined_function_parameters(self) -> list[DetectedError]:
        '''Checks for undefined function parameters'''

        results: list[DetectedError] = []

        for token, val in self.query.tokens:
            if any(val.startswith(p) for p in (':', '@', '?')) and val != '::':
                results.append(DetectedError(SqlErrors.UNDEFINED_PARAMETER, (val,)))

        return results
    
    def detect_25_using_an_undefined_correlation_name(self) -> list[DetectedError]:
        return []
    # endregion

    # region 3) Fixable errors
    def detect_9_misspellings_schemas_tables(self) -> list[DetectedError]:
        '''
        Check for misspellings in table names.
        '''

        results: set[DetectedError] = set()     # use a set to avoid applying the same correction multiple times

        for select in self.query.selects:
            select_stripped = select.strip_subqueries()

            if select_stripped.ast is None:
                continue

            for table in select_stripped.ast.find_all(exp.Table):
                table = deepcopy(table)  # avoid modifying the original AST until we are sure we want to apply the correction
                table_str = table.sql()
                table_name = util.ast.table.get_real_name(table)
                schema_name = util.ast.table.get_schema(table)

                if schema_name:
                    # Fully qualified table (schema.table)
                    if select.catalog.has_table(schema_name, table_name):
                        continue

                    # check "schema.table" for more accurate matches in edge cases (i.e. can't determine if the misspelled part is schema or table)
                    available_tables = {f'{s}.{t}' for s in select.catalog.schema_names for t in select.catalog.get_schema(s).table_names}
                    match = difflib.get_close_matches(f'{schema_name}.{table_name}', available_tables, n=1, cutoff=0.6)
                    if match:
                        s, t = match[0].split('.')

                        table.set('db', exp.TableAlias(this=exp.to_identifier(s, quoted=True)))
                        table.set('this', exp.to_identifier(t, quoted=True))
                        
                        results.add(DetectedError(SqlErrors.MISSPELLINGS, (table_str, table.sql())))
                    continue
                
                else:
                    # Unqualified table (table)
                    # Check if table is a CTE
                    if select.catalog.has_table('', table_name):
                        continue

                    # Check if table is in the current schema
                    if select.catalog.has_table(select.search_path, table_name):
                        continue

                    available_tables = {t for s in select.catalog.schema_names for t in select.catalog.get_schema(s).table_names}
                    match = difflib.get_close_matches(table_name, available_tables, n=1, cutoff=0.6)
                    if match:
                        db = next(s for s in select.catalog.schema_names if select.catalog.has_table(s, match[0]))
                        table.set('this', exp.to_identifier(match[0], quoted=True))
                        if db != select.search_path:
                            table.set('db', exp.TableAlias(this=exp.to_identifier(db, quoted=True)))
                        results.add(DetectedError(SqlErrors.MISSPELLINGS, (table_str, table.sql())))

        return [*results]     

    def detect_9_misspellings_columns(self) -> list[DetectedError]:
        '''
            Check for misspellings in table and column names.
            Performs two passes: first try to match objects to their own type, then try to match to any type.
        '''
        results: set[DetectedError] = set()    # use a set to avoid applying the same correction multiple times

        for select in self.query.selects:
            select_stripped = select.strip_subqueries()

            if select_stripped.ast is None:
                continue

            for column in select_stripped.ast.find_all(exp.Column):
                # skip `table.*` syntax, we only want to check actual column references
                if isinstance(column.this, exp.Star):
                    continue

                column = deepcopy(column)  # avoid modifying the original AST until we are sure we want to apply the correction
                column_str = column.sql()
                column_name = util.ast.column.get_name(column)
                table_name = util.ast.column.get_table(column)

                found = False

                for table in select.referenced_tables:
                    if table_name and table.name != table_name:
                        # Qualified column (table.column)
                        # check if column exists only in the specified table
                        continue
                    if table.has_column(column_name):
                        found = True
                        break

                if found:
                    continue

                if table_name:
                    # Qualified column (table.column)
                    available_columns = {f'{t.name}.{c.name}' for t in select.referenced_tables for c in t.columns}
                else:
                    # Unqualified column (column)
                    available_columns = {c.name for t in select.referenced_tables for c in t.columns}

                match = difflib.get_close_matches(column_name if not table_name else f'{table_name}.{column_name}', available_columns, n=1, cutoff=0.6)
                if match:
                    if table_name:
                        matched_table, matched_column = match[0].split('.')
                        column.set('table', exp.to_identifier(matched_table, quoted=True))
                        column.set('this', exp.to_identifier(matched_column, quoted=True))
                    else:
                        column.set('this', exp.to_identifier(match[0], quoted=True))
                    
                    results.add(DetectedError(SqlErrors.MISSPELLINGS, (column_str, column.sql())))

        return [*results]
    
    def detect_10_synonyms(self) -> list[DetectedError]:
        return []

    def detect_11_omitted_quotes(self) -> list[DetectedError]:
        '''
        Checks for potential omitting of quotes around character data in WHERE/HAVING clauses.
        
        Returns:
        A list of DetectedErrors. data=(offending_value,corrected_value)
        '''
        return []

        results: list[DetectedError] = []

        

        comparisons = self.query.comparisons


        # for comparison in comparisons:


        return results

        # # 3. Build sets of ALL known identifiers for the entire query (main + subqueries + CTEs)
        # valid_source_identifiers = set()
        # all_known_columns_lower = set()
        # db_tables = self.catalog.get('table_columns', {})

        # # -- Main Query --
        # main_query_sources = self._get_referenced_tables()
        # main_alias_map = self.query_map.alias_mapping
        # valid_source_identifiers.update(s.lower() for s in main_query_sources)
        # valid_source_identifiers.update(a.lower() for a in main_alias_map.keys())
        # for source in main_query_sources:
        #     actual_base_name = next((k for k in db_tables if k.lower() == source.lower()), None)
        #     if actual_base_name:
        #         all_known_columns_lower.update(c.lower() for c in db_tables[actual_base_name])

        # # -- Subqueries --
        # for subq_map in self.subquery_map.values():
        #     sub_sources = []
        #     sub_from = subq_map.from_value
        #     if sub_from:
        #         sub_sources.append(sub_from)
        #     sub_joins = subq_map.join_value
        #     sub_sources.extend(sub_joins)
        #     sub_aliases = subq_map.alias_mapping
        #     valid_source_identifiers.update(s.lower() for s in sub_sources)
        #     valid_source_identifiers.update(a.lower() for a in sub_aliases.keys())
        #     for source in sub_sources:
        #         actual_base_name = next((k for k in db_tables if k.lower() == source.lower()), None)
        #         if actual_base_name:
        #             all_known_columns_lower.update(c.lower() for c in db_tables[actual_base_name])
        
        # # -- CTEs --
        # if self.cte_map:
        #     valid_source_identifiers.update(name.lower() for name in self.cte_map.keys())
        #     for cte_name, cte_columns in self.cte_catalog.cte_tables.items():
        #         all_known_columns_lower.update(c.lower() for c in cte_columns)


                # 4. Main Token-based Check
        is_where_or_having = False
        is_rhs_of_comparison = False    #   nothing prevents an expression to have its sides inverted, although it's unlikely to happen
        comparison_operators = {'=', '<>', '!=', '<', '>', '<=', '>=', 'LIKE', 'NOT LIKE'}
        known_keywords = {'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'AS', 'DISTINCT'}

        for i, (tt, val) in enumerate(self.query.tokens):
            if tt == sqlparse.tokens.Keyword and val.upper() in {'WHERE', 'HAVING'}:
                is_where_or_having = True
            if tt == sqlparse.tokens.Error:
                continue
            if val in comparison_operators:
                is_rhs_of_comparison = True
                continue
            if tt in sqlparse.tokens.Literal or tt in (sqlparse.tokens.String.Single, sqlparse.tokens.String.Symbol):
                if is_where_or_having and is_rhs_of_comparison:
                    stripped_val = val.strip()
                    if stripped_val.startswith('"') and stripped_val.endswith('"'):
                        results.append(DetectedError(SqlErrors.SYN_11_OMITTING_QUOTES_AROUND_CHARACTER_DATA, (val,)))
                is_rhs_of_comparison = False
                continue
            if tt is not sqlparse.tokens.Name:
                is_rhs_of_comparison = False
                continue
            if val.upper() in known_keywords:
                is_rhs_of_comparison = False
                continue
            if val.lower() in valid_source_identifiers:
                is_rhs_of_comparison = False
                continue
            if val.lower() in output_aliases_lower:
                continue

            clean_val = val


            # if string OP notcol -> error
            # if date OP notcol2 -> error
            # if extract(notstring FROM ...) -> error
            # like notstring -> error
            
            # is this the correct approach? col OP notColumn
            # TODO: literal or string.single/string.symbol in RHS of WHERE/HAVING
            if is_where_or_having and is_rhs_of_comparison:
                if clean_val.isalpha() and clean_val.lower() not in all_known_columns_lower:
                    results.append(DetectedError(SqlErrors.SYN_11_OMITTING_QUOTES_AROUND_CHARACTER_DATA, (val,)))
                    is_rhs_of_comparison = False
                    continue
            
        return results
    
    def detect_27_confusing_table_names_with_column_names(self) -> list[DetectedError]:
        return []
    
    def detect_33_omitting_commas(self) -> list[DetectedError]:
        '''
        Flags queries where commas are likely missing between column expressions 
        (e.g., SELECT name age FROM ..., GROUP BY x y).
        '''
        return []

        results = []

        clause_starters = {
            "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "JOIN", "ON"
        }
        comma_required_clauses = {"SELECT", "GROUP BY", "ORDER BY", "VALUES"}
        current_clause = None
        in_clause_block = False

        tokens = self.tokens
        i = 0
        while i < len(tokens):
            tt, val = tokens[i]
            val_upper = val.upper().strip()

            # Detect clause start
            if val_upper in {"SELECT", "GROUP BY", "ORDER BY", "VALUES"}:
                current_clause = val_upper
                in_clause_block = True
            elif val_upper in clause_starters:
                current_clause = None
                in_clause_block = False

            # Check for missing commas inside comma-required clauses
            if in_clause_block and current_clause in comma_required_clauses:
                is_valid_column = (
                    tt in sqlparse.tokens.Name or
                    (tt is None and val.replace('.', '').isalnum())
                )
                if is_valid_column and val_upper not in clause_starters:
                    # Look ahead to the next non-whitespace token
                    j = i + 1
                    while j < len(tokens) and tokens[j][0] in sqlparse.tokens.Whitespace:
                        j += 1
                    if j < len(tokens):
                        next_tt, next_val = tokens[j]
                        next_val_upper = next_val.upper().strip()
                        is_next_valid_column = (
                            next_tt in sqlparse.tokens.Name or
                            (next_tt is None and next_val.replace('.', '').isalnum())
                        )
                        if (
                            is_next_valid_column and
                            next_val_upper not in clause_starters and
                            next_val != ','
                        ):
                            results.append((
                                SqlErrors.SYN_33_OMITTING_COMMAS,
                                f"Possible missing comma between '{val}' and '{next_val}' in {current_clause} clause"
                            ))
            i += 1

        return results

    def detect_36_nonstandard_operators(self) -> list[DetectedError]:
        '''
        Flags usage of non-standard or language-specific operators like &&, ||, ==, etc.
        '''

        results: list[DetectedError] = []
        
        # dict {error: correction}
        nonstandard_ops = {
            '=='    : '=',
            '==='   : '=',
            '!=='   : '<>',
            '&&'    : ' AND ',
            '||'    : ' OR ',
            '!'     : ' NOT ',
            # '^'     : '',
            # '~'     : '',
            '>>'    : '>',
            '<<'    : '<',
            '≠'     : '<>',
            '≥'     : '>=',
            '≤'     : '<=',
        }

        for ttype, val in self.query.tokens:
            val_stripped = val.strip()
            if ttype in sqlparse.tokens.Operator or ttype in sqlparse.tokens.Operator.Comparison or ttype == sqlparse.tokens.Error:
                if val_stripped in nonstandard_ops:
                    correction = nonstandard_ops[val_stripped]
                    results.append(DetectedError(SqlErrors.NONSTANDARD_OPERATORS, (val_stripped, correction)))

        return results
    # endregion

    # region 4) Other checks
    def detect_12_failure_to_specify_column_name_twice(self) -> list[DetectedError]:
        return []
    
    def detect_13_data_type_mismatch(self) -> list[DetectedError]:
        '''
        Checks for data type mismatches in comparisons within the query.
        '''

        def parse_set_operation(set_op: 'SetOperation', location: str) -> list[DetectedError]:

            '''
            Util function to parse a SetOperation and check for data type mismatches among its main selects.
            '''
            errors: list[DetectedError] = []
            expected_output = None # type of the first select's output
            for select in set_op.main_selects:

                typed_ast = select.typed_ast
                
                if typed_ast is None:
                    continue

                columns_type = get_type(typed_ast, select.catalog, select.search_path)

                # 1st select: set expected output type
                if expected_output is None:
                    expected_output = columns_type
                else:
                    # compare with expected output type
                    if expected_output != columns_type:
                        errors.append(DetectedError(SqlErrors.DATA_TYPE_MISMATCH, (location,"setop types inconsistent")))

                # load found messages
                for message in columns_type.messages:
                    errors.append(DetectedError(SqlErrors.DATA_TYPE_MISMATCH, message))

            return errors

        results: list[DetectedError] = []

        # CTEs
        for cte in self.query.ctes:
            results.extend(parse_set_operation(cte, f"CTE {cte.output.name}"))

        # Main Query
        results.extend(parse_set_operation(self.query.main_query, "Main Query"))

        return results
    
    def detect_14_aggregate_function_outside_select_or_having(self) -> list[DetectedError]:
        '''
        Flags use of aggregate functions (SUM, AVG, COUNT, MIN, MAX) outside SELECT or HAVING clauses,
        respecting subquery scopes.
        '''

        results: list[DetectedError] = []

        functions = self.query.functions
        for function, clause in functions:
            function_name = function.get_name()
            if function_name and function_name.upper() in {'SUM', 'AVG', 'COUNT', 'MIN', 'MAX'}:
                if clause not in {'SELECT', 'HAVING', 'ORDER BY'}:   # allow ORDER BY as well, as some databases support aggregates there
                    results.append(DetectedError(SqlErrors.AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, (function_name, clause)))

        return results
    
    def detect_15_aggregate_functions_cannot_be_nested(self) -> list[DetectedError]:
        '''
        Flags cases where aggregate functions are nested within the *same query scope*,
        which mainstream SQL dialects do not allow (e.g., SUM(MAX(x))).
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            stripped = select.strip_subqueries()

            if stripped.ast is None:
                continue

            aggregate_functions = stripped.ast.find_all(exp.AggFunc)

            for outer_agg in aggregate_functions:
                inner = outer_agg.this
                for inner_agg in inner.find_all(exp.AggFunc):
                    results.append(DetectedError(
                        SqlErrors.AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED,
                        (outer_agg.sql(),)
                    ))

        return results
    
    def detect_16_extraneous_or_omitted_grouping_column(self) -> list[DetectedError]:
        '''
            All columns in SELECT must be either included in the GROUP BY clause or aggregated.

            All non-aggregated columns in HAVING must not be included in the GROUP BY clause.
        '''

        @dataclass(frozen=True)
        class ColumnInfo:
            name: str
            alias: str
            is_aggregated: bool = False

        def get_column_name(col: exp.Column | exp.Alias, *, alias: str | None = None, is_aggregated: bool = False) -> ColumnInfo:
            '''Return normalized column name and alias. If no alias, both are the same.'''
            col_name = util.ast.column.get_real_name(col)
            col_alias = alias if alias is not None else util.ast.column.get_name(col)
            return ColumnInfo(col_name, col_alias, is_aggregated)

        results: list[DetectedError] = []

        for select in self.query.selects:
            select_stripped = select.strip_subqueries()   # we only care about the top-level SELECT for this check
            
            if select_stripped.ast is None:
                continue

            if not select_stripped.group_by:
                continue    # no GROUP BY, skip

            select_columns: list[ColumnInfo] = [] # we need a list for positional GROUP BY handling
            
            def parse_expression_for_columns(expr: exp.Expr, alias: str | None = None):
                '''Recursively parse an expression to extract all column references, handling aliases and aggregate functions.'''
                
                if isinstance(expr, exp.Star):
                    # SELECT * case: expand to all columns from all referenced tables
                    for table in select.referenced_tables:
                        for table_col in table.columns:
                            select_columns.append(ColumnInfo(table_col.name, table_col.name))
                elif isinstance(expr, exp.Alias):
                    return parse_expression_for_columns(expr.this, alias=util.ast.column.get_name(expr))
                elif isinstance(expr, exp.Column):
                    col_name = get_column_name(expr, alias=alias)
                    select_columns.append(col_name)
                elif isinstance(expr, exp.AggFunc):
                    # aggregated, add the column but skip it later
                    for c in expr.find_all(exp.Column):
                        col_name = get_column_name(c, alias=alias, is_aggregated=True)
                        select_columns.append(col_name)
                else:
                    # Complex expression: try to extract columns
                    for c in expr.find_all(exp.Column):
                        col_name = get_column_name(c, alias=alias)
                        select_columns.append(col_name)

            # Gather non-aggregated columns from SELECT
            for col in select_stripped.ast.expressions:
                parse_expression_for_columns(col)

            # Gather columns from GROUP BY
            group_by_columns: set[ColumnInfo] = set()
            for gb in select_stripped.group_by:
                if isinstance(gb, exp.Column):
                    gb_name = get_column_name(gb)
                    group_by_columns.add(gb_name)
                elif isinstance(gb, exp.Literal):
                    try:
                        val = int(gb.this)
                        # Positional GROUP BY: map to selected columns
                        if 1 <= val <= len(select_columns):
                            group_by_columns.add(select_columns[val - 1])
                    except ValueError:
                        continue
                elif isinstance(gb, exp.AggFunc):
                    for c in gb.find_all(exp.Column):
                        gb_name = get_column_name(c, is_aggregated=True)
                        group_by_columns.add(gb_name)
                else:
                    # Complex expression in GROUP BY: try to extract columns
                    for c in gb.find_all(exp.Column):
                        gb_name = get_column_name(c)
                        group_by_columns.add(gb_name)


            # Ensure all non-aggregated columns in SELECT are in GROUP BY
            for select_col in set(select_columns):  # convert to set to avoid outputting the same error multiple times
                if select_col.is_aggregated:
                    continue    # aggregated, skip
                if any(select_col.name == group_col.name or select_col.alias == group_col.alias for group_col in group_by_columns):
                    continue    # valid: in GROUP BY
                results.append(DetectedError(SqlErrors.EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,(select_col.name, 'ONLY IN SELECT')))

            # Ensure all non-aggregated columns in GROUP BY are in SELECT
            # (Note: aggregated columns in GROUP BY are invalid)
            for group_col in group_by_columns:
                if group_col.is_aggregated:
                    results.append(DetectedError(SqlErrors.EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,(group_col.name, 'AGGREGATED IN GROUP BY')))
                    continue
                if any(group_col.name == select_col.name or group_col.alias == select_col.alias for select_col in select_columns):
                    continue # valid: in SELECT
                results.append(DetectedError(SqlErrors.EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,(group_col.name, 'ONLY IN GROUP BY')))
            # Ensure all non-aggregated columns in HAVING are in GROUP BY

        return results

    def detect_17_having_without_group_by(self) -> list[DetectedError]:
        '''
        Flags queries where HAVING is used without a GROUP BY clause.
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            if select.having and not select.group_by:
                results.append(DetectedError(SqlErrors.HAVING_WITHOUT_GROUP_BY))

        return results
    
    def detect_18_confusing_function_with_function_parameter(self) -> list[DetectedError]:
        return []
    
    def detect_19_using_where_twice(self) -> list[DetectedError]:
        '''
        Flags multiple WHERE clauses in a single query block (main query, CTEs, subqueries).
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:

            # By removing subqueries, we can check only the top-level WHERE clauses in this select.
            stripped = select.strip_subqueries().strip_filters()

            # TODO: remove FILTER(WHERE ...) clauses from the count, as they are not top-level WHERE clauses

            where_count = 0
            for ttype, val in stripped.tokens:
                if ttype == sqlparse.tokens.Keyword and val.upper() == 'WHERE':
                    where_count += 1

            if where_count > 1:
                results.append(DetectedError(SqlErrors.USING_WHERE_TWICE, (select.sql, where_count)))

        return results

    def detect_20_omitted_from_clause(self) -> list[DetectedError]:
        '''
        Flags queries that omit the FROM clause entirely when it's required.
        A FROM clause is not required if:
        - The query selects only constants/literals/functions
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            stripped = select.strip_subqueries()

            from_found = False
            for ttype, val in stripped.tokens:
                if ttype == sqlparse.tokens.Keyword and val.upper() == 'FROM':
                    from_found = True
                    break

            if from_found:
                continue    # valid, has FROM clause

            ast = stripped.ast
            if ast is None:
                continue    # no AST, can't check further

            for col in ast.expressions:
                for c in col.find_all(exp.Column):
                    if util.ast.column.get_real_name(c).upper() in ('CURRENT_ROLE'):
                        # valid functions not properly recognized by sqlglot
                        continue
                    
                    results.append(DetectedError(SqlErrors.OMITTED_FROM_CLAUSE, (select.sql,)))
                    break   # no need to check further columns, one is enough to flag the error

                
                # if isinstance(col, exp.Func):
                #     if not col.find_all(exp.Column):
                #         continue    # valid, has function without column references
                # elif isinstance(col, exp.Literal):
                #     continue    # valid, has literal
                # elif isinstance(col, exp.Null):
                #     continue    # valid, has NULL literal

                # results.append(DetectedError(SqlErrors.OMITTED_FROM_CLAUSE, (select.sql,)))

            # # Check if selecting only constants/literals/functions
            # for col in stripped.output.columns:
            #     if not col.is_constant:
            #         results.append(DetectedError(SqlErrors.OMITTED_FROM_CLAUSE, (select.sql,)))
            #         break

        return results

    def detect_21_comparison_with_null(self) -> list[DetectedError]:
        '''
        Flags SQL comparisons using = NULL, <> NULL, etc. instead of IS NULL / IS NOT NULL.
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            select = select.strip_subqueries(replacement='1')   # avoid false positives from subqueries

            if select.ast is None:
                continue

            for comparison in select.ast.find_all(exp.EQ, exp.NEQ, exp.LT, exp.GT, exp.LTE, exp.GTE):
                left = comparison.left
                right = comparison.right
                if (isinstance(left, exp.Null) or isinstance(right, exp.Null)):
                    results.append(DetectedError(SqlErrors.COMPARISON_WITH_NULL, (comparison.sql(),)))

        return results

    def detect_23_date_time_field_overflow(self) -> list[DetectedError]:
        return []

    def detect_24_duplicate_clause(self) -> list[DetectedError]:
        '''
        Flags queries that contain duplicate clauses (e.g., two WHERE clauses).
        '''
        results: list[DetectedError] = []

        clause_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET'}
        
        for select in self.query.selects:
            stripped = select.strip_subqueries().strip_filters()

            clause_count = {}
            for ttype, val in stripped.tokens:
                val_upper = val.upper()
                if ttype == sqlparse.tokens.DML and val_upper == 'SELECT':
                    clause_count[val_upper] = clause_count.get(val_upper, 0) + 1
                if ttype == sqlparse.tokens.Keyword and val_upper in clause_keywords:
                    clause_count[val_upper] = clause_count.get(val_upper, 0) + 1

            for clause, count in clause_count.items():
                if count > 1:
                    results.append(DetectedError(SqlErrors.DUPLICATE_CLAUSE, (clause, count)))

        return results

    def detect_26_too_many_columns_in_subquery(self) -> list[DetectedError]:
        '''
        Flags subqueries that return more columns than expected in contexts like WHERE IN (subquery).
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:
            for subquery, clause, depth in select.subqueries:
                if clause == 'EXISTS' or 'FROM' in clause or 'JOIN' in clause:
                    # FROM/EXISTS subqueries can have any number of columns
                    continue
                
                output_columns = len(subquery.output.columns)
                expected_columns = 1  # Default expected columns for most contexts
                
                col_difference = output_columns - expected_columns
                if col_difference != 0:
                    results.append(DetectedError(SqlErrors.TOO_MANY_COLUMNS_IN_SUBQUERY, (subquery.sql, col_difference)))

        return results
    
    def detect_30_confused_order_of_keywords(self) -> list[DetectedError]:
        '''
        Flags queries where the standard order of SQL clauses is not respected.
        Expected order:
        SELECT → FROM → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT → OFFSET
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            stripped = select.strip_subqueries().strip_filters()

            expected_order = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET']
            actual_order: list[str] = []

            for ttype, val in stripped.tokens:
                if ttype == sqlparse.tokens.DML:
                    actual_order.append('SELECT')
                elif ttype == sqlparse.tokens.Keyword:
                    val_upper = val.upper()
                    if val_upper == 'FROM':
                        actual_order.append('FROM')
                    elif val_upper == 'WHERE':
                        actual_order.append('WHERE')
                    elif val_upper == 'GROUP BY':
                        actual_order.append('GROUP BY')
                    elif val_upper == 'HAVING':
                        actual_order.append('HAVING')
                    elif val_upper == 'ORDER BY':
                        actual_order.append('ORDER BY')
                    elif val_upper == 'LIMIT':
                        actual_order.append('LIMIT')
                    elif val_upper == 'OFFSET':
                        actual_order.append('OFFSET')

            # Check the order of clauses
            last_index = -1
            for clause in actual_order:
                if clause in expected_order:
                    current_index = expected_order.index(clause)
                    if current_index < last_index:
                        results.append(DetectedError(
                            SqlErrors.CONFUSED_ORDER_OF_KEYWORDS,
                            (actual_order,)
                        ))
                        break
                    last_index = current_index

        return results
            
    # NOTE: is this implementation actually coherent with the error description?
    def detect_32_confused_syntax_of_keywords(self) -> list[DetectedError]:
        '''
        Flags use of SQL keywords like LIKE, IN, BETWEEN, etc. with incorrect function-like syntax (e.g., LIKE(...)).
        '''
        return []

        results = []
        tokens = self.tokens
        keywords = {"LIKE", "BETWEEN", "IS", "IS NOT"}

        i = 0
        while i < len(tokens):
            tt, val = tokens[i]
            val_upper = val.upper()

            # Handle two-word operators like NOT IN and IS NOT
            if val_upper == "NOT" and i + 1 < len(tokens) and tokens[i + 1][1].upper() == "IN":
                keyword = "NOT IN"
                next_index = i + 2
            elif val_upper == "IS" and i + 1 < len(tokens) and tokens[i + 1][1].upper() == "NOT":
                keyword = "IS NOT"
                next_index = i + 2
            elif val_upper in keywords:
                keyword = val_upper
                next_index = i + 1
            else:
                i += 1
                continue

            # Look for '(' after the keyword → indicates function misuse
            if next_index < len(tokens):
                next_val = tokens[next_index][1].strip()
                if next_val == "(":
                    results.append((
                        SqlErrors.SYN_32_CONFUSING_THE_SYNTAX_OF_KEYWORDS,
                        f"Misuse of keyword '{keyword}' as a function with parentheses"
                    ))
                    i = next_index  # Skip ahead to avoid duplicate flag
            i += 1

        return results
    
    def detect_107_108_curly_square_or_unmatched_brackets(self) -> list[DetectedError]:
        '''
        Flags unmatched parentheses or usage of non-standard square or curly brackets in the SQL query.
        '''

        results: list[DetectedError] = []
        
        round_open = 0
        round_close = 0
        square_open = 0
        square_close = 0
        curly_open = 0
        curly_close = 0

        for ttype, val in self.query.tokens:
            if ttype is sqlparse.tokens.Punctuation:
                if val == '(':
                    round_open += 1
                elif val == ')':
                    round_close += 1
                elif val == '[':
                    square_open += 1
                elif val == ']':
                    square_close += 1
            elif ttype is sqlparse.tokens.Error:
                if val == '{':
                    curly_open += 1
                elif val == '}':
                    curly_close += 1
            elif ttype is sqlparse.tokens.Name:
                if val.startswith('{') or val.endswith('}'):
                    curly_open += val.count('{')
                    curly_close += val.count('}')
                if val.startswith('[') or val.endswith(']'):
                    square_open += val.count('[')
                    square_close += val.count(']')

        # Check for imbalance
        if round_open != round_close:
            results.append(DetectedError(SqlErrors.UNMATCHED_BRACKETS, ('round', round_open, round_close)))
        if square_open > 0 or square_close > 0:
            results.append(DetectedError(SqlErrors.CURLY_OR_SQUARE_BRACKETS, ('square', square_open, square_close)))
        if curly_open > 0 or curly_close > 0:
            results.append(DetectedError(SqlErrors.CURLY_OR_SQUARE_BRACKETS, ('curly', curly_open, curly_close)))

        return results

    def detect_35_is_where_not_applicable(self) -> list[DetectedError]:
        '''
        Find all erroneous usages of IS where it is not applicable
        '''

        def parse_set_operation(set_operation: 'SetOperation') -> list[DetectedError]:
            '''
            Util function to parse a SetOperation and check for invalid usage of IS in all its main selects.
            '''

            errors: list[DetectedError] = []
            for select in set_operation.main_selects:

                typed_ast = select.typed_ast
                
                if typed_ast is None:
                    continue

                for is_expr in typed_ast.find_all(exp.Is):
                    for error in collect_errors(is_expr, select.catalog, select.search_path):

                        # if the expected type is boolean|null, it means that the part after IS is not valid
                        if error[2] == 'boolean|null':
                            errors.append(DetectedError(SqlErrors.IS_WHERE_NOT_APPLICABLE, error))

            return errors

        results: list[DetectedError] = []

        # CTEs
        for cte in self.query.ctes:
            results.extend(parse_set_operation(cte))

        # Main Query
        results.extend(parse_set_operation(self.query.main_query))

        return results
    
    def detect_36_nonstandard_keywords_or_standard_keywords_in_wrong_context(self) -> list[DetectedError]:
        return []

    def detect_109_different_tuples_in_set_operation(self) -> list[DetectedError]:
        return []
    
    def detect_106_missing_quantifier(self) -> list[DetectedError]:
        return []
    # endregion

