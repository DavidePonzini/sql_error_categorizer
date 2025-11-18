'''Detector for syntax errors in SQL queries.'''

import difflib
import re
import sqlparse
from sqlglot import exp
from typing import Callable
from copy import deepcopy

from .base import BaseDetector, DetectedError
from ..query import Query
from ..sql_errors import SqlErrors
from .. import util


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
        checks = [self.syn_22_syn_38_additional_omitted_semicolons]

        for check in checks:
            check_result, fixed_query_str = check()
            results.extend(check_result)

            if fixed_query_str != self.query.sql:
                self.update_query(fixed_query_str, check.__name__)

        # 2) detect unexisting objects (before corrections, to avoid false positives)
        unexisting_checks = [
            self.syn_2_syn_4_undefined_columns_ambiguous_columns,
            self.syn_3_ambiguous_function,
            self.syn_5_undefined_functions,
            self.syn_6_undefined_functions_parameters,
            self.syn_7_syn_8_undefined_tables,
            self.syn_25_using_an_undefined_correlation_name,
        ]

        for check in unexisting_checks:
            check_result = check()
            results.extend(check_result)

        # 3.1) detect fixable errors and apply corrections for improved subsequent checks
        # NOTE: leave in this order!
        misspelling_checks = [
            self.syn_33_omitting_commas,
            self.syn_27_confusing_table_names_with_column_names,
            self.syn_37_nonstandard_operators,
            self.syn_9_misspellings_schemas_tables,
            self.syn_9_misspellings_columns,
            self.syn_10_synonyms,
            self.syn_11_omitted_quotes,
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
            self.syn_12_failure_to_specify_column_name_twice,
            self.syn_13_data_type_mismatch,
            self.syn_14_aggregate_function_outside_select_or_having,
            self.syn_15_aggregate_functions_cannot_be_nested,
            self.syn_16_extraneous_or_omitted_grouping_column,
            self.syn_17_having_without_group_by,
            self.syn_18_confusing_function_with_function_parameter,
            self.syn_19_using_where_twice,
            self.syn_20_omitting_the_from_clause,
            self.syn_21_comparison_with_null,
            self.syn_23_date_time_field_overflow,
            self.syn_24_duplicate_clause,
            self.syn_26_too_many_columns_in_subquery,
            self.syn_28_restriction_in_select_clause,
            self.syn_29_projection_in_where_clause,
            self.syn_30_confusing_the_order_of_keywords,
            self.syn_31_confusing_the_logic_of_keywords,
            self.syn_32_confusing_the_syntax_of_keywords,
            self.syn_34_curly_square_or_unmatched_brackets,
            self.syn_35_is_where_not_applicable,
            self.syn_36_nonstandard_keywords_or_standard_keywords_in_wrong_context,
        ]
    
        for check in checks:
            results.extend(check())
        return results

    # region Utils
    # TODO: remove
    @staticmethod
    def _are_types_compatible(type1: str, type2: str) -> bool:
        '''
            Checks if two data types are compatible for comparison.
        '''

        type1 = type1.lower()
        type2 = type2.lower()

        if type1 == type2:
            return True

        # Compatible string types
        string_types = {'varchar', 'text', 'char', 'string'}
        if type1 in string_types and type2 in string_types:
            return True

        # Compatible numeric types
        numeric_types = {'int', 'integer', 'float', 'double', 'decimal', 'numeric', 'real'}
        if type1 in numeric_types and type2 in numeric_types:
            return True

        return False
    # endregion

    # region 1) Semicolons
    def syn_22_syn_38_additional_omitted_semicolons(self) -> tuple[list[DetectedError], str]:
        '''
        Flags queries that omit the semicolon at the end or have multiple semicolons.

        Returns:
        - List of DetectedError instances for semicolon issues.
        - The cleaned query string with extra semicolons removed.
        '''

        results: list[DetectedError] = []

        all_tokens = []
        for statement in self.query.all_statements:
            all_tokens.extend(list(statement.flatten()))
        
        good_tokens = []
        trailing_semicolon_found = False
        non_whitespace_found = False
        
        for token in reversed(all_tokens):  # start from end to preserve only the last semicolon
            # check for whitespace/newline
            if token.ttype in (sqlparse.tokens.Whitespace, sqlparse.tokens.Newline):
                # keep as is and continue
                good_tokens.append(token.value)
                continue
            
            # check for semicolons: the first one before any non-whitespace is kept, others are flagged
            if token.ttype == sqlparse.tokens.Punctuation and token.value == ';':
                if non_whitespace_found:
                    # we encountered a semicolon in the middle of the query!
                    # we don't care if this is the first one we encounter, it's surely not supposed to be here
                    results.append(DetectedError(SqlErrors.SYN_38_ADDITIONAL_SEMICOLON))
                    continue
                
                if not trailing_semicolon_found:
                    # we encountered the trailing semicolon for the first time
                    # it's good, keep it
                    good_tokens.append(token.value)
                    trailing_semicolon_found = True
                    continue

                # else, we have already found the trailing semicolon, so this is an extra one at the end
                results.append(DetectedError(SqlErrors.SYN_38_ADDITIONAL_SEMICOLON))
                continue
            
            # any other token
            non_whitespace_found = True
            good_tokens.append(token.value)
                
        if not trailing_semicolon_found:
            results.append(DetectedError(SqlErrors.SYN_22_OMITTING_THE_SEMICOLON))

        return (results, ''.join(reversed(good_tokens)))
    # endregion

    # region 2) Pre-fixing
    # TODO: implement
    def syn_3_ambiguous_function(self) -> list[DetectedError]:
        return []

    def syn_7_syn_8_undefined_tables(self) -> list[DetectedError]:
        '''
        Checks for undefined tables in the FROM clause
        '''
        
        results: list[DetectedError] = []

        for select in self.query.selects:
            select = select.strip_subqueries()

            if select.ast is None:
                continue

            for table in select.ast.find_all(exp.Table):
                table_name = util.ast.table.get_real_name(table)
                schema_name = util.ast.table.get_schema(table)

                if schema_name:
                    # Fully qualified table (schema.table)
                    if not select.catalog.has_schema(schema_name):
                        results.append(DetectedError(SqlErrors.SYN_8_INVALID_SCHEMA_NAME, (table.sql(),)))
                        continue

                    if not select.catalog.has_table(schema_name, table_name):
                        results.append(DetectedError(SqlErrors.SYN_7_UNDEFINED_OBJECT, (table.sql(),)))
                        continue
                else:
                    # Unqualified table (table)
                    # Check if table is a CTE
                    if select.catalog.has_table('', table_name):
                        continue

                    # Check if table is in the current schema
                    if select.catalog.has_table(select.search_path, table_name):
                        continue

                    results.append(DetectedError(SqlErrors.SYN_7_UNDEFINED_OBJECT, (table.sql(),)))

        return results

    def syn_2_syn_4_undefined_columns_ambiguous_columns(self) -> list[DetectedError]:
        '''
        Checks for undefined and ambiguous columns.
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:
            select = select.strip_subqueries()

            if select.ast is None:
                continue

            for column in select.ast.find_all(exp.Column):
                column_name = util.ast.column.get_name(column)
                table_name = util.ast.column.get_table(column)

                possible_matches = []

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

                if len(possible_matches) == 0:
                    results.append(DetectedError(SqlErrors.SYN_4_UNDEFINED_COLUMN, (column.sql(),)))
                elif len(possible_matches) > 1:
                    results.append(DetectedError(SqlErrors.SYN_2_AMBIGUOUS_COLUMN, (column.sql(), possible_matches)))

        return results

    def syn_5_undefined_functions(self) -> list[DetectedError]:
        '''Checks for undefined functions (i.e. invalid names followed by parentheses).'''

        results: list[DetectedError] = []

        # standard_functions = {
        known_aggregate_functions = {
            'SUM', 'AVG', 'COUNT', 'MIN', 'MAX',
            'IN', 'EXISTS', 'ANY', 'ALL',
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
                results.append(DetectedError(SqlErrors.SYN_5_UNDEFINED_FUNCTION, (func_name, clause)))

        return results

    def syn_6_undefined_functions_parameters(self) -> list[DetectedError]:
        '''Checks for undefined function parameters'''

        results: list[DetectedError] = []

        for token, val in self.query.tokens:
            if any(val.startswith(p) for p in (':', '@', '?')):
                results.append(DetectedError(SqlErrors.SYN_6_UNDEFINED_PARAMETER, (val,)))

        return results
    
    # TODO: implement
    def syn_25_using_an_undefined_correlation_name(self) -> list[DetectedError]:
        return []
    # endregion

    # region 3) Fixable errors
    def syn_9_misspellings_schemas_tables(self) -> list[DetectedError]:
        '''
        Check for misspellings in table names.
        '''

        results: set[DetectedError] = set()     # use a set to avoid applying the same correction multiple times

        for select in self.query.selects:
            select = select.strip_subqueries()

            if select.ast is None:
                continue

            for table in select.ast.find_all(exp.Table):
                table = deepcopy(table)  # avoid modifying the original AST until we are sure we want to apply the correction
                table_str = table.sql()
                table_name = util.ast.table.get_real_name(table)
                schema_name = util.ast.table.get_schema(table)

                if schema_name:
                    # Fully qualified table (schema.table)
                    if select.catalog.has_table(schema_name, table_name):
                        continue

                    # check "schema.table" for more accurate matches in edge cases (i.e. can't determine if the misspelled part is schema or table)
                    available_tables = {f'{s}.{t}' for s in select.catalog.schema_names for t in select.catalog[s].table_names}
                    match = difflib.get_close_matches(f'{schema_name}.{table_name}', available_tables, n=1, cutoff=0.6)
                    if match:
                        s, t = match[0].split('.')

                        table.set('db', exp.TableAlias(this=exp.to_identifier(s, quoted=True)))
                        table.set('this', exp.to_identifier(t, quoted=True))
                        
                        results.add(DetectedError(SqlErrors.SYN_9_MISSPELLINGS, (table_str, table.sql())))
                    continue
                
                else:
                    # Unqualified table (table)
                    # Check if table is a CTE
                    if select.catalog.has_table('', table_name):
                        continue

                    # Check if table is in the current schema
                    if select.catalog.has_table(select.search_path, table_name):
                        continue

                    available_tables = {t for s in select.catalog.schema_names for t in select.catalog[s].table_names}
                    match = difflib.get_close_matches(table_name, available_tables, n=1, cutoff=0.6)
                    if match:
                        db = next(s for s in select.catalog.schema_names if select.catalog.has_table(s, match[0]))
                        table.set('this', exp.to_identifier(match[0], quoted=True))
                        if db != select.search_path:
                            table.set('db', exp.TableAlias(this=exp.to_identifier(db, quoted=True)))
                        results.add(DetectedError(SqlErrors.SYN_9_MISSPELLINGS, (table_str, table.sql())))

        return [*results]     

    def syn_9_misspellings_columns(self) -> list[DetectedError]:
        '''
            Check for misspellings in table and column names.
            Performs two passes: first try to match objects to their own type, then try to match to any type.
        '''
        results: set[DetectedError] = set()    # use a set to avoid applying the same correction multiple times

        for select in self.query.selects:
            select = select.strip_subqueries()

            if select.ast is None:
                continue

            for column in select.ast.find_all(exp.Column):
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
                    
                    results.add(DetectedError(SqlErrors.SYN_9_MISSPELLINGS, (column_str, column.sql())))

        return [*results]
    
    # TODO: implement
    def syn_10_synonyms(self) -> list[DetectedError]:
        return []

    # TODO: refactor
    def syn_11_omitted_quotes(self) -> list[DetectedError]:
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
    
    # TODO: implement
    def syn_27_confusing_table_names_with_column_names(self) -> list[DetectedError]:
        return []
    
    # TODO: refactor
    def syn_33_omitting_commas(self) -> list[DetectedError]:
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

    def syn_37_nonstandard_operators(self) -> list[DetectedError]:
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
                    results.append(DetectedError(SqlErrors.SYN_37_NONSTANDARD_OPERATORS, (val_stripped, correction)))

        return results
    # endregion

    # region 4) Other checks
    # TODO: implement
    def syn_12_failure_to_specify_column_name_twice(self) -> list[DetectedError]:
        return []
    
    # TODO: refactor, needs AST
    def syn_13_data_type_mismatch(self) -> list[DetectedError]:
        '''
        Checks for data type mismatches in comparisons within the query.
        '''

        return []
        
        # Check for data type mismatches in the query.
        results: list[DetectedError] = []

        comparison_operators = {'=', '<>', '!=', '<', '>', '<=', '>='}

        
        tokens = self.tokens
        alias_map = self.query_map.get('alias_mapping', {})
        all_table_columns = self.catalog.get("table_columns", {})
        column_metadata = self.catalog.get("column_metadata", {})

        # Build reverse alias map for resolving columns to tables
        column_to_table = {}
        for table, columns in all_table_columns.items():
            for col in columns:
                column_to_table.setdefault(col.lower(), set()).add(table)

        i = 0
        while i < len(tokens):
            tt, val = tokens[i]
            if val in comparison_operators:
                lhs_token = tokens[i - 1] if i - 1 >= 0 else None
                rhs_token = tokens[i + 1] if i + 1 < len(tokens) else None

                lhs_type = rhs_type = None

                # --- LHS type resolution ---
                if lhs_token:
                    lhs_val = lhs_token[1].strip('"`')
                    if '.' in lhs_val:
                        tbl, col = lhs_val.split('.', 1)
                        tbl = alias_map.get(tbl, [tbl])[0] if tbl in alias_map else tbl
                        lhs_type = column_metadata.get(tbl, {}).get(col, {}).get("type")
                    elif lhs_val.lower() in column_to_table:
                        for tbl in column_to_table[lhs_val.lower()]:
                            t = column_metadata.get(tbl, {}).get(lhs_val, {}).get("type")
                            if t:
                                lhs_type = t
                                break
                    elif lhs_val.startswith("'") and lhs_val.endswith("'"):
                        lhs_type = "text"
                    elif re.match(r'^\d+\.\d+$', lhs_val):
                        lhs_type = "float"
                    elif lhs_val.isdigit():
                        lhs_type = "int"

                # --- RHS type resolution ---
                if rhs_token:
                    rhs_val = rhs_token[1].strip('"`')
                    if '.' in rhs_val:
                        tbl, col = rhs_val.split('.', 1)
                        tbl = alias_map.get(tbl, [tbl])[0] if tbl in alias_map else tbl
                        rhs_type = column_metadata.get(tbl, {}).get(col, {}).get("type")
                    elif rhs_val.lower() in column_to_table:
                        for tbl in column_to_table[rhs_val.lower()]:
                            t = column_metadata.get(tbl, {}).get(rhs_val, {}).get("type")
                            if t:
                                rhs_type = t
                                break
                    elif rhs_val.startswith("'") and rhs_val.endswith("'"):
                        rhs_type = "text"
                    elif re.match(r'^\d+\.\d+$', rhs_val):
                        rhs_type = "float"
                    elif rhs_val.isdigit():
                        rhs_type = "int"

                # --- Check mismatch ---
                if lhs_type and rhs_type and not self._are_types_compatible(lhs_type, rhs_type):
                    results.append((
                        SqlErrors.SYN_13_DATA_TYPE_MISMATCH,
                        f"Comparison type mismatch: {lhs_token[1]} ({lhs_type}) {val} {rhs_token[1]} ({rhs_type})"
                    ))

            i += 1

        return results    
    
    def syn_14_aggregate_function_outside_select_or_having(self) -> list[DetectedError]:
        '''
        Flags use of aggregate functions (SUM, AVG, COUNT, MIN, MAX) outside SELECT or HAVING clauses,
        respecting subquery scopes.
        '''

        results: list[DetectedError] = []

        functions = self.query.functions
        for function, clause in functions:
            function_name = function.get_name()
            if function_name and function_name.upper() in {'SUM', 'AVG', 'COUNT', 'MIN', 'MAX'}:
                if clause not in {'SELECT', 'HAVING'}:
                    results.append(DetectedError(SqlErrors.SYN_14_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, (function_name, clause)))

        return results
    
    def syn_15_aggregate_functions_cannot_be_nested(self) -> list[DetectedError]:
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
                        SqlErrors.SYN_15_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED,
                        (outer_agg.sql(),)
                    ))

        return results
    
    def syn_16_extraneous_or_omitted_grouping_column(self) -> list[DetectedError]:
        '''
        Enforces the SQL "single-value rule":
        All selected columns must be either included in the GROUP BY clause or aggregated.
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:
            if select.ast is None:
                continue

            if not select.group_by:
                continue    # no GROUP BY, skip

            select_columns: list[tuple[str, str]] = [] # we need a list for positional GROUP BY handling

            def get_column_name(col: exp.Column | exp.Alias) -> tuple[str, str]:
                '''Return normalized column name and alias. If no alias, both are the same.'''
                col_name = util.ast.column.get_real_name(col)
                col_alias = util.ast.column.get_name(col)
                return col_name, col_alias

            for col in select.ast.expressions:
                if isinstance(col, exp.Star):
                    # SELECT * case: expand to all columns from all referenced tables
                    for table in select.referenced_tables:
                        for table_col in table.columns:
                            select_columns.append((table_col.name, table_col.name))
                if isinstance(col, exp.Column) or isinstance(col, exp.Alias):
                    col_name = get_column_name(col)
                    select_columns.append(col_name)
                elif isinstance(col, exp.Func):
                    continue  # aggregated, skip
                else:
                    # Complex expression: try to extract columns
                    for c in col.find_all(exp.Column):
                        col_name = get_column_name(c)
                        select_columns.append(col_name)

            group_by_columns = set()
            for gb in select.group_by:
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
                else:
                    # Complex expression in GROUP BY: try to extract columns
                    for c in gb.find_all(exp.Column):
                        gb_name = get_column_name(c)
                        group_by_columns.add(gb_name)

            for sel_col, sel_alias in set(select_columns):  # convert to set to avoid outputting the same error multiple times
                if any(sel_col == group_col or sel_alias == group_alias for group_col, group_alias in group_by_columns):
                    continue    # valid: in GROUP BY
                results.append(DetectedError(SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,(sel_col, 'ONLY IN SELECT')))

            for group_col, group_alias in group_by_columns:
                if any(group_col == select_col or group_alias == select_alias for select_col, select_alias in select_columns):
                    continue # valid: in SELECT
                results.append(DetectedError(SqlErrors.SYN_16_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,(group_col, 'ONLY IN GROUP BY')))

        return results

    def syn_17_having_without_group_by(self) -> list[DetectedError]:
        '''
        Flags queries where HAVING is used without a GROUP BY clause.
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            if select.having and not select.group_by:
                results.append(DetectedError(SqlErrors.SYN_17_HAVING_WITHOUT_GROUP_BY))

        return results
    
    #TODO: implement
    def syn_18_confusing_function_with_function_parameter(self) -> list[DetectedError]:
        return []
    
    def syn_19_using_where_twice(self) -> list[DetectedError]:
        '''
        Flags multiple WHERE clauses in a single query block (main query, CTEs, subqueries).
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:

            # By removing subqueries, we can check only the top-level WHERE clauses in this select.
            stripped = select.strip_subqueries()

            where_count = 0
            for ttype, val in stripped.tokens:
                if ttype == sqlparse.tokens.Keyword and val.upper() == 'WHERE':
                    where_count += 1

            if where_count > 1:
                results.append(DetectedError(SqlErrors.SYN_19_USING_WHERE_TWICE, (select.sql, where_count)))

        return results

    def syn_20_omitting_the_from_clause(self) -> list[DetectedError]:
        '''
        Flags queries that omit the FROM clause entirely when it's required.
        A FROM clause is not required if:
        - The query selects only constants/literals
        - The query uses CTEs and references them implicitly
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

            # Check if selecting only constants/literals
            for col in stripped.output.columns:
                if not col.is_constant:
                    results.append(DetectedError(SqlErrors.SYN_20_OMITTING_THE_FROM_CLAUSE, (select.sql,)))
                    break

        return results

    def syn_21_comparison_with_null(self) -> list[DetectedError]:
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
                    results.append(DetectedError(SqlErrors.SYN_21_COMPARISON_WITH_NULL, (comparison.sql(),)))

        return results

    # TODO: implement, needs AST
    def syn_23_date_time_field_overflow(self) -> list[DetectedError]:
        return []

    def syn_24_duplicate_clause(self) -> list[DetectedError]:
        '''
        Flags queries that contain duplicate clauses (e.g., two WHERE clauses).
        '''
        results: list[DetectedError] = []

        clause_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET'}
        
        for select in self.query.selects:
            stripped = select.strip_subqueries()

            clause_count = {}
            for ttype, val in stripped.tokens:
                val_upper = val.upper()
                if ttype == sqlparse.tokens.DML and val_upper == 'SELECT':
                    clause_count[val_upper] = clause_count.get(val_upper, 0) + 1
                if ttype == sqlparse.tokens.Keyword and val_upper in clause_keywords:
                    clause_count[val_upper] = clause_count.get(val_upper, 0) + 1

            for clause, count in clause_count.items():
                if count > 1:
                    results.append(DetectedError(SqlErrors.SYN_24_DUPLICATE_CLAUSE, (clause, count)))

        return results

    def syn_26_too_many_columns_in_subquery(self) -> list[DetectedError]:
        '''
        Flags subqueries that return more columns than expected in contexts like WHERE IN (subquery).
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:
            for subquery, clause in select.subqueries:
                if clause in ('FROM', 'EXISTS'):
                    continue    # FROM/EXISTS subqueries can have any number of columns
                
                output_columns = len(subquery.output.columns)
                expected_columns = 1  # Default expected columns for most contexts
                
                col_difference = output_columns - expected_columns
                if col_difference != 0:
                    results.append(DetectedError(SqlErrors.SYN_26_TOO_MANY_COLUMNS_IN_SUBQUERY, (subquery.sql, col_difference)))

        return results
    
    # TODO: refactor
    def syn_28_restriction_in_select_clause(self) -> list[DetectedError]:
        '''
        Flags queries where comparison operations (restrictions) are used in SELECT clause
        instead of WHERE clause. For example: SELECT quantity > 100 FROM transaction
        '''
        return []

        results = []
        comparison_operators = {"=", "<>", "!=", "<", ">", "<=", ">=", "IS", "LIKE"}
        
        def check_select_restrictions(map_name: str, query_map: dict):
            select_values = query_map.get("select_value", [])
            
            for select_expr in select_values:
                select_expr = select_expr.strip()
                if not select_expr:
                    continue
                
                # Check if the select expression contains comparison operators
                # that are not part of CASE statements or functions
                for op in comparison_operators:
                    if op in select_expr:
                        # Skip if it's part of a CASE statement
                        if 'CASE' in select_expr.upper() and 'WHEN' in select_expr.upper():
                            continue
                        
                        # Skip if it's inside parentheses (likely a function or subquery)
                        if '(' in select_expr and ')' in select_expr:
                            # Check if the comparison is inside parentheses
                            paren_depth = 0
                            op_in_parens = False
                            for i, char in enumerate(select_expr):
                                if char == '(':
                                    paren_depth += 1
                                elif char == ')':
                                    paren_depth -= 1
                                elif select_expr[i:i+len(op)] == op and paren_depth > 0:
                                    op_in_parens = True
                                    break
                            if op_in_parens:
                                continue
                        
                        # This looks like a restriction (comparison) in SELECT clause
                        results.append((
                            SqlErrors.SYN_28_RESTRICTION_IN_SELECT_CLAUSE,
                            f"Comparison operation '{op}' found in SELECT clause: '{select_expr}'. Consider using WHERE clause instead."
                        ))
                        break  # Only flag once per select expression

        # Check main query
        check_select_restrictions("main query", self.query_map)

        # Check CTEs
        for name, cte in self.cte_map.items():
            check_select_restrictions(f"CTE '{name}'", cte)

        # Check subqueries
        for cond, subq in self.subquery_map.items():
            check_select_restrictions(f"subquery in '{cond}'", subq)

        return results
    
    # TODO: refactor
    def syn_29_projection_in_where_clause(self) -> list[DetectedError]:
        '''
        Flags queries where a WHERE clause contains only a projection (e.g., column name)
        instead of a valid condition, including after AND/OR.
        Ignores valid literal comparisons, EXISTS(...), or qualified identifiers like t.cID.
        '''
        return []

        results = []
        tokens = self.tokens
        logical_keywords = {"AND", "OR", "WHERE"}
        boolean_functions = {"EXISTS", "NOT EXISTS", "IS", "IS NOT"}

        i = 0
        while i < len(tokens):
            ttype, val = tokens[i]
            val_upper = val.upper().strip()

            if val_upper in logical_keywords:
                # Look ahead to next non-whitespace token
                j = i + 1
                while j < len(tokens) and tokens[j][0] in sqlparse.tokens.Whitespace:
                    j += 1

                if j < len(tokens):
                    next_ttype, next_val = tokens[j]
                    next_val_stripped = next_val.strip()
                    next_val_upper = next_val_stripped.upper()

                    # Ignore EXISTS / NOT EXISTS
                    if next_val_upper in boolean_functions or next_val_upper.startswith("EXISTS"):
                        i = j
                        continue

                    # Skip qualified identifiers like t.cID
                    is_qualified_identifier = False
                    if next_ttype == sqlparse.tokens.Name and j + 2 < len(tokens):
                        dot_token = tokens[j + 1]
                        col_token = tokens[j + 2]
                        if dot_token[1] == "." and col_token[0] == sqlparse.tokens.Name:
                            is_qualified_identifier = True
                    if is_qualified_identifier:
                        i = j + 2
                        continue

                    # Check if it's a projection candidate (identifier or literal)
                    is_candidate = (
                        next_ttype in sqlparse.tokens.Name or
                        (next_ttype is None and next_val.replace(".", "").isalnum()) or
                        next_ttype in sqlparse.tokens.Literal
                    )

                    if is_candidate:
                        # Look ahead to see if a comparison operator or valid keyword follows
                        k = j + 1
                        found_operator = False
                        while k < len(tokens):
                            k_ttype, k_val = tokens[k]
                            k_val_upper = k_val.upper().strip()

                            if (
                                k_ttype in (sqlparse.tokens.Operator, sqlparse.tokens.Operator.Comparison)
                                or k_val_upper in {"IN", "NOT IN", "LIKE", "BETWEEN", "IS", "IS NOT"}
                            ):
                                found_operator = True
                                break
                            elif k_val_upper in logical_keywords or k_val in {";", ")"}:
                                break
                            elif k_ttype not in sqlparse.tokens.Whitespace:
                                break
                            k += 1

                        if not found_operator:
                            results.append((
                                SqlErrors.SYN_29_PROJECTION_IN_WHERE_CLAUSE,
                                f"Suspicious projection-like expression in WHERE: '{next_val_stripped}'"
                            ))

            i += 1

        return results

    def syn_30_confusing_the_order_of_keywords(self) -> list[DetectedError]:
        '''
        Flags queries where the standard order of SQL clauses is not respected.
        Expected order:
        SELECT → FROM → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT → OFFSET
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            stripped = select.strip_subqueries()

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
                            SqlErrors.SYN_30_CONFUSING_THE_ORDER_OF_KEYWORDS,
                            (actual_order,)
                        ))
                        break
                    last_index = current_index

        return results
        
    #TODO: implement
    def syn_31_confusing_the_logic_of_keywords(self) -> list[DetectedError]:
        return []
    
    # TODO: check and refactor
    # NOTE: is this implementation actually coherent with the error description?
    def syn_32_confusing_the_syntax_of_keywords(self) -> list[DetectedError]:
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

    def syn_34_curly_square_or_unmatched_brackets(self) -> list[DetectedError]:
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
            results.append(DetectedError(SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('round', round_open, round_close)))
        if square_open > 0 or square_close > 0:
            results.append(DetectedError(SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('square', square_open, square_close)))
        if curly_open > 0 or curly_close > 0:
            results.append(DetectedError(SqlErrors.SYN_34_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('curly', curly_open, curly_close)))

        return results
    
    #TODO: implement
    def syn_35_is_where_not_applicable(self) -> list[DetectedError]:
        return []
    
    #TODO: implement
    def syn_36_nonstandard_keywords_or_standard_keywords_in_wrong_context(self) -> list[DetectedError]:
        return []
    # endregion

