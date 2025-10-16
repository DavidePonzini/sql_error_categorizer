import difflib
import re
import sqlparse
from sqlglot import exp
from typing import Any, Callable
from copy import deepcopy

from .base import BaseDetector, DetectedError
from ..query import Query, Select
from ..sql_errors import SqlErrors
from ..catalog import Catalog
from ..parser import QueryMap, SubqueryMap, CTEMap, CTECatalog
from .. import util


class SyntaxErrorDetector(BaseDetector):
    def __init__(self, *,
                 query: Query,
                 catalog: Catalog,
                 query_map: QueryMap,
                 subquery_map: SubqueryMap,
                 cte_map: CTEMap,
                 cte_catalog: CTECatalog,
                 update_query: Callable[[str], None],
                 **kwargs,  # we don't need correct solutions for syntax errors, but we may still receive it during initialization
                ):
        super().__init__(
            query=query,
            catalog=catalog,
            query_map=query_map,
            subquery_map=subquery_map,
            cte_map=cte_map,
            cte_catalog=cte_catalog,
            update_query=update_query,
        )

    # TODO: refactor
    def run(self) -> list[DetectedError]:
        '''Run the detector and return a list of detected errors with their descriptions'''
        results: list[DetectedError] = []

        # First, detect unexisting objects (before misspellings, to avoid false positives)
        unexisting_checks = [
            self.syn_2_undefined_tables,
            self.syn_1_syn_2_undefined_columns_ambiguous_columns,
            self.syn_2_undefined_functions,
            self.syn_2_undefined_functions_parameters,
        ]

        for check in unexisting_checks:
            results.extend(check())

        # Second, detect object misspellings and missing quotes and apply corrections for improved subsequent checks
        misspelling_checks = [
            self.syn_2_misspellings_schemas_tables,
            self.syn_2_misspellings_columns,
            # self.syn_2_synonyms,      # TODO: implement
            # self.syn_2_omitted_quotes,
        ]

        # Apply corrections: replace misspelled strings in query and retokenize
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
                self.update_query(corrected_sql)
            
        # Proceed with all other checks
        checks = [
            # self.syn_3_data_type_mismatch,    # TODO: refactor
            self.syn_4_aggregate_function_outside_select_or_having,
            # self.syn_4_illegal_aggregate_function_placement_grouping_error_aggregate_functions_cannot_be_nested,  # TODO: refactor
            # self.syn_5_illegal_or_insufficient_grouping_grouping_error_extraneous_or_omitted_grouping_column, # TODO: refactor
            self.syn_5_illegal_or_insufficient_grouping_strange_having_having_without_group_by,   # TODO: refactor
            # self.syn_6_common_syntax_error_using_where_twice, # TODO: refactor
            # self.syn_6_common_syntax_error_omitting_the_from_clause,  # TODO: refactor
            # self.syn_6_common_syntax_error_comparison_with_null,  # TODO: refactor
            self.syn_6_additional_omitted_semicolons,
            # self.syn_6_common_syntax_error_restriction_in_select_clause,  # TODO: refactor
            # self.syn_6_common_syntax_error_projection_in_where_clause,    # TODO: refactor
            # self.syn_6_common_syntax_error_confusing_the_order_of_keywords,   # TODO: refactor
            # self.syn_6_common_syntax_error_confusing_the_syntax_of_keywords,  # TODO: refactor
            # self.syn_6_common_syntax_error_omitting_commas,   # TODO: refactor
            self.syn_6_common_syntax_error_curly_square_or_unmatched_brackets,
            self.syn_6_common_syntax_error_nonstandard_operators,
        ]
    
        for check in checks:
            results.extend(check())
        return results

    # region Utils
    
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

    #TODO: implement
    def syn_1_ambiguous_database_object_ambiguous_function(self) -> list[DetectedError]:
        return []
    # endregion

    def syn_2_undefined_tables(self) -> list[DetectedError]:
        '''
        Checks for undefined tables in the FROM clause
        '''
        
        results: list[DetectedError] = []

        for select in self.query.selects:
            if select.ast is None:
                continue

            for table in select.ast.find_all(exp.Table):
                table_name = util.normalize_ast_table_real_name(table)
                schema_name = util.normalize_ast_schema_name(table)

                if schema_name:
                    # Fully qualified table (schema.table)
                    if not select.catalog.has_schema(schema_name):
                        results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_INVALID_SCHEMA_NAME, (table.sql(),)))
                        continue

                    if not select.catalog.has_table(schema_name, table_name):
                        results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT, (table.sql(),)))
                        continue
                else:
                    # Unqualified table (table)
                    # Check if table is a CTE
                    if select.catalog.has_table('', table_name):
                        continue

                    # Check if table is in the current schema
                    if select.catalog.has_table(select.search_path, table_name):
                        continue

                    results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_OBJECT, (table.sql(),)))

        return results

    def syn_1_syn_2_undefined_columns_ambiguous_columns(self) -> list[DetectedError]:
        '''
        Checks for undefined and ambiguous columns.
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:
            if select.ast is None:
                continue

            for column in select.ast.find_all(exp.Column):
                column_name = util.normalize_ast_column_name(column)
                table_name = util.normalize_ast_column_table(column)

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
                    results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_COLUMN, (column.sql(),)))
                elif len(possible_matches) > 1:
                    results.append(DetectedError(SqlErrors.SYN_1_AMBIGUOUS_DATABASE_OBJECT_AMBIGUOUS_COLUMN, (column.sql(), possible_matches)))

        return results



    # region SYN-2
    # TODO: refactor, needs AST
    def syn_2_omitted_quotes(self) -> list[DetectedError]:
        '''
        Checks for potential omitting of quotes around character data in WHERE/HAVING clauses.
        
        Returns:
        A list of DetectedErrors. data=(offending_value,corrected_value)
        '''
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
                        results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_OMITTING_QUOTES_AROUND_CHARACTER_DATA, (val,)))
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
                    results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_OMITTING_QUOTES_AROUND_CHARACTER_DATA, (val,)))
                    is_rhs_of_comparison = False
                    continue
            
        return results

    def syn_2_undefined_functions(self) -> list[DetectedError]:
        '''Checks for undefined functions (i.e. invalid names followed by parentheses).'''

        results: list[DetectedError] = []

        # standard_functions = {
        known_aggregate_functions = {'SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'IN', 'EXISTS', 'ANY', 'ALL', 'COALESCE', 'NULLIF', 'CAST', 'CONVERT'}
        user_defined_functions = set() # TODO: self.catalog.functions

        all_functions = known_aggregate_functions.union(user_defined_functions)

        for func, clause in self.query.functions:
            func_name = func.get_name()
            
            if func_name is None:
                continue
            
            if func_name.upper() not in all_functions:
                results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_FUNCTION, (func_name, clause)))

        return results

    def syn_2_undefined_functions_parameters(self) -> list[DetectedError]:
        '''Checks for undefined function parameters'''

        results: list[DetectedError] = []

        for token, val in self.query.tokens:
            if any(val.startswith(p) for p in (':', '@', '?')):
                results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_UNDEFINED_PARAMETER, (val,)))

        return results

    def syn_2_misspellings_schemas_tables(self) -> list[DetectedError]:
        '''
        Check for misspellings in table names.
        '''

        results: list[DetectedError] = []

        for select in self.query.selects:
            if select.ast is None:
                continue

            for table in select.ast.find_all(exp.Table):
                table = deepcopy(table)  # avoid modifying the original AST until we are sure we want to apply the correction
                table_str = table.sql()
                table_name = util.normalize_ast_table_real_name(table)
                schema_name = util.normalize_ast_schema_name(table)

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
                        
                        results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_MISSPELLINGS, (table_str, table.sql())))
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
                        table.set('this', exp.to_identifier(match[0], quoted=True))
                        results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_MISSPELLINGS, (table_str, table.sql())))

        return results
            

    def syn_2_misspellings_columns(self) -> list[DetectedError]:
        '''
            Check for misspellings in table and column names.
            Performs two passes: first try to match objects to their own type, then try to match to any type.
        '''
        results: list[DetectedError] = []

        for select in self.query.selects:
            if select.ast is None:
                continue

            for column in select.ast.find_all(exp.Column):
                column = deepcopy(column)  # avoid modifying the original AST until we are sure we want to apply the correction
                column_str = column.sql()
                column_name = util.normalize_ast_column_name(column)
                table_name = util.normalize_ast_column_table(column)

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
                    
                    results.append(DetectedError(SqlErrors.SYN_2_UNDEFINED_DATABASE_OBJECT_MISSPELLINGS, (column_str, column.sql())))

        return results
    
    # TODO: implement
    def syn_2_synonyms(self) -> list[DetectedError]:
        return []
    # endregion

    # region SYN-3
    # TODO: implement
    def syn_3_data_type_mismatch_failure_to_specify_column_name_twice(self) -> list[DetectedError]:
        return []
    
    # TODO: refactor, needs AST
    def syn_3_data_type_mismatch(self) -> list[DetectedError]:
        '''
        Checks for data type mismatches in comparisons within the query.
        '''
        
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
                        SqlErrors.SYN_3_DATA_TYPE_MISMATCH,
                        f"Comparison type mismatch: {lhs_token[1]} ({lhs_type}) {val} {rhs_token[1]} ({rhs_type})"
                    ))

            i += 1

        return results    
    # endregion

    # region SYN-4
    def syn_4_aggregate_function_outside_select_or_having(self) -> list[DetectedError]:
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
                    results.append(DetectedError(SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_USING_AGGREGATE_FUNCTION_OUTSIDE_SELECT_OR_HAVING, (function_name, clause)))

        return results
    
    # TODO: refactor
    # TODO: check
    def syn_4_illegal_aggregate_function_placement_grouping_error_aggregate_functions_cannot_be_nested(self) -> list[DetectedError]:
        '''
        Flags cases where aggregate functions are nested within the *same query scope*,
        which mainstream SQL dialects do not allow (e.g., SUM(MAX(x))).
        Aggregates inside a subquery are NOT flagged.
        '''
        results: list[DetectedError] = []

        # Consider making this configurable / DB-specific
        AGG_FUNCS = {
            'SUM','AVG','COUNT','MIN','MAX',
            'STRING_AGG','ARRAY_AGG','BOOL_AND','BOOL_OR',
            'BIT_AND','BIT_OR','EVERY',
            'VARIANCE','VAR_POP','VAR_SAMP','STDDEV','STDDEV_POP','STDDEV_SAMP'
        }

        def _is_aggregate_function(name: str | None) -> bool:
            if not name:
                return False
            return name.upper() in AGG_FUNCS

        def _is_select_tokenlist(tl) -> bool:
            # Heuristic: this TokenList contains a SELECT statement (subquery)
            try:
                from sqlparse import sql as sqlmod
                if isinstance(tl, sqlmod.Statement):
                    return True
                text = tl.value.upper()
                # Cheap but effective heuristic for subqueries in parentheses
                return 'SELECT' in text and any(k in text for k in (' FROM ', '\nFROM '))
            except Exception:
                return False

        def _contains_nested_agg(tokenlist, inside_agg_stack: list[str]) -> list[tuple[str, str]]:
            '''
            Walks tokenlist recursively. If we are already inside an aggregate (stack not empty)
            and we encounter another aggregate function in the same scope (i.e., not under a subquery),
            we record (outer_agg, inner_agg).
            '''
            pairs: list[tuple[str, str]] = []
            from sqlparse import sql as sqlmod

            # If we hit a subquery, stop recursion *within that branch* (different scope)
            if _is_select_tokenlist(tokenlist):
                return pairs

            for tok in getattr(tokenlist, 'tokens', []):
                # Recurse into groups while respecting subquery boundaries
                if getattr(tok, 'is_group', False):
                    # If this group *is* a subquery, skip into it entirely
                    if _is_select_tokenlist(tok):
                        continue

                    # If this group is a function, handle specially
                    if isinstance(tok, sqlmod.Function):
                        fname = tok.get_name()
                        if _is_aggregate_function(fname):
                            # If already inside an aggregate, this is nested (same scope)
                            if inside_agg_stack:
                                pairs.append((inside_agg_stack[-1], fname))
                            # Recurse deeper, now we are inside this aggregate too
                            pairs.extend(_contains_nested_agg(tok, inside_agg_stack + [fname]))
                            continue

                    # Generic descent into other grouped nodes (COALESCE(...), CASE, arithmetic, etc.)
                    pairs.extend(_contains_nested_agg(tok, inside_agg_stack))
                # Non-group tokens: nothing to do
            return pairs

        from sqlparse import sql as sqlmod

        # Walk each function found by your precomputed list, but do a full scan from there
        for function, clause in self.query.functions:
            fname = function.get_name()
            if not _is_aggregate_function(fname):
                continue

            nested = _contains_nested_agg(function, [fname])
            for outer_name, inner_name in nested:
                # Build a compact display like SUM(MAX(...))
                results.append(DetectedError(
                    SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED,
                    (f'{outer_name}({inner_name}(...))',)
                ))

        return results



    # def syn_4_illegal_aggregate_function_placement_grouping_error_aggregate_functions_cannot_be_nested(self) -> list[DetectedError]:
    #     '''Flags cases where aggregate functions are nested, which is not allowed in SQL.'''

    #     results: list[DetectedError] = []

    #     for function, clause in self.query.functions:
    #         function_name = function.get_name()
    #         if function_name and function_name.upper() in {'SUM', 'AVG', 'COUNT', 'MIN', 'MAX'}:
    #             # Check if any argument is another aggregate function
    #             for token in function.tokens:
    #                 if token.is_group:
    #                     for subtoken in token.tokens:
    #                         if subtoken.is_group and isinstance(subtoken, sqlparse.sql.Function):
    #                             sub_func_name = subtoken.get_name()
    #                             if sub_func_name and sub_func_name.upper() in {'SUM', 'AVG', 'COUNT', 'MIN', 'MAX'}:
    #                                 results.append(DetectedError(
    #                                     SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED,
    #                                     (f"{function_name}({sub_func_name}(...))",)
    #                                 ))

    #     results = []
    #     aggregate_functions = {"SUM", "AVG", "COUNT", "MIN", "MAX"}

    #     if not self.parsed_qry:
    #         return results

    #     def contains_nested_agg(token_list, in_aggregate=False):
    #         '''
    #         Recursively check for nested aggregate functions, only flagging true nesting (e.g., SUM(AVG(x))).
    #         '''
    #         aggregate_functions = {"SUM", "AVG", "COUNT", "MIN", "MAX"}

    #         for token in token_list.tokens if hasattr(token_list, 'tokens') else []:

    #             # Detect aggregate function by structure: Name followed by Parenthesis
    #             if token.is_group and isinstance(token, sqlparse.sql.Function):
    #                 func_name_token = token.get_name()
    #                 if func_name_token and func_name_token.upper() in aggregate_functions:
    #                     if in_aggregate:
    #                         return (
    #                             SqlErrors.SYN_4_ILLEGAL_AGGREGATE_FUNCTION_PLACEMENT_GROUPING_ERROR_AGGREGATE_FUNCTIONS_CANNOT_BE_NESTED,
    #                             f"Nested aggregate function found: {func_name_token.upper()}"
    #                         )
    #                     # Dive into the content of the function with in_aggregate=True
    #                     for subtoken in token.tokens:
    #                         if subtoken.is_group:
    #                             nested = contains_nested_agg(subtoken, in_aggregate=True)
    #                             if nested:
    #                                 return nested
    #                     continue  # Don't scan this token again outside

    #             # For any other group, scan recursively
    #             if token.is_group:
    #                 nested = contains_nested_agg(token, in_aggregate)
    #                 if nested:
    #                     return nested

    #         return None



    #     # Walk the whole parsed query
    #     for token in self.parsed_qry.tokens:
    #         if token.is_group:
    #             nested_result = contains_nested_agg(token)
    #             if nested_result:
    #                 results.append(nested_result)

    #     return results
    # endregion

    # region SYN-5
    # TODO: refactor
    def syn_5_illegal_or_insufficient_grouping_grouping_error_extraneous_or_omitted_grouping_column(self) -> list[DetectedError]:
        '''
        Enforces the SQL "single-value rule":
        All selected columns must be either included in the GROUP BY clause or aggregated.
        '''
        results = []
        aggregate_funcs = {"SUM", "AVG", "COUNT", "MIN", "MAX"}

        def is_aggregate(expr: str) -> bool:
            '''Returns True if the select expression is an aggregate function.'''
            expr_upper = expr.upper().strip()
            return any(expr_upper.startswith(func + '(') for func in aggregate_funcs)

        def extract_columns(expr: str) -> list:
            '''Extracts raw column names from a select expression like 't.col' or 'SUM(t.col)'.'''
            expr = expr.strip()
            if '(' in expr:
                inner = expr[expr.find('(') + 1 : expr.rfind(')')]
                return [inner.strip()]
            return [expr]

        def check_single_value_rule(map_name: str, query_map: dict):
            group_by = set(col.lower() for col in query_map.get("group_by_values", []))
            select_values = query_map.get("select_value", [])

            if not group_by: return

            for expr in select_values:
                expr = expr.strip()
                if not expr or is_aggregate(expr):
                    continue  # valid: aggregated

                cols = extract_columns(expr)
                for col in cols:
                    if col.lower() not in group_by:
                        results.append((
                            SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_GROUPING_ERROR_EXTRANEOUS_OR_OMITTED_GROUPING_COLUMN,
                            f"Column '{col}' must be grouped or aggregated."
                        ))

        # Check main query
        check_single_value_rule("main query", self.query_map)

        # Check CTEs
        for name, cte in self.cte_map.items():
            check_single_value_rule(f"CTE '{name}'", cte)

        # Check subqueries
        for cond, subq in self.subquery_map.items():
            check_single_value_rule(f"subquery in '{cond}'", subq)

        return results
     
    def syn_5_illegal_or_insufficient_grouping_strange_having_having_without_group_by(self) -> list[DetectedError]:
        '''
        Flags queries where HAVING is used without a GROUP BY clause.
        '''
        results: list[DetectedError] = []

        def check_having_without_group_by(query_map: QueryMap, data: tuple[Any, ...]):
            if query_map.having and not query_map.group_by_values:
                results.append(DetectedError(SqlErrors.SYN_5_ILLEGAL_OR_INSUFFICIENT_GROUPING_STRANGE_HAVING_HAVING_WITHOUT_GROUP_BY, data))

        # Check main query
        check_having_without_group_by(self.query_map, ('Main',))

        # Check CTEs
        for name, cte in self.cte_map.items():
            check_having_without_group_by(cte, ('CTE', name))

        # Check subqueries
        for cond, subq in self.subquery_map.items():
            check_having_without_group_by(subq, ('Subquery', cond))

        return results
    # endregion

    # region SYN-6
    #TODO def syn_6_common_syntax_error_confusing_function_with_function_parameter(self):
    
    # TODO: refactor
    def syn_6_common_syntax_error_using_where_twice(self) -> list[DetectedError]:
        '''
        Flags multiple WHERE clauses in a single query block (main query, CTEs, subqueries).
        '''
        results = []

        def count_where_clauses(query: str) -> int:
            # Remove strings and comments to avoid counting 'WHERE' inside them
            cleaned = re.sub(r"'(?:''|[^'])*'", '', query)
            cleaned = re.sub(r"--.*?$", '', cleaned, flags=re.MULTILINE)
            cleaned = re.sub(r"/\*.*?\*/", '', cleaned, flags=re.DOTALL)
            return len(re.findall(r'\bWHERE\b', cleaned, flags=re.IGNORECASE))

        # --- Step 1: Extract CTEs and check each one ---
        cte_blocks = []
        query = QueryParser.normalize_query(self.query)

        if query.strip().upper().startswith("WITH"):
            cte_part_match = re.search(r'\bWITH\b(.*?)(?=SELECT\b)', query, re.IGNORECASE | re.DOTALL)
            if cte_part_match:
                cte_part = cte_part_match.group(1)
                # Simple split on 'AS (' to get individual CTE bodies
                cte_bodies = re.findall(r'AS\s*\((.*?)\)', cte_part, re.IGNORECASE | re.DOTALL)
                for body in cte_bodies:
                    if count_where_clauses(body) > 1:
                        results.append((
                            SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_USING_WHERE_TWICE,
                            "Multiple WHERE clauses detected in CTE"
                        ))

        # --- Step 2: Isolate and check subqueries ---
        _, query_without_cte = QueryParser.extract_ctes(self.query)
        subq_map, cleaned_main_query = QueryParser.extract_subqueries(query_without_cte)

        # The `cond` (key of subq_map) contains the context, including the outer `WHERE`.
        # The bug is checking this whole string. We must check only the subquery body.
        for cond in subq_map.keys():
            # Isolate the subquery body from its context by finding the parenthesized expression.
            subquery_body = ""
            start_pos = cond.find('(')
            if start_pos != -1:
                depth = 0
                for i in range(start_pos, len(cond)):
                    char = cond[i]
                    if char == '(':
                        depth += 1
                    elif char == ')':
                        depth -= 1
                    if depth == 0:
                        subquery_body = cond[start_pos + 1:i]
                        break
            
            # Only check the isolated subquery for multiple WHERE clauses.
            if subquery_body and count_where_clauses(subquery_body) > 1:
                results.append((
                    SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_USING_WHERE_TWICE,
                    "Multiple WHERE clauses detected in a subquery"
                ))

        # --- Step 3: Check the main query (with subqueries conceptually removed) ---
        def count_top_level_where(query_str: str) -> int:
            '''Counts WHERE clauses at the top level, ignoring those inside parentheses.'''
            depth = 0
            count = 0
            in_string = False
            for match in re.finditer(r"\bWHERE\b|\(|\)|'", query_str, re.IGNORECASE):
                token = match.group(0)
                if token == "'":
                    in_string = not in_string
                if not in_string:
                    if token == '(':
                        depth += 1
                    elif token == ')':
                        depth = max(0, depth - 1)
                    elif token.upper() == 'WHERE' and depth == 0:
                        count += 1
            return count

        if count_top_level_where(cleaned_main_query) > 1:
            results.append((
                SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_USING_WHERE_TWICE,
                "Multiple WHERE clauses detected in main query"
            ))

        return list(set(results))

    # TODO: refactor
    def syn_6_common_syntax_error_omitting_the_from_clause(self) -> list[DetectedError]:
        '''
        Flags queries that omit the FROM clause entirely when it's required.
        A FROM clause is not required if:
        - The query selects only constants/literals
        - The query uses CTEs and references them implicitly
        '''
        results = []

        def check_from_clause(map_name: str, query_map: dict):
            from_val = query_map.get("from_value", "")
            if not from_val or str(from_val).strip() == "":
                                
                # Check if selecting only constants/literals
                select_values = query_map.get("select_value", [])
                if select_values:
                    # Check if all select values are constants/literals
                    all_constants = True
                    for val in select_values:
                        val_clean = val.strip()
                        # Check if it's a number, string literal, or simple expression
                        if not (val_clean.isdigit() or 
                               (val_clean.startswith("'") and val_clean.endswith("'")) or
                               (val_clean.startswith('"') and val_clean.endswith('"')) or
                               val_clean.upper() in {'NULL', 'TRUE', 'FALSE'} or
                               val_clean.startswith('CAST(') or
                               val_clean.startswith('CONVERT(')):
                            all_constants = False
                            break
                    
                    if all_constants:
                        # Selecting only constants is valid without FROM clause
                        return
                
                results.append((
                    SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_FROM_CLAUSE,
                    f"Missing FROM clause in {map_name}"
                ))

        # Main query
        check_from_clause("main query", self.query_map)

        # CTEs
        for name, cte in self.cte_map.items():
            check_from_clause(f"CTE '{name}'", cte)

        # Subqueries
        for cond, subq in self.subquery_map.items():
            check_from_clause(f"subquery in '{cond}'", subq)

        return results

    # TODO: refactor
    def syn_6_common_syntax_error_comparison_with_null(self) -> list[DetectedError]:
        '''
        Flags SQL comparisons using = NULL, <> NULL, etc. instead of IS NULL / IS NOT NULL.
        '''
        results = []
        comparison_ops = {"=", "<>", "!=", "<", ">", "<=", ">="}
        null_literals = {"NULL", "null"}
        
        tokens = self.tokens

        for i, (tt, val) in enumerate(tokens):
            #print(f"DEBUG: Token {i}: {tt} -> {val}")
            if val.upper() in comparison_ops:
                # Check left and right tokens
                lhs = tokens[i - 1][1].strip() if i > 0 else ""
                rhs = tokens[i + 1][1].strip() if i + 1 < len(tokens) else ""

                if lhs.upper() in null_literals or rhs.upper() in null_literals:
                    results.append((
                        SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_COMPARISON_WITH_NULL,
                        f"Invalid NULL comparison using '{val}' with NULL: use IS NULL or IS NOT NULL instead"
                    ))

        return results
    
    def syn_6_additional_omitted_semicolons(self) -> list[DetectedError]:
        '''
        Flags queries that omit the semicolon at the end or have multiple semicolons.
        '''

        results: list[DetectedError] = []

        has_semicolon_at_end = self.query.tokens and self.query.tokens[-1][1].strip() == ';'
        # Check if the last token is a semicolon
        if not has_semicolon_at_end:
            results.append(DetectedError(SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_THE_SEMICOLON))
        
        # Check for multiple semicolons inside the query
        tokens_to_check = self.query.tokens[:-1] if has_semicolon_at_end else self.query.tokens
        for token, val in tokens_to_check:
            if token == sqlparse.tokens.Punctuation and val == ';':
                results.append(DetectedError(SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON)) 

        # Check for multiple semicolons at the end of the query (resulting in multiple statements)
        for _ in self.query.all_statements[1:]:     # skip the actual query
            results.append(DetectedError(SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_ADDITIONAL_SEMICOLON))
        
        return results


    
    #TODO def syn_6_common_syntax_error_date_time_field_overflow(self):
    #TODO def syn_6_common_syntax_error_duplicate_clause(self):
    #TODO def syn_6_common_syntax_error_using_an_undefined_correlation_name(self):
    #TODO def syn_6_common_syntax_error_too_many_columns_in_subquery(self) -> list:
    #TODO def syn_6_common_syntax_error_confusing_table_names_with_column_names(self):
    
    # TODO: refactor
    def syn_6_common_syntax_error_restriction_in_select_clause(self) -> list[DetectedError]:
        '''
        Flags queries where comparison operations (restrictions) are used in SELECT clause
        instead of WHERE clause. For example: SELECT quantity > 100 FROM transaction
        '''
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
                            SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_RESTRICTION_IN_SELECT_CLAUSE,
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
    def syn_6_common_syntax_error_projection_in_where_clause(self) -> list[DetectedError]:
        '''
        Flags queries where a WHERE clause contains only a projection (e.g., column name)
        instead of a valid condition, including after AND/OR.
        Ignores valid literal comparisons, EXISTS(...), or qualified identifiers like t.cID.
        '''
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
                                SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_PROJECTION_IN_WHERE_CLAUSE,
                                f"Suspicious projection-like expression in WHERE: '{next_val_stripped}'"
                            ))

            i += 1

        return results


    # TODO: refactor
    def syn_6_common_syntax_error_confusing_the_order_of_keywords(self) -> list[DetectedError]:
        '''
        Flags queries where the standard order of SQL clauses is not respected.
        Expected order:
        SELECT → FROM → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT
        '''
        results = []

        clause_order = {
            "SELECT": 1,
            "FROM": 2,
            "WHERE": 3,
            "GROUP BY": 4,
            "HAVING": 5,
            "ORDER BY": 6,
            "LIMIT": 7
        }

        # Record the position where each clause appears
        clause_positions = {}

        tokens = self.tokens
        i = 0
        while i < len(tokens):
            _, val = tokens[i]
            val_upper = val.upper().strip()

            if val_upper == "GROUP" and i + 1 < len(tokens) and tokens[i + 1][1].upper() == "BY":
                clause_positions["GROUP BY"] = i
                i += 2
                continue
            elif val_upper == "ORDER" and i + 1 < len(tokens) and tokens[i + 1][1].upper() == "BY":
                clause_positions["ORDER BY"] = i
                i += 2
                continue
            elif val_upper in clause_order:
                if val_upper not in clause_positions:
                    clause_positions[val_upper] = i
            i += 1

        # Check order
        sorted_clauses = sorted(clause_positions.items(), key=lambda kv: kv[1])
        found_order = [clause for clause, _ in sorted_clauses]
        expected_order = list(clause_order.keys())

        # Reduce expected to only the ones that appear in this query
        expected_sequence = [cl for cl in expected_order if cl in clause_positions]

        if found_order != expected_sequence:
            results.append((
                SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_ORDER_OF_KEYWORDS,
                f"Incorrect clause order: found {found_order}, expected {expected_sequence}"
            ))

        return results
        
    #TODO def syn_6_common_syntax_error_confusing_the_logic_of_keywords(self):
    
    # TODO: refactor
    def syn_6_common_syntax_error_confusing_the_syntax_of_keywords(self) -> list[DetectedError]:
        '''
        Flags use of SQL keywords like LIKE, IN, BETWEEN, etc. with incorrect function-like syntax (e.g., LIKE(...)).
        '''
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
                        SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CONFUSING_THE_SYNTAX_OF_KEYWORDS,
                        f"Misuse of keyword '{keyword}' as a function with parentheses"
                    ))
                    i = next_index  # Skip ahead to avoid duplicate flag
            i += 1

        return results

    # TODO: refactor
    def syn_6_common_syntax_error_omitting_commas(self) -> list[DetectedError]:
        '''
        Flags queries where commas are likely missing between column expressions 
        (e.g., SELECT name age FROM ..., GROUP BY x y).
        '''
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
                                SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_OMITTING_COMMAS,
                                f"Possible missing comma between '{val}' and '{next_val}' in {current_clause} clause"
                            ))
            i += 1

        return results

    def syn_6_common_syntax_error_curly_square_or_unmatched_brackets(self) -> list[DetectedError]:
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

        # Check for imbalance
        if round_open != round_close:
            results.append(DetectedError(SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('round', round_open, round_close)))
        if square_open > 0 or square_close > 0:
            results.append(DetectedError(SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('square', square_open, square_close)))
        if curly_open > 0 or curly_close > 0:
            results.append(DetectedError(SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_CURLY_SQUARE_OR_UNMATCHED_BRACKETS, ('curly', curly_open, curly_close)))

        return results
    
    #TODO: implement
    def syn_6_common_syntax_error_is_where_not_applicable(self) -> list[DetectedError]:
        return []
    
    #TODO: implement
    def syn_6_common_syntax_error_nonstandard_keywords_or_standard_keywords_in_wrong_context(self) -> list[DetectedError]:
        return []
    
    def syn_6_common_syntax_error_nonstandard_operators(self) -> list[DetectedError]:
        '''
        Flags usage of non-standard or language-specific operators like &&, ||, ==, etc.
        '''

        results: list[DetectedError] = []
        
        nonstandard_ops = {
            '==', '===', '!==', '&&', '||', '!', '+=', '-=', '*=', '/=', '^', '~', '>>', '<<', '≠', '≥', '≤'
        }

        for ttype, val in self.query.tokens:
            val_stripped = val.strip()
            if ttype in sqlparse.tokens.Operator or ttype in sqlparse.tokens.Operator.Comparison:
                if val_stripped in nonstandard_ops:
                    results.append(DetectedError(SqlErrors.SYN_6_COMMON_SYNTAX_ERROR_NONSTANDARD_OPERATORS, (val_stripped,)))

        return results

    # endregion

