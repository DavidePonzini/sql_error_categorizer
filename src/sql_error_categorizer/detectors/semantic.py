import difflib
import re
import sqlparse
import sqlparse.keywords
from typing import Callable

from .base import BaseDetector, DetectedError
from ..query import Query
from ..sql_errors import SqlErrors
from ..catalog import Catalog
from ..parser import QueryMap, SubqueryMap, CTEMap, CTECatalog


class SemanticErrorDetector(BaseDetector):
    def __init__(self, *,
                 query: Query,
                 catalog: Catalog,
                 query_map: QueryMap,
                 subquery_map: SubqueryMap,
                 cte_map: CTEMap,
                 cte_catalog: CTECatalog,
                 update_query: Callable[[str], None],
                 correct_solutions: list[str] = [],
                ):
        super().__init__(
            query=query,
            catalog=catalog,
            query_map=query_map,
            subquery_map=subquery_map,
            cte_map=cte_map,
            cte_catalog=cte_catalog,
            update_query=update_query,
            correct_solutions=correct_solutions,
        )

    # def _prepare(self):
    #     super()._prepare()
    #     self.parsed_qry = sqlparse.parse(self.query)
    #     if not self.parsed_qry: return
    #     self.parsed_qry = self.parsed_qry[0]

    def run(self) -> list[DetectedError]:
        results: list[DetectedError] = []

        checks = [
            # self.sem_1_inconsistent_expression,   # TODO: refactor
            # self.sem_1_distinct_in_sum_or_avg,    # TODO: refactor
            # self.sem_1_wildcards_without_like,    # TODO: refactor
            # self.sem_1_incorrect_wildcard,    # TODO: refactor
            # self.sem_1_mixing_comparison_and_null,    # TODO: refactor
            # self.sem_2_join_on_incorrect_column,  # TODO: refactor
            # self.sem_5_constant_column_output,    # TODO: refactor
            # self.sem_5_duplicate_column_output    # TODO: refactor
        ]
        
        for chk in checks:
            results.extend(chk())
        return results

    def sem_1_inconsistent_expression(self) -> list:
        """Detect contradictory WHERE conditions: equality mismatches and non-overlapping ranges"""
        # Need to check at least 2x equality or range conditions on the same column
        # e.g., col='x' AND col='y' or col < a AND col > b where b >= a
        
        results = []
        # pull out the WHERE clause body
        m = re.search(r'WHERE\s+(.*?)\s*(GROUP|HAVING|ORDER|LIMIT|$)',
                    self.query, re.IGNORECASE | re.DOTALL)
        if not m:
            return results
        body = m.group(1)

        # 1) Equality contradictions: col = v1 AND col = v2, for any literal
        eqs = re.findall(
            r"(\w+)\s*=\s*('[^']+'|\d+(?:\.\d+)?)",
            body
        )
        eq_map = {}
        for col, raw in eqs:
            eq_map.setdefault(col.lower(), []).append(raw)
        for col, raws in eq_map.items():
            if len(raws) > 1:
                for i in range(len(raws)):
                    for j in range(i + 1, len(raws)):
                        if raws[i] != raws[j]:
                            expr = f"{col} = {raws[i]} AND {col} = {raws[j]}"
                            results.append((
                                SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_AND_INSTEAD_OF_OR,
                                expr
                            ))

        # 2) Range contradictions: col < a AND col > b where b >= a (strings or numbers)
        rngs = re.findall(
            r"(\w+)\s*(<|<=|>|>=)\s*('[^']+'|\d+(?:\.\d+)?)",
            body
        )
        range_map = {}
        for col, op, raw in rngs:
            # raw stays quoted for reporting; strip for comparison
            val = raw.strip("'") if raw.startswith("'") else float(raw)
            range_map.setdefault(col.lower(), []).append((op, val, raw))

        for col, conds in range_map.items():
            if len(conds) < 2:
                continue
            # examine each mixed upper/lower pair
            for i in range(len(conds)):
                op1, v1, r1 = conds[i]
                for j in range(i + 1, len(conds)):
                    op2, v2, r2 = conds[j]
                    # need one upper-bound and one lower-bound
                    if ((op1 in ('<','<=') and op2 in ('>','>=')) or
                        (op2 in ('<','<=') and op1 in ('>','>='))):
                        # assign upper vs lower
                        if op1 in ('<','<='):
                            upper_v, upper_op, upper_r = v1, op1, r1
                            lower_v, lower_op, lower_r = v2, op2, r2
                        else:
                            upper_v, upper_op, upper_r = v2, op2, r2
                            lower_v, lower_op, lower_r = v1, op1, r1

                        # compare lexicographically if both strings, numerically otherwise
                        bad = False
                        if isinstance(lower_v, str) and isinstance(upper_v, str):
                            bad = lower_v >= upper_v
                        else:
                            bad = float(lower_v) >= float(upper_v)

                        if bad:
                            expr = (
                                f"{col} {lower_op} {lower_r} AND "
                                f"{col} {upper_op} {upper_r}"
                            )
                            results.append((
                                SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_AND_INSTEAD_OF_OR,
                                expr
                            ))

        return results
    
    #TODO def sem_1_tautological_or_inconsistent_expression(self):

    def sem_1_distinct_in_sum_or_avg(self) -> list:
        """Detect SUM(DISTINCT ...) or AVG(DISTINCT ...)"""
        # Accepts SUM(DISTINCT col), SUM(DISTINCT(col)), etc.
        if re.search(r"\b(SUM|AVG)\s*\(\s*DISTINCT\s*\(?\s*\w+\s*\)?", self.query, re.IGNORECASE):
            return [(SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_DISTINCT_IN_SUM_OR_AVG, 'DISTINCT')]
        return []
    
    #TODO def sem_1_distinct_removing_important_duplicates(self):
    
    def sem_1_wildcards_without_like(self) -> list:
        """Detect = '%...%' instead of LIKE"""
        m = re.search(r"=\s*'[^']*%[^']*'", self.query)
        if m:
            return [(SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_WILDCARDS_WITHOUT_LIKE, m.group(0))]
        return []
    
    def sem_1_incorrect_wildcard(self) -> list:
        """Detect misuse of '_' or '*' wildcards"""
        # 1) Using '=' with '_' suggests misuse of wildcard instead of LIKE/%
        if re.search(r"=\s*'[^']*_[^']*'", self.query, re.IGNORECASE):
            return [(SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_INCORRECT_WILDCARD_USING_UNDERSCORE_INSTEAD_OF_PERCENT, '_')]

        # 2) Using LIKE with '_' but perhaps intending any-length match (should use '%')
        if re.search(r"LIKE\s*'[^']*_[^']*'", self.query, re.IGNORECASE):
            return [(SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_INCORRECT_WILDCARD_USING_UNDERSCORE_INSTEAD_OF_PERCENT, '_')]

        # 3) Using '*' (either with = or LIKE) instead of '%' wildcard
        if re.search(r"(?:LIKE|=)\s*'[^']*\*[^']*'", self.query, re.IGNORECASE):
            return [(SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_INCORRECT_WILDCARD_USING_UNDERSCORE_INSTEAD_OF_PERCENT, '*')]

        return []
    
    def sem_1_mixing_comparison_and_null(self) -> list: 
        """Detect mixing of >0 with IS NOT NULL or empty string with IS NULL on the same column"""
        results = []
        # a > 0 AND a IS NOT NULL
        m = re.search(r"(\w+)\s*>\s*0\s+AND\s+\1\s+IS\s+NOT\s+NULL", self.query, re.IGNORECASE)
        if m:
            results.append((
                SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_MIXING_A_GREATER_THAN_0_WITH_IS_NOT_NULL,
                m.group(0)
            ))

        # a = '' AND a IS NULL
        m2 = re.search(r"(\w+)\s*=\s*''\s+AND\s+\1\s+IS\s+NULL", self.query, re.IGNORECASE)
        if m2:
            results.append((
                SqlErrors.SEM_1_INCONSISTENT_EXPRESSION_MIXING_A_GREATER_THAN_0_WITH_IS_NOT_NULL,
                m2.group(0)
            ))

        return results    
    
    #TODO def sem_2_null_in_in_subquery(self) -> list:
        """Detect potential NULL/UNKNOWN in IN/ANY/ALL subqueries when subquery column is nullable.
            heuristically assume that if a column is not declared as NOT NULL, then every typical 
            database state contains at least one row in which it is null. """

    def sem_2_join_on_incorrect_column(self) -> list:
        """
        For each JOIN … ON: require at least one “A.col = B.col” in the ON clause.
        For comma-style joins (FROM A, B): require at least one “A.col = B.col” in the WHERE.
        If no such predicate is found for a given join, emit SEM_2_JOIN_ON_INCORRECT_COLUMN.
        If the join operation is a self-join, then skip the check.
        Check based on the content of the catalog column_metadata the compatibility of the columns.
        """
        results = []
        return results
            
    #TODO def sem_3_missing_join(self) -> list:
    
    #TODO def sem_4_duplicate_rows(self) -> list:
    
    def sem_5_constant_column_output(self) -> list:
        """
        Detect when a SELECT-list column is constrained to a constant.
        - If WHERE has A = c and A is in SELECT, warn.
        - If WHERE has A = c and also A = B, then both A and B in SELECT should warn.
        """
        results = []

        # 1. Extract selected columns (simple ones only)
        select_cols = set()
        for expr in self.query_map.get("select_value", []):
            expr = expr.strip()
            if expr == "*" or "(" in expr:
                continue
            # Remove potential table qualification and aliases for the check
            col = expr.split("AS")[0].strip().split(".")[-1]
            select_cols.add(col.lower())

        # 2. Extract WHERE clause from the query text
        where_clause_match = re.search(
            r"\bWHERE\b\s+(?P<w>.+?)(?=(?:\bGROUP\b|\bHAVING\b|\bORDER\b|$))",
            self.query, re.IGNORECASE | re.DOTALL
        )
        if not where_clause_match:
            return results

        where_clause = where_clause_match.group("w")

        # Remove subqueries from the WHERE clause text to avoid checking their conditions.
        # This prevents the recognizer from applying a subquery's constraints to the outer query.
        where_clause_no_subs = re.sub(r'\(\s*SELECT.*?\)', '', where_clause, flags=re.IGNORECASE | re.DOTALL)

        # 3. Detect constant columns and column-to-column equalities in the processed clause
        const_re = re.compile(
            r"(?P<col>[a-zA-Z_]\w*(?:\.\w+)?)\s*=\s*(?P<const>'[^']*'|\d+(?:\.\d+)?)",
            re.IGNORECASE
        )
        eq_re = re.compile(
            r"(?P<c1>[a-zA-Z_]\w*(?:\.\w+)?)\s*=\s*(?P<c2>[a-zA-Z_]\w*(?:\.\w+)?)",
            re.IGNORECASE
        )

        const_map = {}
        for m in const_re.finditer(where_clause_no_subs):
            col = m.group("col").split(".")[-1].lower()
            const_map[col] = m.group("const")

        adj = {}
        for m in eq_re.finditer(where_clause_no_subs):
            c1 = m.group("c1").split(".")[-1].lower()
            c2 = m.group("c2").split(".")[-1].lower()
            if c1 in const_map or c2 in const_map:
                continue
            # Avoid self-loops from simple equality checks
            if c1 != c2:
                adj.setdefault(c1, set()).add(c2)
                adj.setdefault(c2, set()).add(c1)

        # 4. Propagate constant constraints via BFS
        constant_cols = set(const_map.keys())
        for start_node in list(const_map):
            queue = [start_node]
            visited = {start_node}
            while queue:
                u = queue.pop(0)
                for v in adj.get(u, []):
                    if v not in visited:
                        visited.add(v)
                        queue.append(v)
            constant_cols.update(visited)

        # 5. Check if any selected columns are constrained to be constant
        for col in select_cols:
            if col in constant_cols:
                # Find the original casing for the error message
                original_col_name = next((c for c in self.query_map.get("select_value", []) if c.lower().endswith(col)), col)
                msg = f"Column `{original_col_name}` in SELECT is constrained to constant"
                results.append((SqlErrors.SEM_5_REDUNDANT_COLUMN_OUTPUT_CONSTANT_COLUMN_OUTPUT, msg))

        return results
    
    def sem_5_duplicate_column_output(self) -> list:
        """
        Detects if the same column or expression appears multiple times in the SELECT list.
        """
        results = []

        # 1. Usa il SELECT list già parsato dalla query_map
        select_items = self.query_map.get("select_value", [])
        if not select_items:
            return results

        norm_counts = {}

        for expr in select_items:
            # Normalizza l’espressione: rimuove alias, spazi, case-insensitive
            clean_expr = expr.strip()

            # Rimuovi alias "AS xyz" o finali (non rompere funzioni con parentesi)
            clean_expr = re.sub(r"\s+AS\s+\w+$", "", clean_expr, flags=re.IGNORECASE)
            clean_expr = re.sub(r"\s+\w+$", "", clean_expr)

            # Normalizza spazi e case
            key = clean_expr.strip().lower()
            norm_counts[key] = norm_counts.get(key, 0) + 1

        # 2. Rileva duplicati
        for expr, count in norm_counts.items():
            if count > 1:
                msg = f"Output expression `{expr}` appears {count} times in SELECT"
                results.append((
                    SqlErrors.SEM_5_REDUNDANT_COLUMN_OUTPUT_DUPLICATE_COLUMN_OUTPUT,
                    msg
                ))

        return results

