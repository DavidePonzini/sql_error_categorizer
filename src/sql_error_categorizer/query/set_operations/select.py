from ...query.util import remove_parentheses
from .set_operation import SetOperation
from ..tokenized_sql import TokenizedSQL
from .. import extractors
from ...catalog import Catalog, Table, UniqueConstraintType, UniqueConstraintColumn
from ...catalog.catalog import UniqueConstraint
from ...util import *
from ..util import extract_CNF
from ..typechecking import get_type, to_res_type

import sqlglot
import sqlglot.errors
from sqlglot import exp
import re
from copy import deepcopy
import itertools

class Select(SetOperation, TokenizedSQL):
    '''Represents a single SQL SELECT statement.'''

    def __init__(self,
                 query: str,
                 *,
                 catalog: Catalog = Catalog(),
                 search_path: str = 'public',
                 subquery_level: int = 0,
        ) -> None:
        '''
        Initializes a SelectStatement object.

        Args:
            query (str): The SQL query string to tokenize.
            catalog_db (Catalog): The database catalog for resolving table and column names.
            catalog_query (Catalog): The query-specific catalog for resolving table and column names.
            search_path (str): The search path for schema resolution.
            parent (TokenizedSQL | None): The parent TokenizedSQL object if this is a subquery.
        '''

        SetOperation.__init__(self, query, subquery_level=subquery_level)
        TokenizedSQL.__init__(self, query)

        self.catalog = catalog
        '''Catalog representing tables that can be referenced in this query.'''
        
        self.search_path = search_path

        self._subqueries = None
        self._referenced_tables = None
        '''Catalog representing tables that are read by this query.'''

        try:
            self.ast = sqlglot.parse_one(self.sql)
        except sqlglot.errors.ParseError:
            self.ast = None  # Empty expression on parse error        
        
    # region Auxiliary
    def _get_referenced_tables(self) -> list[Table]:
        '''Extracts referenced tables from the SQL query and returns them as a Catalog object.'''

        result: list[Table] = []

        if not self.ast:
            return result

        def add_tables_from_expression(expr: exp.Expression) -> None:
            '''Recursively adds tables from an expression to the result catalog.'''
            # Subquery: get its output columns
            if isinstance(expr, exp.Subquery):
                table_name_out = normalize_ast_subquery_name(expr)

                subquery_sql = expr.this.sql()
                subquery = Select(subquery_sql, catalog=self.catalog, search_path=self.search_path)

                result.append(subquery.output)
            
            # Table: look it up in the IN catalog
            elif isinstance(expr, exp.Table):
                # schema name
                schema_name = normalize_ast_schema_name(expr)
                table_name_in = normalize_ast_table_real_name(expr)
                table_name_out = normalize_ast_table_name(expr)

                if schema_name is None:
                    # If no schema is specified, try to find the table in the CTEs
                    if self.catalog.has_table(schema_name='', table_name=table_name_in):
                        schema_name = ''
                    # If not found in CTEs, use the search path
                    else:
                        schema_name = self.search_path 

                # check if the table exists in the catalog
                if self.catalog.has_table(schema_name=schema_name, table_name=table_name_in):
                    # Table exists
                    table_in = self.catalog[schema_name][table_name_in]

                    # Create a copy of the table with the output name
                    table = deepcopy(table_in)
                    table.name = table_name_out
                    result.append(table)
                else:
                    # Table does not exist, add as empty table
                    result.append(Table(name=table_name_out))
        
        from_expr = self.ast.args.get('from')
        

        if from_expr:
            add_tables_from_expression(from_expr.this)
            
        for join in self.ast.args.get('joins', []):
            add_tables_from_expression(join.this)

        return result

    # endregion

    def strip_subqueries(self, replacement: str = 'NULL') -> 'Select':
        '''Returns the SQL query with all subqueries removed (replaced by a context-aware placeholder).'''

        stripped_sql = self.sql

        subquery_sqls = extractors.extract_subqueries_tokens(self.sql)

        counter = 1
        for subquery_sql, clause in subquery_sqls:
            repl = replacement  # default safe fallback

            clause_upper = (clause or '').upper()

            if clause_upper in ('FROM', 'JOIN'):
                repl = f'__subq{counter}'
                counter += 1
            elif clause_upper in ('WHERE', 'HAVING', 'ON', 'SELECT', 'COMPARISON'):
                repl = replacement
            elif clause_upper in ('IN', 'EXISTS'):
                repl = f'({replacement})'

            escaped = re.escape(subquery_sql)
            pattern = rf'\(\s*{escaped}\s*\)'

            # Replace the parentheses and enclosed subquery entirely
            stripped_sql, n = re.subn(pattern, repl, stripped_sql, count=1)

            # Fallback: if not found with parentheses, remove raw subquery text
            if n == 0:
                stripped_sql = re.sub(escaped, repl, stripped_sql, count=1)

        return Select(stripped_sql, catalog=self.catalog, search_path=self.search_path)

    def get_join_conditions(self) -> list[exp.Expression]:
        '''Returns a list of join conditions used in the main query.'''
        if not self.ast:
            return []

        join_conditions = []
        for join in self.ast.args.get('joins', []):
            on_condition = join.args.get('on')
            if on_condition:
                join_conditions.append(on_condition)
        
        return join_conditions
    
    def get_join_equalities(self) -> list[tuple[exp.Column, exp.Column]]:
        '''Returns a list of join equality conditions used in the main query.'''
        result: list[tuple[exp.Column, exp.Column]] = []

        def extract_column_equalities(expr: exp.Expression) -> list[tuple[exp.Column, exp.Column]]:
            equalities = []
            conjuncts = extract_CNF(expr)
            for conj in conjuncts:
                if isinstance(conj, exp.EQ):
                    left = conj.args.get('this')
                    right = conj.args.get('expression')
                    if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                        equalities.append((left, right))
            return equalities

        for join_condition in self.get_join_conditions():
            result.extend(extract_column_equalities(join_condition))

        if self.where:
            result.extend(extract_column_equalities(self.where))

        return result

    # region Properties
    @property
    def subqueries(self) -> list[tuple['Select', str]]:
        '''
            Returns a list of subqueries as TokenizedSQL objects.
        
            Returns:
                list[tuple[Select, str]]: A list of tuples containing subquery Select objects and their associated clause.
        '''
        if self._subqueries is None:
            self._subqueries = []
            # try to find subqueries via sqlglot AST, since it's more reliable
            # if self.ast:
            #     subquery_asts = extractors.extract_subqueries_ast(self.ast)
            #     for subquery_ast in subquery_asts:
            #         while not isinstance(subquery_ast, exp.Select):
            #             subquery_ast = subquery_ast.this
            #         subquery = Select(subquery_ast.sql(), catalog=self.catalog, search_path=self.search_path)
            #         self._subqueries.append(subquery)
            # else:
                # fallback: AST cannot be constructed, try to find subqueries via sqlparse
            subquery_sqls = extractors.extract_subqueries_tokens(self.sql)

            for subquery_sql, clause in subquery_sqls:
                subquery = Select(subquery_sql, catalog=self.catalog, search_path=self.search_path)
                self._subqueries.append((subquery, clause))
    
        return self._subqueries

    @property
    def distinct(self) -> bool:
        '''Returns True if the main query has a DISTINCT clause.'''
        if self.ast and self.ast.args.get('distinct', False):
            return True
        return False
    
    @property
    def referenced_tables(self) -> list[Table]:
        '''Returns a list of tables that are referenced in the SQL query.'''

        if self._referenced_tables is None:
            self._referenced_tables = self._get_referenced_tables()
        
        return self._referenced_tables
    

    @property
    def output(self) -> Table:
        '''
        Returns a Table object representing the output of this SELECT query.
        It includes inferred columns (name, type, nullability, constancy)
        and merged unique constraints from referenced tables.
        '''
        result = super().output
        if not self.ast:
            return result

        anonymous_counter = 1

        # ----------------------------------------------------------------------
        # Helper functions
        # ----------------------------------------------------------------------

        def get_anonymous_column_name() -> str:
            '''Generates a unique anonymous column name.'''
            nonlocal anonymous_counter
            name = f'?column_{anonymous_counter}?'
            anonymous_counter += 1
            return name

        def add_star() -> None:
            '''Expand SELECT * by adding all columns from all referenced tables.'''
            for idx, table in enumerate(self.referenced_tables):
                for col in table.columns:
                    result.add_column(
                        name=col.name,
                        table_idx=idx,
                        column_type=to_res_type(col.column_type).value,
                        is_nullable=col.is_nullable,
                        is_constant=col.is_constant
                    )

        def add_alias(column: exp.Alias) -> None:
            '''Add an expression with an explicit alias (e.g. SELECT expr AS alias).'''
            alias = column.args['alias']
            name = alias.this if alias.quoted else alias.this.lower()
            res_type = get_type(column.this, self.referenced_tables)[0]

            result.add_column(
                name=name,
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def add_table_star(column: exp.Column) -> None:
            '''Add all columns from a specific table (SELECT table.*).'''
            table_name = normalize_ast_column_table(column)
            table = next((t for t in self.referenced_tables if t.name == table_name), None)
            if table:
                for col in table.columns:
                    res_type = get_type(col, self.referenced_tables)[0]
                    result.add_column(
                        name=col.name,
                        table_idx=self.referenced_tables.index(table),
                        column_type=res_type.type.value,
                        is_nullable=res_type.nullable,
                        is_constant=res_type.constant
                    )

        def add_column(column: exp.Column) -> None:
            '''Add a column reference (SELECT column or table.column).'''
            table_name = normalize_ast_column_table(column)
            col_name = column.alias_or_name
            name = col_name if column.this.quoted else col_name.lower()

            # Resolve which table this column belongs to
            if table_name:
                table_idx = next((i for i, t in enumerate(self.referenced_tables) if t.name == table_name), None)
            else:
                table_idx = next((i for i, t in enumerate(self.referenced_tables)
                                if any(c.name == name for c in t.columns)), None)

            res_type = get_type(column, self.referenced_tables)[0]
            
            result.add_column(
                name=name,
                table_idx=table_idx,
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def add_subquery(column: exp.Subquery) -> None:
            '''Add a column derived from a subquery expression (SELECT (SELECT ...)).'''
            subq = Select(remove_parentheses(column.sql()), catalog=self.catalog, search_path=self.search_path)
            
            # Add the first column of the subquery's output
            if subq.output.columns:
                first_col = subq.output.columns[0]
                res_type = get_type(first_col, self.referenced_tables)[0]
            
                result.add_column(
                    name=first_col.name,
                    column_type=res_type.type.value,
                    is_nullable=res_type.nullable,
                    is_constant=res_type.constant
                )
            else:
                result.add_column(name=get_anonymous_column_name(), column_type='None')

        def add_literal(column: exp.Literal | exp.Expression) -> None:
            '''Add a literal or computed expression as a pseudo-column (e.g. SELECT 1, SELECT a+b).'''
            res_type = get_type(column, self.referenced_tables)[0]

            result.add_column(
                name=get_anonymous_column_name(),
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def add_function(column: exp.Func) -> None:
            '''Add a function output column (e.g. SELECT MAX(col)).'''
            res_type = get_type(column, self.referenced_tables)[0]

            result.add_column(
                name=get_anonymous_column_name(),
                column_type=res_type.type.value,
                is_nullable=res_type.nullable,
                is_constant=res_type.constant
            )

        def merge_unique_constraints() -> list[UniqueConstraint]:
            '''
            Merge unique constraints from all referenced tables.
            When multiple tables are joined, constraints are combined
            by unioning column sets across participating tables.
            '''
            result: list[UniqueConstraint] = []

            tables = self.referenced_tables
            if not tables:
                return result

            all_constraints = [t.unique_constraints for t in tables]
            
            # Assign base table index
            for constraint in all_constraints[0]:
                c = UniqueConstraint(set(), UniqueConstraintType.UNIQUE)
                result.append(c)
                
                for constraint_column in constraint.columns:
                    c.columns.add(UniqueConstraintColumn(constraint_column.name, table_idx=0))


            # Combine constraints across tables (Cartesian merge)
            for table_idx, constraints in enumerate(all_constraints[1:], start=1):
                merged: list[UniqueConstraint] = []

                for c1 in result:
                    for c2 in constraints:
                        c = UniqueConstraint(set(), UniqueConstraintType.UNIQUE)

                        for constraint_column in c2.columns:
                            c.columns.add(UniqueConstraintColumn(constraint_column.name, table_idx=table_idx))
                        merged.append(UniqueConstraint(c1.columns.union(c.columns), UniqueConstraintType.UNIQUE))
                
                result = merged

            return result

        def build_equality_groups(equalities: list[tuple[UniqueConstraintColumn, UniqueConstraintColumn]]) -> list[set[UniqueConstraintColumn]]:
            '''Given a list of equality pairs, return transitive closure groups.'''
            parent = {}

            def find(x):
                while parent[x] != x:
                    parent[x] = parent[parent[x]]
                    x = parent[x]
                return x

            def union(x, y):
                rx, ry = find(x), find(y)
                if rx != ry:
                    parent[ry] = rx

            # Initialize all columns
            for l, r in equalities:
                parent.setdefault(l, l)
                parent.setdefault(r, r)

            # Union connected columns
            for l, r in equalities:
                union(l, r)

            # Group by representative
            groups = {}
            for col in parent:
                root = find(col)
                groups.setdefault(root, set()).add(col)

            return list(groups.values())

        def filter_valid_constraints() -> list[UniqueConstraint]:
            constraints: list[UniqueConstraint] = []

            if self.distinct:
                # If DISTINCT is present, the entire output is unique -> add a new constraint, don't discard existing ones
                
                uc_cols = { UniqueConstraintColumn(col.name, col.table_idx) for col in result.columns }
                constraints.append(UniqueConstraint(uc_cols, UniqueConstraintType.UNIQUE))

            all_constraints = merge_unique_constraints()
            
            if self.group_by:
                # If GROUP BY is present, treat grouped columns as unique.
                # Keep existing constraints that are subsets of the grouped columns.

                group_by_cols: set[UniqueConstraintColumn] = set()
                for col in self.group_by:
                    if isinstance(col, exp.Column):
                        table_name = normalize_ast_column_table(col)
                        col_name = col.alias_or_name
                        name = col_name if col.this.quoted else col_name.lower()

                        # Resolve which table this column belongs to
                        if table_name:
                            table_idx = next((i for i, t in enumerate(self.referenced_tables) if t.name == table_name), None)
                        else:
                            table_idx = next((i for i, t in enumerate(self.referenced_tables)
                                            if any(c.name == name for c in t.columns)), None)
                        
                        group_by_cols.add(UniqueConstraintColumn(name, table_idx))

                # Add GROUP BY constraint
                all_constraints.append(UniqueConstraint(group_by_cols, UniqueConstraintType.UNIQUE))

            equalities = self.get_join_equalities()
            if equalities:
                def resolve(col):
                    table_name = normalize_ast_column_table(col)
                    col_name = col.alias_or_name
                    name = col_name if col.this.quoted else col_name.lower()
                    if table_name:
                        table_idx = next((i for i, t in enumerate(self.referenced_tables) if t.name == table_name), None)
                    else:
                        table_idx = next((i for i, t in enumerate(self.referenced_tables)
                                        if any(c.name == name for c in t.columns)), None)
                    return UniqueConstraintColumn(name, table_idx)
                
                # Normalize all equalities as UniqueConstraintColumns
                uc_equalities = [(resolve(left_col), resolve(right_col)) for left_col, right_col in equalities]

                # Compute transitive closure (equivalence classes)
                equality_groups = build_equality_groups(uc_equalities)

                from dav_tools import messages
                messages.debug(f'Equality groups: {equality_groups}')

                # For each constraint, if it contains any member of a group, extend it with the others
                new_constraints: list[UniqueConstraint] = []
                for constraint in all_constraints:
                    expanded_columns = set(constraint.columns)
                    for group in equality_groups:
                        if not expanded_columns.isdisjoint(group):
                            expanded_columns |= group
                    new_constraints.append(UniqueConstraint(expanded_columns, constraint.constraint_type))
                all_constraints = new_constraints

                # Merge overlapping sets
                new_constraints: list[UniqueConstraint] = []
                for equality_group in equality_groups:
                    for col in equality_group:
                        for constraint in all_constraints:
                            if equality_group.isdisjoint(constraint.columns):
                                continue

                            # For each column in the equality group, create a new constraint with all equivalences replaced by that column
                            new_constraints.append(UniqueConstraint(constraint.columns - equality_group | { col }, constraint.constraint_type))

                all_constraints = new_constraints

            # Keep only constraints that are valid for the output columns
            for unique_constraint in all_constraints:
                valid = all(
                    any(output_column.table_idx == constraint_column.table_idx and output_column.name == constraint_column.name
                        for output_column in result.columns if output_column.table_idx is not None)
                    for constraint_column in unique_constraint.columns
                )
                if valid:
                    constraints.append(unique_constraint)

            return constraints

        # ----------------------------------------------------------------------
        # Main column extraction loop
        # ----------------------------------------------------------------------

        for expr in self.ast.expressions:
            if isinstance(expr, exp.Star):
                add_star()
            elif isinstance(expr, exp.Alias):
                add_alias(expr)
            elif isinstance(expr, exp.Column):
                # Handle SELECT table.* separately
                if isinstance(expr.this, exp.Star):
                    add_table_star(expr)
                else:
                    add_column(expr)
            elif isinstance(expr, exp.Subquery):
                add_subquery(expr)
            elif isinstance(expr, exp.Literal):
                add_literal(expr)
            elif isinstance(expr, exp.Func):
                add_function(expr)
            else:
                add_literal(expr)  # fallback for other expressions

        # ----------------------------------------------------------------------
        # Merge and attach unique constraints
        # ----------------------------------------------------------------------

        result.unique_constraints = filter_valid_constraints()


        return result
    
    @property
    def where(self) -> exp.Expression | None:
        if not self.ast:
            return None
        where = self.ast.args.get('where')
        if not where:
            return None
        
        return where.this

    @property
    def group_by(self) -> list[exp.Expression]:
        if not self.ast:
            return []
        group = self.ast.args.get('group')
        if not group:
            return []
        
        return group.expressions

    @property
    def having(self) -> exp.Expression | None:
        if not self.ast:
            return None
        having = self.ast.args.get('having')
        if not having:
            return None
        
        return having.this
    
    @property
    def order_by(self) -> list[exp.Expression]:
        if not self.ast:
            return []
        order = self.ast.args.get('order')
        if not order:
            return []

        return order.expressions


    @property
    def limit(self) -> int | None:
        if not self.ast:
            return None
        limit_exp = self.ast.args.get('limit')
        if not limit_exp:
            return None
        try:
            return int(limit_exp.expression.this)
        except ValueError:
            return None
        
    @property
    def offset(self) -> int | None:
        if not self.ast:
            return None
        offset_exp = self.ast.args.get('offset')
        if not offset_exp:
            return None
        try:
            return int(offset_exp.expression.this)
        except ValueError:
            return None

    # def get_output_types(self, catalog: Catalog = Catalog()) -> list[str]:
    #     '''
    #     Returns a list of output column types from the main query.
        
    #     Returns:
    #         list[str]: A list of output column types.
    #     '''
    #     columns = self.ast.expressions if self.ast else []

    #     types = []
    #     for col in columns:

    # endregion


    def print_tree(self, pre: str = '') -> None:
        if self.parsed:
            self.parsed._pprint_tree(_pre=pre)

    def __repr__(self, pre: str = '') -> str:
        return f'{pre}{self.__class__.__name__}(SQL="{self.sql.splitlines()[0]}{"..." if len(self.sql.splitlines()) > 1 else ""}")'
    
    @property
    def selects(self) -> list['Select']:
        return [self] + [subquery for subquery, _ in self.subqueries]

