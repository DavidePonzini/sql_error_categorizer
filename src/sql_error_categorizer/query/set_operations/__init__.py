from .set_operation import SetOperation
from .binary_set_operation import BinarySetOperation, Union, Intersect, Except
from .select import Select

from ...catalog import Catalog

import sqlparse
from sqlparse.sql import TokenList, Parenthesis
from sqlparse.tokens import Whitespace, Newline, Keyword

def create_set_operation_tree(sql: str, catalog: Catalog = Catalog(), search_path: str = 'public') -> SetOperation:
    '''
    Parses a SQL string and constructs a tree of SetOperation objects representing the query structure using sqlparse.

    Args:
        sql (str): The SQL query string to parse.
        catalog (Catalog): The database catalog for resolving table and column names.
        search_path (str): The search path for schema resolution.

    Returns:
        SetOperation: The root of the SetOperation tree representing the query.
    '''

    def is_ws(tok):
        return tok.ttype in (Whitespace, Newline)

    def parse_op_token(tok) -> tuple[str, bool | None] | None:
        '''Parse "UNION", "INTERSECT", "EXCEPT" with optional inline ALL/DISTINCT.
        Returns (op, all_flag) where all_flag is:
            - True  if ALL inline (e.g., "UNION ALL")
            - False if DISTINCT inline (e.g., "EXCEPT DISTINCT")
            - None  if no modifier inline (so caller may look right).'''
        if tok.ttype is not Keyword:
            return None
        parts = tok.normalized.upper().split()
        if not parts:
            return None

        op = parts[0]
        if op not in ('UNION', 'INTERSECT', 'EXCEPT'):
            return None

        if len(parts) > 1:
            if parts[1] == 'ALL':
                return (op, True)
            if parts[1] == 'DISTINCT':
                return (op, False)

        return (op, None)

    def split_on(tokens, idx, all_in_token):
        '''Splits around the operator at idx. If the modifier wasn't inline,
        consume a single immediate ALL/DISTINCT to the right.'''
        left_tokens = tokens[:idx]
        right_tokens = tokens[idx + 1:]

        # trim ws
        while right_tokens and is_ws(right_tokens[0]):
            right_tokens = right_tokens[1:]

        all_flag = all_in_token  # True=ALL, False=DISTINCT, None=unspecified
        if all_flag is None and right_tokens and right_tokens[0].ttype is Keyword:
            kw = right_tokens[0].normalized.upper()
            if kw in ('ALL', 'DISTINCT'):
                all_flag = (kw == 'ALL')  # DISTINCT => False
                right_tokens = right_tokens[1:]
                while right_tokens and is_ws(right_tokens[0]):
                    right_tokens = right_tokens[1:]

        left_sql = TokenList(left_tokens).value.strip()
        right_sql = TokenList(right_tokens).value.strip()
        return left_sql, right_sql, all_flag

    def find_top_level_ops(tokens) -> list[tuple[int, str, bool]]:
        '''
        Finds top-level set operation tokens (UNION, INTERSECT, EXCEPT) in the token list.
        
        Returns a list of tuples (index, operation, all_flag).
        '''
        
        ops = []
        depth = 0
        for i, tok in enumerate(tokens):
            if isinstance(tok, Parenthesis):
                continue
            val = tok.value or ''
            depth += val.count('(')
            depth -= val.count(')')
            if depth == 0:
                parsed = parse_op_token(tok)
                if parsed:
                    op, all_flag = parsed
                    ops.append((i, op, all_flag))
        return ops

    # def strip_trailing_clauses(tokens) -> tuple[list, str | None]:
    #     '''Separate trailing ORDER BY / LIMIT / OFFSET so they can be attached to the root
        
    #     Returns (main_tokens, trailing_sql)'''
    #     order_clause = []
    #     limit_clause = []
    #     offset_clause = []

    #     kws = {'ORDER', 'BY', 'LIMIT', 'OFFSET'}
    #     for i, tok in enumerate(tokens):
    #         if tok.ttype is Keyword and tok.value.upper().split()[0] in kws:
    #             return tokens[:i], TokenList(tokens[i:]).value.strip()
    #     return tokens, None

    parsed = sqlparse.parse(sql)
    if not parsed:
        return Select(sql, catalog=catalog, search_path=search_path)

    statement = parsed[0]
    tokens = statement.tokens

    # Strip trailing ORDER BY / LIMIT / OFFSET
    main_tokens = tokens
    # main_tokens, trailing = strip_trailing_clauses(tokens)

    top_ops = find_top_level_ops(main_tokens)
    if not top_ops:
        node = Select(TokenList(main_tokens).value.strip(), catalog=catalog, search_path=search_path)
        # if trailing:
        #     node.trailing_sql = trailing  # optional attribute for ORDER/LIMIT
        return node

    # Precedence: split lowest-precedence first (UNION/EXCEPT) so INTERSECT stays grouped
    union_except = [(i, op, a) for (i, op, a) in top_ops if op in ('UNION', 'EXCEPT')]
    if union_except:
        split_idx, op, all_in_token = union_except[0]
    else:
        split_idx, op, all_in_token = top_ops[0]  # only INTERSECTs remain

    left_sql, right_sql, all_kw = split_on(main_tokens, split_idx, all_in_token)

    left_node  = create_set_operation_tree(left_sql,  catalog=catalog, search_path=search_path)
    right_node = create_set_operation_tree(right_sql, catalog=catalog, search_path=search_path)

    if op == 'UNION':
        node = Union(sql, left_node, right_node, all=(all_kw is True))
    elif op == 'EXCEPT':
        node = Except(sql, left_node, right_node, all=(all_kw is True))
    else:  # INTERSECT
        node = Intersect(sql, left_node, right_node, all=(all_kw is True))

    # if trailing:
    #     node.trailing_sql = trailing  # apply ORDER BY / LIMIT / OFFSET at top level

    return node
