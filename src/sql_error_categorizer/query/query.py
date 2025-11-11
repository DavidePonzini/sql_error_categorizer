import sqlparse
import sqlparse.tokens


from .set_operations import SetOperation, Select, create_set_operation_tree
from ..catalog import Catalog

from .tokenized_sql import TokenizedSQL

from ..util import OrderByColumn

class Query(TokenizedSQL):
    def __init__(self,
                sql: str,
                *,
                catalog: Catalog = Catalog(),
                search_path: str = 'public'
        ) -> None:
        '''
        Represents a full SQL query, potentially with multiple statements (i.e., CTEs and set operations).
        '''

        super().__init__(sql)

        self.catalog = catalog.copy()
        '''Catalog representing tables that can be referenced in this query.'''
        
        self.search_path = search_path
        '''The search path for resolving unqualified table names.'''

        # Extract CTEs and main query
        self.ctes: list[SetOperation] = []
        cte_tokens = []
        main_query_tokens = []

        is_cte_section = False
        for token in self.parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword.CTE:
                is_cte_section = True
                continue

            if token.ttype is sqlparse.tokens.DML:
                is_cte_section = False

            if not is_cte_section:
                # Collect all tokens for the main query
                main_query_tokens.append(token)
                continue

            # Collect CTE names and their corresponding SQL
            if isinstance(token, sqlparse.sql.IdentifierList):
                cte_tokens.extend(token.get_identifiers())

            elif isinstance(token, sqlparse.sql.Identifier):
                cte_tokens.append(token)

        for cte_token in cte_tokens:
            cte_name = cte_token.get_name()
            
            cte_parenthesis = next(cte_token.get_sublists())
            if not cte_parenthesis:
                continue

            cte_parenthesis_str = str(cte_parenthesis)[1:-1]  # Remove surrounding parentheses
            cte = create_set_operation_tree(cte_parenthesis_str, catalog=self.catalog, search_path=self.search_path)

            self.ctes.append(cte)

            # Add CTE output columns to catalog
            output = cte.output
            output.name = cte_name
            self.catalog[self.search_path][cte_name] = output

        main_query_sql = ''.join(str(token) for token in main_query_tokens).strip()
        self.main_query = create_set_operation_tree(main_query_sql, catalog=self.catalog, search_path=self.search_path)

        self.set_operations_order_by: list[OrderByColumn] = []
        self.set_operations_limit: int | None = None
        self.set_operations_offset: int | None = None

    # region Properties
    @property
    def selects(self) -> list[Select]:
        result: list[Select] = []

        for cte in self.ctes:
            result.extend(cte.selects)

        result.extend(self.main_query.selects)

        return result
    
    # endregion   

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.sql})'
    
    