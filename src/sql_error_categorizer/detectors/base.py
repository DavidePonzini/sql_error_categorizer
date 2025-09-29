from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

from ..sql_errors import SqlErrors
from ..tokenizer import TokenizedSQL
from ..catalog import Catalog
from ..parser import QueryMap, SubqueryMap, CTEMap, CTECatalog

@dataclass(repr=False)
class DetectedError:
    error: SqlErrors
    data: tuple[Any, ...] = field(default_factory=tuple)

    def __repr__(self):
        return f"DetectedError({self.error.value} - {self.error.name}: {self.data})"
    
    def __str__(self) -> str:
        return f'[{self.error.value:3}] {self.error.name}: {self.data}]'

class BaseDetector(ABC):
    def __init__(self, *,
                 query: TokenizedSQL,
                 catalog: Catalog,
                 search_path: str = '',
                 query_map: QueryMap,
                 subquery_map: SubqueryMap,
                 cte_map: CTEMap,
                 cte_catalog: CTECatalog,
                 update_query: Callable[[str], None],
                 correct_solutions: list[str] = [],
        ):        
        self.query = query
        self.catalog = catalog
        self.search_path = search_path
        self.query_map = query_map
        self.subquery_map = subquery_map
        self.cte_map = cte_map
        self.cte_catalog = cte_catalog
        self.update_query = update_query
        self.correct_solutions = correct_solutions

    @abstractmethod
    def run(self) -> list[DetectedError]:
        '''Run the detector and return a list of detected errors with their descriptions'''
        return []
    
    @staticmethod
    def _normalize(identifier: str) -> str:
        '''Normalize an SQL identifier by stripping quotes and converting to lowercase if unquoted.'''
        if identifier.startswith('"') and identifier.endswith('"') and len(identifier) > 1:
            return identifier[1:-1]
        
        return identifier.lower()