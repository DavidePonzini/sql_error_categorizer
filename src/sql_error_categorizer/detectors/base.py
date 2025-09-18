from abc import ABC, abstractmethod
from ..sql_errors import SqlErrors
from ..tokenizer import TokenizedSQL
from ..catalog import Catalog
from ..parser import QueryMap, SubqueryMap, CTEMap, CTECatalog

class BaseErrorDetector(ABC):
    def __init__(self,
                 query: TokenizedSQL,
                 catalog: Catalog,
                 query_map: QueryMap, subquery_map: SubqueryMap, cte_map: CTEMap, cte_catalog: CTECatalog,
                 correct_solutions: list[str] = [],
        ):        
        self.query = query
        self.catalog = catalog
        self.query_map = query_map
        self.subquery_map = subquery_map
        self.cte_map = cte_map
        self.cte_catalog = cte_catalog
        self.correct_solutions = correct_solutions

    def _prepare(self):
        '''This method can be overridden by subclasses for additional preparation before running the detector'''
        pass

    @abstractmethod
    def run(self) -> list[tuple[SqlErrors, str]]:
        '''Run the detector and return a list of detected errors with their descriptions'''
        pass
