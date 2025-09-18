from abc import ABC, abstractmethod
from ..sql_errors import SqlErrors

class BaseErrorDetector(ABC):
    def __init__(self,
        query: str, tokens,
        nl_description: str = '', correct_solutions: list[str] = [],
        catalog=None,
        query_map=None, subquery_map=None, cte_map=None,
        cte_catalog=None,
        debug=False):
        
        self.query = query
        self.nl_description = nl_description
        self.correct_solutions = correct_solutions
        self.tokens = tokens
        
        self.catalog = catalog or {
            'schemas': [], 'tables': [], 'columns': [], 'functions': [],
            'synonyms': {}, 'cte_tables': {}, 'subquery_tables': {}
        }
        self.query_map = query_map or {}
        self.subquery_map = subquery_map or {}
        self.cte_map = cte_map or {}
        self.cte_catalog = cte_catalog or {}
        self.debug = debug

    def _prepare(self):
        '''This method can be overridden by subclasses for additional preparation before running the detector'''
        pass

    @abstractmethod
    def run(self) -> list[tuple[SqlErrors, str]]:
        '''Run the detector and return a list of detected errors with their descriptions'''
        pass
