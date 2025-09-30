from .. import parser, catalog
from .. import tokenizer
from ..sql_errors import SqlErrors
from .base import BaseDetector
from .syntax import SyntaxErrorDetector
from .semantic import SemanticErrorDetector
from .logical import LogicalErrorDetector
from .complications import ComplicationDetector

class Detector:
    def __init__(self, query: str, *,
                 search_path: str = '',
                 correct_solutions: list = [],
                 catalog: catalog.Catalog = catalog.Catalog(),
                 detectors: list[type[BaseDetector]] = [],
                 debug: bool = False):
        
        # Context data: they don't need to be parsed again if the query changes
        self.search_path = search_path
        self.correct_solutions = correct_solutions
        self.catalog = catalog
        self.detectors: list[BaseDetector] = []
        self.debug = debug

        self.set_query(query)

        # NOTE: Add detectors after setting the query to ensure they are correctly initialized
        for detector_cls in detectors:
            self.add_detector(detector_cls)

    def set_query(self, query: str) -> None:
        '''Set a new query, re-parse it, and update all detectors. Doesn't affect detected errors.'''
        
        if self.debug:
            print('=' * 20)
            print(f'Updating query:\n{query}')
            print('=' * 20)

        self.query = tokenizer.TokenizedSQL(query)
        try:
            self.parse_result = parser.parse(self.query.sql)
            self.cte_catalog = parser.create_cte_catalog(self.parse_result.cte_map)
        except Exception:
            self.parse_result = parser.ParseResult()
            self.cte_catalog = parser.CTECatalog()

        # Update all detectors with the new query and parse results
        for detector in self.detectors:
            detector.query = self.query
            detector.catalog = self.catalog
            detector.query_map = self.parse_result.query_map
            detector.subquery_map = self.parse_result.subquery_map
            detector.cte_map = self.parse_result.cte_map
            detector.cte_catalog = self.cte_catalog
            detector.update_query = lambda new_query: self.set_query(new_query)
            detector.correct_solutions = self.correct_solutions

    def add_detector(self, detector_cls: type[BaseDetector]) -> None:
        '''Add a detector instance to the list of detectors'''

        # Make copies to avoid possible modifications during detection
        # TODO: check if it's needed
        detector = detector_cls(
            query=self.query,
            catalog=self.catalog,
            search_path=self.search_path,
            query_map=self.parse_result.query_map,
            subquery_map=self.parse_result.subquery_map,
            cte_map=self.parse_result.cte_map,
            cte_catalog=self.cte_catalog,
            update_query=lambda new_query: self.set_query(new_query),
            correct_solutions=self.correct_solutions,
        )

        self.detectors.append(detector)


    def run(self) -> set[SqlErrors]:
        '''Run all detectors and return detected error types'''

        if self.debug:
            print('===== Query =====')
            print(self.query.sql)

        result = set()

        for detector in self.detectors:
            errors = detector.run()

            if self.debug:
                print(f'===== Detected errors from {detector.__class__.__name__} =====')
                for error in errors:
                    print(error)

            for error in errors:
                result.add(error.error)

        return result
