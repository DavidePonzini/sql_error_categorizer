import json
from .. import parser, catalog
from .. import tokenizer
from ..sql_errors import SqlErrors
from .base import BaseErrorDetector
# from .syntax import SyntaxErrorDetector

class ErrorDetector:
    def __init__(self, query: str, *,
                 correct_solutions: list = [],
                 catalog: catalog.Catalog = catalog.Catalog(),
                 detectors: list[type[BaseErrorDetector]] = []):
        
        self.query = tokenizer.TokenizedSQL(query)
        self.correct_solutions = correct_solutions
        self.catalog = catalog
        self.detectors: list[BaseErrorDetector] = []

        # Parse the query once here
        try:
            self.parse_result = parser.parse(self.query.sql)
            self.cte_catalog = parser.create_cte_catalog(self.parse_result.cte_map)
        except Exception:
            self.parse_result = parser.ParseResult()
            self.cte_catalog = parser.CTECatalog()

        for detector_cls in detectors:
            self.add_detector(detector_cls)

    def add_detector(self, detector_cls: type[BaseErrorDetector]) -> None:
        detector = detector_cls(
            query=self.query.copy(),    # Make a copy to avoid possible modifications during detection
            catalog=self.catalog.copy(),
            correct_solutions=self.correct_solutions,
            query_map=self.parse_result.query_map,
            subquery_map=self.parse_result.subquery_map,
            cte_map=self.parse_result.cte_map,
            cte_catalog=self.cte_catalog,
        )

        self.detectors.append(detector)


    def run(self) -> set[SqlErrors]:
        result = set()

        for detector in self.detectors:
            errors = detector.run()
            
            for error, _ in errors:
                result.add(error)

        return result
