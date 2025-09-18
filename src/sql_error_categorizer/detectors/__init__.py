import json
from .. import parser, catalog
from .. import tokenizer
from ..sql_errors import SqlErrors
from .base import BaseErrorDetector
from .syntax import SyntaxErrorDetector

class ErrorDetector:
    def __init__(self, query: str, *,
                 nl_description: str = '',
                 correct_solutions: list = [],
                 catalog: catalog.Catalog = catalog.Catalog(),
                 debug: bool = False,
                 detectors: list[type[BaseErrorDetector]] = []):
        
        self.query = query
        self.nl_description = nl_description
        self.correct_solutions = correct_solutions
        self.catalog = catalog.copy()  # Make a copy to avoid modifying the original catalog on subsequent calls
        self.debug = debug
        self.detectors: list[BaseErrorDetector] = []

        if self.debug:
            print('\n' + '='*20 + ' DEBUG INFO FOR QUERY ' + '='*20)
            print(f'Query: {self.query.strip()}')
            print('\n--- QUERY-SPECIFIC CATALOG ---')
            print(json.dumps(self.catalog, indent=2, default=str))
            print('-----------------------------------------------------\n')

        # Parse the query once here
        if self.query:
            try:
                self.parse_result = parser.parse(self.query)
                self.cte_catalog = parser.create_cte_catalog(self.parse_result.cte_map)
            except Exception as e:
                if self.debug:
                    print(f'DEBUG: Error during parsing: {e}')
                
                self.query_map = parser.QueryMap()
                self.subquery_map = parser.SubqueryMap()
                self.cte_map = parser.CTEMap()
                self.cte_catalog = parser.CTECatalog()
        else:
            if self.debug:
                print('DEBUG: No query provided to parse')

            self.query_map = parser.QueryMap()
            self.subquery_map = parser.SubqueryMap()
            self.cte_map = parser.CTEMap()
            self.cte_catalog = parser.CTECatalog()
    
        self.tokens = tokenizer.tokenize(query)

        for detector_cls in detectors:
            self.add_detector(detector_cls)

    def add_detector(self, detector_cls: type[BaseErrorDetector]) -> None:
        detector = detector_cls(
            tokens=self.tokens,
            catalog=self.catalog,
            query=self.query,
            nl_description=self.nl_description,
            correct_solutions=self.correct_solutions,
            query_map=self.query_map,
            subquery_map=self.subquery_map,
            cte_map=self.cte_map,
            cte_catalog=self.cte_catalog,
            debug=self.debug
        )

        self.detectors.append(detector)


    def run(self) -> set[SqlErrors]:
        result = set()

        for detector in self.detectors:
            errors = detector.run()
            
            for error, _ in errors:
                result.add(error)

        return result
