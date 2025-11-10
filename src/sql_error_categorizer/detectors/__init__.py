'''SQL error detectors.'''

from .. import catalog
from ..query import Query
from .base import BaseDetector, DetectedError

# exported detectors
from .syntax import SyntaxErrorDetector
from .semantic import SemanticErrorDetector
from .logical import LogicalErrorDetector
from .complications import ComplicationDetector

class Detector:
    '''Manages and runs SQL error detectors on a query.'''
    def __init__(self,
                 query: str,
                 *,
                 search_path: str = 'public',
                 solution_search_path: str = 'public',
                 solutions: list[str] = [],
                 catalog: catalog.Catalog = catalog.Catalog(),
                 detectors: list[type[BaseDetector]] = [],
                 debug: bool = False):
        
        # Context data: they don't need to be parsed again if the query changes
        self.search_path = search_path
        self.solution_search_path = solution_search_path
        self.catalog = catalog
        self.solutions = [Query(sol, catalog=self.catalog, search_path=self.solution_search_path) for sol in solutions]
        self.detectors: list[BaseDetector] = []
        self.debug = debug

        self.set_query(query)

        # NOTE: Add detectors after setting the query to ensure they are correctly initialized
        for detector_cls in detectors:
            self.add_detector(detector_cls)

    def set_query(self, query: str, reason: str | None = None) -> None:
        '''Set a new query, re-parse it, and update all detectors. Doesn't affect detected errors.'''
        
        if self.debug:
            print('=' * 20)
            if reason:
                print(f'Updating query ({reason}):\n{query}')
            else:
                print(f'Updating query:\n{query}')
            print('=' * 20)

        self.query = Query(query, catalog=self.catalog, search_path=self.search_path)

        # Update all detectors with the new query and parse results
        for detector in self.detectors:
            detector.query = self.query
            detector.update_query = lambda new_query, reason=None: self.set_query(new_query, reason)

    def add_detector(self, detector_cls: type[BaseDetector]) -> None:
        '''Add a detector instance to the list of detectors'''

        # Make copies to avoid possible modifications during detection
        # TODO: check if it's needed
        detector = detector_cls(
            query=self.query,
            solutions=self.solutions,
            update_query=lambda new_query, reason=None: self.set_query(new_query, reason),
        )

        self.detectors.append(detector)

    def run(self) -> list[DetectedError]:
        '''
        Run all detectors and return a list of detected errors.
        This function can return duplicate errors, as well as additional information on the detected errors.
        '''

        if self.debug:
            print('===== Query =====')
            print(self.query.sql)

            print('===== search_path =====')
            print(self.search_path)

            print('===== solution_search_path =====')
            print(self.solution_search_path)

            print('===== Solutions =====')
            print('\n-----\n'.join(sol.sql for sol in self.solutions))

            print('===== Catalog =====')
            print(self.catalog)

        results: list[DetectedError] = []

        for detector in self.detectors:
            errors = detector.run()

            if self.debug:
                print(f'===== Detected errors from {detector.__class__.__name__} =====')
                for error in errors:
                    print(error)

            results.extend(errors)

        return results
