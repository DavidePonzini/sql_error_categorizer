'''Detect and categorize SQL errors in queries.'''

# Hidden, internal use only
from .detectors import BaseDetector as _BaseDetector, Detector as _Detector

# Public API
from .sql_errors import SqlErrors
from .catalog import Catalog, build_catalog, load_catalog
from .detectors import SyntaxErrorDetector, SemanticErrorDetector, LogicalErrorDetector, ComplicationDetector, DetectedError

def get_errors(query_str: str,
               solutions: list[str] = [],
               catalog: Catalog = Catalog(),
               search_path: str = 'public',
               solution_search_path: str = 'public',
               detectors: list[type[_BaseDetector]] = [
                   SyntaxErrorDetector,
                   SemanticErrorDetector,
                   LogicalErrorDetector,
                   ComplicationDetector
                ],
               debug: bool = False) -> list[DetectedError]:
    '''Detect SQL errors in the given query string.'''
    det = _Detector(query_str,
                    solutions=solutions,
                    catalog=catalog,
                    search_path=search_path,
                    solution_search_path=solution_search_path,
                    debug=debug)

    for detector in detectors:
        det.add_detector(detector)

    return det.run()

def get_error_types(query_str: str,
                    solutions: list[str] = [],
                    catalog: Catalog = Catalog(),
                    search_path: str = 'public',
                    solution_search_path: str = 'public',
                    detectors: list[type[_BaseDetector]] = [
                        SyntaxErrorDetector,
                        SemanticErrorDetector,
                        LogicalErrorDetector,
                        ComplicationDetector
                    ],
                    debug: bool = False) -> set[SqlErrors]:
    '''Detect SQL error types in the given query string.'''

    detected_errors = get_errors(query_str,
                                 solutions=solutions,
                                 catalog=catalog,
                                 search_path=search_path,
                                 solution_search_path=solution_search_path,
                                 detectors=detectors,
                                 debug=debug)
    
    return {error.error for error in detected_errors}
