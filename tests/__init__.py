from typing import Any
from sql_error_categorizer.catalog import load_json as load_catalog, Catalog
from sql_error_categorizer.detectors import Detector, BaseDetector, DetectedError
from sql_error_categorizer.detectors import SyntaxErrorDetector, SemanticErrorDetector, LogicalErrorDetector, ComplicationDetector
from sql_error_categorizer.sql_errors import SqlErrors

def run_test(query: str, *,
             catalog_filename: str | None = None,
             search_path: str = 'public', 
             detectors: list[type[BaseDetector]],
             expected_solutions: list[str] = [],
             debug: bool = False
    ) -> list[DetectedError]:
    
    if catalog_filename:
        catalog = load_catalog(f'tests/datasets/{catalog_filename}')
    else:
        catalog = Catalog()

    detector = Detector(
        query=query,
        solutions=expected_solutions,
        catalog=catalog,
        search_path=search_path,
        solution_search_path=search_path,
        detectors=detectors,
        debug=debug
    )

    return detector._run()

def has_any_error(detected_errors: list[DetectedError], error: SqlErrors) -> bool:
    '''Check if any detected error matches the given error type, regardless of data.'''
    for detected_error in detected_errors:
        if detected_error.error == error:
            return True
    return False

def has_error(detected_errors: list[DetectedError], error: SqlErrors, data: tuple[Any, ...] = ()) -> bool:
    '''Check if any detected error matches the given error type and data.'''
    for detected_error in detected_errors:
        if detected_error.error == error and detected_error.data == data:
            return True
    return False