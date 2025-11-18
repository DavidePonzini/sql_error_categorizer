from typing import Any
from sql_error_categorizer import SqlErrors, Catalog, load_catalog
from sql_error_categorizer.detectors import Detector, BaseDetector, DetectedError
from sql_error_categorizer import SyntaxErrorDetector, SemanticErrorDetector, LogicalErrorDetector, ComplicationDetector

def run_test(query: str, *,
             catalog_filename: str | None = None,
             search_path: str | None = None, 
             detectors: list[type[BaseDetector]],
             solutions: list[str] = []
    ) -> list[DetectedError]:
    
    if catalog_filename:
        catalog = load_catalog(f'datasets/catalogs/{catalog_filename}.json')
    else:
        catalog = Catalog()

    if search_path is None:
        search_path = 'public'

    detector = Detector(
        query=query,
        solutions=solutions,
        catalog=catalog,
        search_path=search_path,
        solution_search_path=search_path,
        detectors=detectors,
        debug=True
    )

    return detector.run()

def has_error(detected_errors: list[DetectedError], error: SqlErrors, data: tuple[Any, ...] = ()) -> bool:
    '''Check if any detected error matches the given error type and data.'''
    for detected_error in detected_errors:
        if detected_error.error == error and detected_error.data == data:
            return True
    return False

def count_errors(detected_errors: list[DetectedError], error: SqlErrors) -> int:
    '''Count how many detected errors match the given error type.'''
    count = 0
    for detected_error in detected_errors:
        if detected_error.error == error:
            count += 1
    return count