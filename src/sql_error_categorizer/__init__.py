# Hidden, internal use only
from .detectors import BaseDetector as _BaseDetector, Detector as _Detector

# Public API
from .sql_errors import SqlErrors
from .catalog import Catalog, build_catalog, load_json as load_catalog
from .detectors import SyntaxErrorDetector, SemanticErrorDetector, LogicalErrorDetector, ComplicationDetector

def get_errors(query_str: str,
               correct_solutions: list[str] = [],
               catalog: catalog.Catalog = catalog.Catalog(),
               search_path: str = 'public',
               detectors: list[type[_BaseDetector]] = [
                   SyntaxErrorDetector,
                   SemanticErrorDetector,
                   LogicalErrorDetector,
                   ComplicationDetector
                ],
               debug: bool = False) -> set[SqlErrors]:
    '''Detect SQL errors in the given query string.'''
    det = _Detector(query_str, correct_solutions=correct_solutions, catalog=catalog, search_path=search_path, debug=debug)

    for detector in detectors:
        det.add_detector(detector)

    return det.run()


# TODO: rename
def get_errors2(query_str: str, correct_solutions: list[str] = [], dataset_str: str = '',
               db_host: str = 'localhost', db_port: int = 5432, db_user: str = 'postgres', db_password: str = 'password',
               debug: bool = False) -> set[SqlErrors]:
    '''Detect SQL errors in the given query string.'''

    cat = build_catalog(dataset_str, hostname=db_host, port=db_port, user=db_user, password=db_password)

    return get_errors(query_str, correct_solutions=correct_solutions, catalog=cat, search_path=cat.schemas.pop(), debug=debug)


def t(query_file: str = 'q_q.sql', solution_file: str = 'q_s.sql', catalog_file: str = 'tests/datasets/cat_miedema.json') -> _Detector:
    '''Test function, remove before production'''

    with open(query_file) as f:
        query = f.read()
    with open(solution_file) as f:
        solution = f.read()

    cat = load_catalog(catalog_file)

    det = _Detector(query, correct_solutions=[solution], catalog=cat, search_path=cat.schemas.pop() or 'public', debug=True)
    det.add_detector(SyntaxErrorDetector)
    det.add_detector(SemanticErrorDetector)
    det.add_detector(LogicalErrorDetector)
    det.add_detector(ComplicationDetector)

    return det


