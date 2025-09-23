from . import catalog, detectors as _detectors
from .sql_errors import SqlErrors

def get_errors(query_str: str,
               correct_solutions: list[str] = [],
               catalog: catalog.Catalog = catalog.Catalog(),
               detectors: list[type[_detectors.BaseDetector]] = [
                   _detectors.SyntaxErrorDetector,
                   _detectors.SemanticErrorDetector,
                   _detectors.LogicalErrorDetector,
                   _detectors.ComplicationDetector
                ],
               debug: bool = False) -> set[SqlErrors]:
    '''Detect SQL errors in the given query string.'''
    det = _detectors.Detector(query_str, correct_solutions=correct_solutions, catalog=catalog, debug=debug)

    for detector in detectors:
        det.add_detector(detector)

    return det.run()


def get_errors2(query_str: str, correct_solutions: list[str] = [], dataset_str: str = '',
               db_host: str = 'localhost', db_port: int = 5432, db_user: str = 'postgres', db_password: str = 'password',
               debug: bool = False) -> set[SqlErrors]:
    '''Detect SQL errors in the given query string.'''

    cat = catalog.build_catalog(dataset_str, hostname=db_host, port=db_port, user=db_user, password=db_password)
    
    return get_errors(query_str, correct_solutions=correct_solutions, catalog=cat, debug=debug)


def t() -> _detectors.Detector:
    with open('q_miedema.sql', 'r') as f:
        miedema = f.read()
    with open('q_q.sql', 'r') as f:
        q = f.read()
    with open('q_s.sql', 'r') as f:
        s = f.read()

    cat = catalog.build_catalog(miedema, hostname='localhost', port=5432, user='postgres', password='password')

    det = _detectors.Detector(q, correct_solutions=[s], catalog=cat, debug=True)
    det.add_detector(_detectors.SyntaxErrorDetector)
    det.add_detector(_detectors.SemanticErrorDetector)
    det.add_detector(_detectors.LogicalErrorDetector)
    det.add_detector(_detectors.ComplicationDetector)

    return det


