from . import catalog, detector
from .sql_errors import SqlErrors

def get_errors(query_str: str, correct_solutions: list[str] = [], dataset_str: str = '',
               db_host: str = 'localhost', db_port: int = 5432, db_user: str = 'postgres', db_password: str = 'password',
               debug: bool = False) -> set[SqlErrors]:
    '''Detect SQL errors in the given query string.'''

    cat = catalog.build_catalog(dataset_str, hostname=db_host, port=db_port, user=db_user, password=db_password)
    det = detector.ErrorDetector(query_str, correct_solutions=correct_solutions, catalog=cat, debug=debug)

    return det.run()


def t() -> detector.ErrorDetector:
    with open('q_miedema.sql', 'r') as f:
        miedema = f.read()
    with open('q_q.sql', 'r') as f:
        q = f.read()
    with open('q_s.sql', 'r') as f:
        s = f.read()

    cat = catalog.build_catalog(miedema, hostname='localhost', port=5432, user='postgres', password='password')
    det = detector.ErrorDetector(q, correct_solutions=[s], catalog=cat, debug=True)

    return det
