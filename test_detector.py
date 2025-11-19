from sql_error_categorizer.detectors import Detector
from sql_error_categorizer import load_catalog, build_catalog_from_postgres, SyntaxErrorDetector, SemanticErrorDetector, LogicalErrorDetector, ComplicationDetector

def make_catalog(file: str) -> None:
    '''Utility function to build a catalog from a source file'''

    with open(f'datasets/sql/{file}.sql') as f:
        content = f.read()

    cat = build_catalog_from_postgres(content, hostname='localhost', port=5432, user='postgres', password='password', schema=file, create_temp_schema=True)

    cat.save_json(f'datasets/catalogs/{file}.json')

def t(query_file: str = 'test_q.sql',
      solution_file: str = 'test_s1.sql',
      catalog_file: str = 'miedema',
      search_path: str | None = None) -> Detector:
    '''Test function, remove before production'''

    with open(query_file) as f:
        query = f.read()
    with open(solution_file) as f:
        solution = f.read()

    cat = load_catalog(f'datasets/catalogs/{catalog_file}.json')

    if search_path is None:
        search_path = cat.schema_names.pop() or 'public'

    det = Detector(query, solutions=[solution], catalog=cat, search_path=search_path, solution_search_path=search_path, debug=True)
    det.add_detector(SyntaxErrorDetector)
    det.add_detector(SemanticErrorDetector)
    det.add_detector(LogicalErrorDetector)
    det.add_detector(ComplicationDetector)

    return det
