from sql_error_categorizer.detectors import Detector
from sql_error_categorizer import load_catalog, build_catalog, SyntaxErrorDetector, SemanticErrorDetector, LogicalErrorDetector, ComplicationDetector

def make_catalog(src_file: str, dest_file: str) -> None:
    '''Utility function to build a catalog from a source file'''

    with open(src_file) as f:
        content = f.read()

    cat = build_catalog(content, hostname='localhost', port=5432, user='postgres', password='password')

    cat.save_json(dest_file)

def t(query_file: str = 'q_q.sql',
      solution_file: str = 'q_s.sql',
      catalog_file: str = 'tests/datasets/cat_miedema.json',
      search_path: str | None = None) -> Detector:
    '''Test function, remove before production'''

    with open(query_file) as f:
        query = f.read()
    with open(solution_file) as f:
        solution = f.read()

    cat = load_catalog(catalog_file)

    if search_path is None:
        search_path = cat.schema_names.pop() or 'public'

    det = Detector(query, solutions=[solution], catalog=cat, search_path=search_path, solution_search_path=search_path, debug=True)
    det.add_detector(SyntaxErrorDetector)
    det.add_detector(SemanticErrorDetector)
    det.add_detector(LogicalErrorDetector)
    det.add_detector(ComplicationDetector)

    return det

det = t()
