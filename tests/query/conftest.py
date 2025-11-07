import pytest
from sql_error_categorizer import load_catalog
from sql_error_categorizer.query import Query

DATASET = 'miedema'

@pytest.fixture
def catalog():
    return load_catalog(f'datasets/catalogs/cat_{DATASET}.json')

@pytest.fixture
def make_query(catalog):
    def _make_query(sql: str):
        return Query(sql, catalog=catalog, search_path=DATASET)
    return _make_query
