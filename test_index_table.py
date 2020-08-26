from tinydb import TinyDB, where
from tinydb.database import Table
from tinydb.storages import MemoryStorage
# from tinydb.utils import catch_warning
import pytest

from index_table import IndexableTable

@pytest.fixture
def db_index():
    TinyDB.table_class = IndexableTable
    TinyDB.table_class.default_index_fields = ['int']

    db_ = TinyDB(storage=MemoryStorage)
    table = db_.table('_default')

    table.insert_multiple({'int': 1, 'yar': 5, 'char': c} for c in 'abc')
#     table.insert({'int': 1, 'char': 'a'})
#     table.insert({'int': 1, 'char': 'b'})
#     table.insert({'int': 1, 'char': 'c'})
    
    return table

@pytest.fixture
def db_no_index():
    TinyDB.table_class = IndexableTable
#     TinyDB.table_class.default_index_fields = ['int']

    db_ = TinyDB(storage=MemoryStorage)
    table = db_.table('_default')

    table.insert_multiple({'int': 1, 'char': c} for c in 'abc')
#     table.insert({'int': 1, 'char': 'a'})
#     table.insert({'int': 1, 'char': 'b'})
#     table.insert({'int': 1, 'char': 'c'})
    
    return table

@pytest.fixture
def db():
    db_ = TinyDB(storage=MemoryStorage)
    db_.drop_tables()

    db_.insert_multiple({'int': 1, 'char': c} for c in 'abc')
    return db_


def test_smart_query_cache(db_index):
    db = db_index

    query = where('int') == 1
    dummy = where('int') == 2

    assert len(db.search(query)) == 3
    assert len(db.search(dummy)) == 0
    
    assert len(db._query_cache[query]) == 3
    assert len(db._query_cache[dummy]) == 0
    
    db.truncate()
    
    assert not db.search(query)
    assert not db.search(dummy)
    assert len(db._query_cache[query]) == 0
    assert len(db._query_cache[dummy]) == 0

    # Test insert
    db.insert({'int': 1})

    assert len(db._query_cache) == 2
    assert len(db._query_cache[query]) == 1
    assert len(db._query_cache[dummy]) == 0

    # Test update
    db.update({'int': 2}, where('int') == 1)

    assert len(db._query_cache[query]) == 0
    assert len(db._query_cache[dummy]) == 1
    assert db.count(query) == 0

    # Test remove
    db.insert({'int': 1})
    db.remove(where('int') == 1)

    assert db.count(where('int') == 1) == 0
    
def test_indexes(db_index):
    db = db_index

    assert db.default_index_fields == ['int']
    assert len(db._index_table) == 1
    assert len(db._index_table['int']) == 3
    
    query = where('int') == 1
    dummy = where('int') == 2

    assert len(db.search(query)) == 3
    assert len(db.search(dummy)) == 0
    
    assert len(db._query_cache[query]) == 3
    assert len(db._query_cache[dummy]) == 0
    
    db.truncate()
    
    assert not db.search(query)
    assert not db.search(dummy)
    assert len(db._query_cache[query]) == 0
    assert len(db._query_cache[dummy]) == 0

    # Test insert
    db.insert({'int': 1})

    assert len(db._query_cache) == 2
    assert len(db._query_cache[query]) == 1
    assert len(db._query_cache[dummy]) == 0

    # Test update
    db.update({'int': 2}, where('int') == 1)

    assert len(db._query_cache[query]) == 0
    assert len(db._query_cache[dummy]) == 1
    assert db.count(query) == 0

    # Test remove
    db.insert({'int': 1})
    db.remove(where('int') == 1)

    assert db.count(where('int') == 1) == 0


def test_custom_table_class_via_class_attribute(db):
    TinyDB.table_class = IndexableTable

    table = db.table('table3')
    assert isinstance(table, IndexableTable)

    TinyDB.table_class = Table


# def test_custom_table_class_via_instance_attribute(db):
#     db.table_class = SmartCacheTable
#     table = db.table('table3')
#     assert isinstance(table, SmartCacheTable)


def test_truncate(db_index):
    db = db_index

    db.truncate()

    db.insert({'int': 5})
    db.truncate()

    assert len(db) == 0
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_all(db_index):
    db = db_index

    db.truncate()

    for i in range(10):
        db.insert({'int':i*10})

    assert len(db.all()) == 10
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_insert(db_index):
    db = db_index

    db.truncate()
    db.insert({'int': 1, 'char': 'a'})

    assert db.count(where('int') == 1) == 1

    db.truncate()

    db.insert({'int': 1, 'char': 'a'})
    db.insert({'int': 1, 'char': 'b'})
    db.insert({'int': 1, 'char': 'c'})

    assert db.count(where('int') == 1) == 3
    assert db.count(where('char') == 'a') == 1
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_insert_ids(db_index):
    db = db_index

    db.truncate()
    assert db.insert({'int': 1, 'char': 'a'}) == 1
    assert db.insert({'int': 1, 'char': 'a'}) == 2
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_insert_multiple(db_index):
    db = db_index

    db.truncate()
    assert not db.contains(where('int') == 1)

    # Insert multiple from list
    db.insert_multiple([{'int': 1, 'char': 'a'},
                        {'int': 1, 'char': 'b'},
                        {'int': 1, 'char': 'c'}])

    assert db.count(where('int') == 1) == 3
    assert db.count(where('char') == 'a') == 1

    # Insert multiple from generator function
    def generator():
        for j in range(10):
            yield {'int': j}

    db.truncate()

    db.insert_multiple(generator())

    for i in range(10):
        assert db.count(where('int') == i) == 1
    if hasattr(where('int'), 'exists'):
        assert db.count(where('int').exists()) == 10
    else:
        assert db.count(where('int')) == 10

    # Insert multiple from inline generator
    db.truncate()

    db.insert_multiple({'int': i} for i in range(10))

    for i in range(10):
        assert db.count(where('int') == i) == 1
        
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_insert_multiple_with_ids(db_index):
    db = db_index

    db.truncate()

    # Insert multiple from list
    assert db.insert_multiple([{'int': 1, 'char': 'a'},
                               {'int': 1, 'char': 'b'},
                               {'int': 1, 'char': 'c'}]) == [1, 2, 3]
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        assert all(i in list(db.all()) for i in list(index))


def test_remove(db_index):
    db = db_index

    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
    
    db.remove(where('char') == 'b')

    assert len(db) == 2
    assert db.count(where('int') == 1) == 2
    assert db.count(where('char') == 'b') == 0
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
#         print(db.all())
#         print(list(index))
#         assert all(i in list(db.all()) for i in list(index))


def test_remove_multiple(db_index):
    db = db_index

    db.remove(where('int') == 1)
#     db.remove(where('char') == 'a')
#     db.remove(where('char') == 'b')
#     db.remove(where('char') == 'c')
    
    assert len(db) == 0
    
    for _, index in db._index_table.items():
        print(index)
        assert len(db.all()) == len(index)
#         assert all(i in list(db.all()) for i in list(index))


def test_remove_ids(db_index):
    db = db_index
    print(db._index_table)
    db.remove(doc_ids=[1, 2])

    assert len(db) == 1

    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_update(db_index):
    db = db_index

    assert db.count(where('int') == 1) == 3

    db.update({'int': 2}, where('char') == 'a')

    assert db.count(where('int') == 2) == 1
    assert db.count(where('int') == 1) == 2
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_update_transform(db_index):
    db = db_index

    def increment(field):
        def transform(el):
            el[field] += 1
        return transform

    def delete(field):
        def transform(el):
            del el[field]
        return transform

    assert db.count(where('int') == 1) == 3

    db.update(increment('int'), where('char') == 'a')
    db.update(delete('char'), where('char') == 'a')

    assert db.count(where('int') == 2) == 1
    assert db.count(where('char') == 'a') == 0
    assert db.count(where('int') == 1) == 2
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_update_ids(db_index):
    db = db_index

    db.update({'int': 2}, doc_ids=[1, 2])

    assert db.count(where('int') == 2) == 2
    
    for _, index in db._index_table.items():
        assert len(index) == len(db.all())
        print(db.all())
        print(list(index))
        assert all(i in list(db.all()) for i in list(index))


def test_search(db_index):
    db = db_index

    assert not db._query_cache
    assert len(db.search(where('int') == 1)) == 3

    assert len(db._query_cache) == 1
    assert len(db.search(where('int') == 1)) == 3  # Query result from cache


def test_contians(db_index):
    db = db_index

    assert db.contains(where('int') == 1)
    assert not db.contains(where('int') == 0)


def test_contains_ids(db_index):
    db = db_index

#     assert db.contains(doc_id=[1, 2])
    assert not db.contains(doc_id=88)


def test_get(db_index):
    db = db_index

    item = db.get(where('char') == 'b')
    assert item['char'] == 'b'


def test_get_ids(db_index):
    db = db_index

    el = db.all()[0]
    assert db.get(doc_id=el.doc_id) == el
    assert db.get(doc_id=float('NaN')) is None


def test_count(db_index):
    db = db_index

    assert db.count(where('int') == 1) == 3
    assert db.count(where('char') == 'd') == 0


def test_contains(db_index):
    db = db_index

    assert db.contains(where('int') == 1)
    assert not db.contains(where('int') == 0)


def test_get_idempotent(db_index):
    db = db_index

    u = db.get(where('int') == 1)
    z = db.get(where('int') == 1)
    assert u == z