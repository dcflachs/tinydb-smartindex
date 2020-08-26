from tinydb.table import Table, Document
from tinydb.storages import Storage
from tinydb.queries import Query
# from sortedcollection import SortedCollection
from sortedcontainers import SortedKeyList

from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Union,
    cast
)

class IndexableTable(Table):
    
    default_index_fields = []
    
    def __init__(
        self,
        storage: Storage,
        name: str,
        cache_size: int = Table.default_query_cache_capacity,
#       index_fields: List[str] = default_index_fields,
    ):
#       print(self.default_index_fields)
#       print(index_fields)
        super().__init__(storage, name, cache_size)
        
        #Create Indexes
        self._index_table = {}
        if not (self.default_index_fields is None):
            for field in self.default_index_fields:
                self._index_table[field] = SortedKeyList(self, lambda x: x[field])
    
    def insert(self, document: Mapping) -> int:
        doc_id = super().insert(document)
        
        #Update Query Cache
        for query in self._query_cache.lru:
            results = self._query_cache[query]
            if query(document) and results is not None:
                results.append(document)
                
        #Update Indexes
        for _ , v in self._index_table.items():
            v.add(document)
                
        return doc_id
    
    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        doc_ids = super().insert_multiple(documents)
        
        #Update Query Cache
        for query in self._query_cache.lru:
            results = self._query_cache[query]
            if results is not None:
                for doc in documents:
                    if query(doc):
                        results.append(doc)
                
        #Update Indexes
        for _ , v in self._index_table.items():
            for doc_id in doc_ids:
                v.add(self.get(doc_id=doc_id))
                
        return doc_ids
    
    def search(self, cond: Query) -> List[Document]:
        if not cond in self._query_cache:
            a = self.get_index_query(cond, list(self._index_table))        
            if a != None:
                index_func = a[0]
                docs = index_func(self._index_table.get(a[1]))
                self._query_cache[cond] = docs[:]
            else:
                docs = super().search(cond)                 
        else:
            docs = super().search(cond)

        return docs       
        
    def get(
        self,
        cond: Optional[Query] = None,
        doc_id: Optional[int] = None,
    ) -> Optional[Document]:
        
        #Check Query Cache
        if (doc_id is None) and (cond is not None):
            if cond in self._query_cache:
                for doc in self._query_cache.get(cond):
                    if cond(doc):
                        return doc        
            #Check indexes
#             a = self.get_index_query(cond, list(self._index_table))        
#             if a != None:
#                 index_func = a[0]
#                 docs = index_func(self._index_table.get(a[1]))
#                 self._query_cache[cond] = docs[:]
#                 print(len(docs))
#                 return docs[0]
        
        return super().get(cond, doc_id)
           
    def update(
        self,
        fields: Union[Mapping, Callable[[Mapping], None]],
        cond: Optional[Query] = None,
        doc_ids: Optional[Iterable[int]] = None,
    ) -> List[int]:
        
        if callable(fields):
            def perform_update(doc):
                # Update documents by calling the update function provided by
                # the user
                fields(doc)
        else:
            def perform_update(doc):
                # Update documents by setting all fields from the provided data
                doc.update(fields)
                
        def perform_update_override(doc):
            old_value = doc.copy()
            
            #Remove from indexes
            for _ , v in self._index_table.items():
                v.remove(old_value)
            
            # Update element
            perform_update(doc)
            new_value = doc
                
            #Update Query Cache
            for query in self._query_cache.lru:   
                results = self._query_cache[query]

                if query(old_value):
                    # Remove old value from cache
                    results.remove(old_value)

                if query(new_value):
                    # Add new value to cache
                    results.append(new_value)
                    
            #Update indexes
            for _ , v in self._index_table.items():
                v.add(new_value)
        
        return super().update(perform_update_override, cond, doc_ids)
    
    def remove(
        self,
        cond: Optional[Query] = None,
        doc_ids: Optional[Iterable[int]] = None,
    ) -> List[int]:
        
        if cond is None and doc_ids is None:
            raise RuntimeError('Use truncate() to remove all documents')
        
        #Remove From Query Cache
        docs_by_id = []
        if not doc_ids is None:
            docs_by_id = [self.get(doc_id=x) for x in doc_ids]
            
        for query in self._query_cache.lru:
            if (not cond is None) and (query == cond):
                del self._query_cache[query]
            else:
                for doc in self._query_cache[query]:
                    if ( ((not cond is None) and (cond(doc))) or
                         ((not doc_ids is None) and doc in docs_by_id) ):
                        self._query_cache[query].remove(doc)
                        
        #Remove from index
        for _ , v in self._index_table.items():
            for document in docs_by_id:
                v.remove(document)
            if (not cond is None): 
                for document in v.copy():
                    if cond(document):
                        v.remove(document)
        
        return super().remove(cond, doc_ids)
    
    def truncate(self) -> None:
        super().truncate()
        self.clear_cache()
        for _ , v in self._index_table.items():
            v.clear()
    
    def _update_table(self, updater: Callable[[Dict[int, Mapping]], None]):
        """
        Perform an table update operation.
        The storage interface used by TinyDB only allows to read/write the
        complete database data, but not modifying only portions of it. Thus
        to only update portions of the table data, we first perform a read
        operation, perform the update on the table data and then write
        the updated data back to the storage.
        As a further optimization, we don't convert the documents into the
        document class, as the table data will *not* be returned to the user.
        """

        tables = self._storage.read()

        if tables is None:
            # The database is empty
            tables = {}

        try:
            raw_table = tables[self.name]
        except KeyError:
            # The table does not exist yet, so it is empty
            raw_table = {}

        # Convert the document IDs to the document ID class.
        # This is required as the rest of TinyDB expects the document IDs
        # to be an instance of ``self.document_id_class`` but the storage
        # might convert dict keys to strings.
        table = {
            self.document_id_class(doc_id): doc
            for doc_id, doc in raw_table.items()
        }

        # Perform the table update operation
        updater(table)

        # Convert the document IDs back to strings.
        # This is required as some storages (most notably the JSON file format)
        # don't require IDs other than strings.
        tables[self.name] = {
            str(doc_id): doc
            for doc_id, doc in table.items()
        }

        # Write the newly updated data back to the storage
        self._storage.write(tables)
        
        # Clear the query cache, as the table contents have changed
#         self.clear_cache()

    def get_index_query(self, cond: Query, index_keys: list):
        path = cond._hash

        def process_tuple(path: tuple, index_keys: list):
            op = path[0]
            key = path[1]

            if op == 'not':
                return process_tuple(key, index_keys)

            if not op in ['==', '<', '>', '<=', '>=', '!=']:
                return None

            key = key[0]
            if not key in index_keys:
                return None

            val = path[2]
            print(op)
            print(key)
            print(val)
            if op == '==':
                def get_items(index):
                    print(index)
                    return [i for i in index.irange_key(val, val)]
            elif op == '<':
                def get_items(index):
                    return [i for i in index.irange_key(None, val, False)]
            elif op == '!=':
                def get_items(index):
                    return [i for i in index.irange_key(None, val, False)] + [i for i in index.irange_key(val, None, False)]
            elif op == '>':
                def get_items(index):
                    return [i for i in index.irange_key(val, None, False)]
            elif op == '<=':
                def get_items(index):
                    return [i for i in index.irange_key(None, val, True)]
            elif op == '>=':
                def get_items(index):
                    return [i for i in index.irange_key(val, None, True)]
            else:
                return None

            return (get_items, key)

        return process_tuple(path, index_keys)
        