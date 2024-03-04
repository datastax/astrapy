# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from ..conftest import ASTRA_DB_SECONDARY_KEYSPACE, TEST_COLLECTION_NAME, TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME
from astrapy import Collection, Database

class TestDDLSync:
    @pytest.mark.describe("test of collection creation, get, and then drop, sync")
    def test_collection_lifecycle_sync(
        self,
        sync_database: Database,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_local_coll"
        col1 = sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=123,
            metric="euclidean",
            indexing={"deny": ["a", "b", "c"]},
        )
        col2 = sync_database.get_collection(TEST_LOCAL_COLLECTION_NAME)
        assert col1 == col2
        sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)

    @pytest.mark.describe("should create and destroy a vector collection using collection drop ")
    def test_create_destroy_collection(self, sync_database: Database) -> None:
        col = sync_database.create_collection(
            name="TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME", dimension=2
        )
        assert isinstance(col, Collection)
        del_res = col.drop()
        assert del_res["status"]["ok"] == 1

    @pytest.mark.describe("should get information about the database")
    def test_db_info(self, sync_database: Database,) -> None:
            name = sync_database.name
            region = sync_database.region
            id = sync_database.dbid
            client_options = sync_database.client_options
            assert name is not None
            assert region is not None
            assert id is not None
            assert client_options is not None


    @pytest.mark.describe("test of Database list_collections, sync")
    def test_database_list_collections_sync(
        self,
        sync_database: Database,
        sync_collection: Collection,
    ) -> None:
        assert TEST_COLLECTION_NAME in sync_database.list_collection_names()


    @pytest.mark.describe("test of Database list_collections unsupported filter, sync")
    def test_database_list_collections_filter_sync(
        self,
        sync_database: Database,
        sync_collection: Collection,
    ) -> None:
        with pytest.raises(TypeError):
            sync_database.list_collection_names(filter={"k": "v"})

    @pytest.mark.skipif(
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of Database list_collections on cross-namespaces, sync")
    def test_database_list_collections_cross_namespace_sync(
        self,
        sync_database: Database,
        sync_collection: Collection,
    ) -> None:
        assert TEST_COLLECTION_NAME not in sync_database.list_collection_names(
            namespace=ASTRA_DB_SECONDARY_KEYSPACE
        )
