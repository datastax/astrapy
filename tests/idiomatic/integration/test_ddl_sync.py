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

import time

from ..conftest import (
    DataAPICredentials,
    DataAPICredentialsInfo,
    SECONDARY_NAMESPACE,
    TEST_COLLECTION_NAME,
    IS_ASTRA_DB,
)
from astrapy.info import (
    CollectionDescriptor,
    DatabaseInfo,
)
from astrapy.constants import DefaultIdType, VectorMetric
from astrapy.ids import ObjectId, UUID
from astrapy import Collection, DataAPIClient, Database


class TestDDLSync:
    @pytest.mark.describe("test of collection creation, get, and then drop, sync")
    def test_collection_lifecycle_sync(
        self,
        sync_database: Database,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_local_coll"
        TEST_LOCAL_COLLECTION_NAME_B = "test_local_coll_b"
        col1 = sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=123,
            metric=VectorMetric.EUCLIDEAN,
            indexing={"deny": ["a", "b", "c"]},
        )
        sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME_B,
            indexing={"allow": ["z"]},
        )
        lc_response = list(sync_database.list_collections())
        #
        expected_coll_descriptor = CollectionDescriptor.from_dict(
            {
                "name": TEST_LOCAL_COLLECTION_NAME,
                "options": {
                    "vector": {
                        "dimension": 123,
                        "metric": "euclidean",
                    },
                    "indexing": {"deny": ["a", "b", "c"]},
                },
            },
        )
        expected_coll_descriptor_b = CollectionDescriptor.from_dict(
            {
                "name": TEST_LOCAL_COLLECTION_NAME_B,
                "options": {
                    "indexing": {"allow": ["z"]},
                },
            },
        )
        assert expected_coll_descriptor in lc_response
        assert expected_coll_descriptor_b in lc_response
        #
        col2 = sync_database.get_collection(TEST_LOCAL_COLLECTION_NAME)
        assert col1 == col2
        dc_response = sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        assert dc_response == {"ok": 1}
        dc_response2 = sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        assert dc_response2 == {"ok": 1}
        sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME_B)

    @pytest.mark.describe("test of default_id_type in creating collections, sync")
    def test_collection_default_id_type_sync(
        self,
        sync_database: Database,
    ) -> None:
        ID_TEST_COLLECTION_NAME_ROOT = "id_type_test_"

        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUID,
            default_id_type=DefaultIdType.UUID,
        )
        assert col.options().default_id.default_id_type == DefaultIdType.UUID
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        doc = col.find_one({})
        assert isinstance(doc["_id"], UUID)
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV6,
            default_id_type=DefaultIdType.UUIDV6,
        )
        assert col.options().default_id.default_id_type == DefaultIdType.UUIDV6
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 6
        doc = col.find_one({})
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 6
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV7,
            default_id_type=DefaultIdType.UUIDV7,
        )
        assert col.options().default_id.default_id_type == DefaultIdType.UUIDV7
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 7
        doc = col.find_one({})
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 7
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.DEFAULT,
            default_id_type=DefaultIdType.DEFAULT,
        )
        assert col.options().default_id.default_id_type == DefaultIdType.DEFAULT
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.OBJECTID,
            default_id_type=DefaultIdType.OBJECTID,
        )
        assert col.options().default_id.default_id_type == DefaultIdType.OBJECTID
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, ObjectId)
        doc = col.find_one({})
        assert isinstance(doc["_id"], ObjectId)
        col.drop()

    @pytest.mark.describe("test of collection drop, sync")
    def test_collection_drop_sync(self, sync_database: Database) -> None:
        col = sync_database.create_collection(
            name="sync_collection_to_drop", dimension=2
        )
        del_res = col.drop()
        assert del_res["ok"] == 1
        assert "sync_collection_to_drop" not in sync_database.list_collection_names()

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of database metainformation, sync")
    def test_get_database_info_sync(
        self,
        sync_database: Database,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        assert isinstance(sync_database.id, str)
        assert isinstance(sync_database.name(), str)
        assert sync_database.namespace == data_api_credentials_kwargs["namespace"]
        assert isinstance(sync_database.info(), DatabaseInfo)
        assert isinstance(sync_database.info().raw_info, dict)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of collection metainformation, sync")
    def test_get_collection_info_sync(
        self,
        sync_collection: Collection,
    ) -> None:
        info = sync_collection.info()
        assert info.namespace == sync_collection.namespace
        assert info.namespace == sync_collection._astra_db_collection.astra_db.namespace

    @pytest.mark.describe("test of Database list_collections, sync")
    def test_database_list_collections_sync(
        self,
        sync_database: Database,
        sync_collection: Collection,
    ) -> None:
        assert TEST_COLLECTION_NAME in sync_database.list_collection_names()

    @pytest.mark.describe("test of Collection options, sync")
    def test_collection_options_sync(
        self,
        sync_collection: Collection,
    ) -> None:
        options = sync_collection.options()
        assert options.vector is not None
        assert options.vector.dimension == 2

    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary namespace provided"
    )
    @pytest.mark.describe("test of Database list_collections on cross-namespaces, sync")
    def test_database_list_collections_cross_namespace_sync(
        self,
        sync_database: Database,
        sync_collection: Collection,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        assert TEST_COLLECTION_NAME not in sync_database.list_collection_names(
            namespace=data_api_credentials_info["secondary_namespace"]
        )

    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary namespace provided"
    )
    @pytest.mark.describe("test of cross-namespace collection lifecycle, sync")
    def test_collection_namespace_sync(
        self,
        sync_database: Database,
        client: DataAPIClient,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME1 = "test_crossns_coll1"
        TEST_LOCAL_COLLECTION_NAME2 = "test_crossns_coll2"
        database_on_secondary = client.get_database(
            data_api_credentials_kwargs["api_endpoint"],
            token=data_api_credentials_kwargs["token"],
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME1,
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        col2_on_secondary = sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME2,
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME1 in database_on_secondary.list_collection_names()
        )
        database_on_secondary.drop_collection(TEST_LOCAL_COLLECTION_NAME1)
        sync_database.drop_collection(col2_on_secondary)
        assert (
            TEST_LOCAL_COLLECTION_NAME1
            not in database_on_secondary.list_collection_names()
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME2
            not in database_on_secondary.list_collection_names()
        )

    @pytest.mark.describe("test of database command targeting collection, sync")
    def test_database_command_on_collection_sync(
        self,
        sync_database: Database,
        sync_collection: Collection,
    ) -> None:
        cmd1 = sync_database.command(
            {"countDocuments": {}}, collection_name=sync_collection.name
        )
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)
        cmd2 = sync_database._copy(namespace="...").command(
            {"countDocuments": {}},
            namespace=sync_collection.namespace,
            collection_name=sync_collection.name,
        )
        assert cmd2 == cmd1

    @pytest.mark.describe("test of database command, sync")
    def test_database_command_sync(
        self,
        sync_database: Database,
    ) -> None:
        cmd1 = sync_database.command({"findCollections": {}})
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["collections"], list)
        cmd2 = sync_database._copy(namespace="...").command(
            {"findCollections": {}}, namespace=sync_database.namespace
        )
        assert cmd2 == cmd1

    @pytest.mark.describe("test of database command, sync")
    def test_collection_command_sync(
        self,
        sync_collection: Collection,
    ) -> None:
        cmd1 = sync_collection.command({"countDocuments": {}})
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)

    @pytest.mark.describe("test of tokenless client creation, sync")
    def test_tokenless_client_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        api_endpoint = data_api_credentials_kwargs["api_endpoint"]
        token = data_api_credentials_kwargs["token"]
        client = DataAPIClient(environment=data_api_credentials_info["environment"])
        database = client.get_database(api_endpoint, token=token)
        assert isinstance(database.list_collection_names(), list)
