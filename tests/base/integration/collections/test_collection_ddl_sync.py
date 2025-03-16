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

from __future__ import annotations

import os
import time

import pytest

from astrapy import DataAPIClient, Database
from astrapy.constants import DefaultIdType, VectorMetric
from astrapy.ids import UUID, ObjectId
from astrapy.info import (
    AstraDBDatabaseInfo,
    CollectionDefaultIDOptions,
    CollectionDefinition,
    CollectionDescriptor,
    CollectionLexicalOptions,
    CollectionRerankingOptions,
    CollectionVectorOptions,
    RerankingServiceOptions,
)

from ..conftest import (
    IS_ASTRA_DB,
    SECONDARY_KEYSPACE,
    TEST_COLLECTION_NAME,
    DataAPICredentials,
    DataAPICredentialsInfo,
    DefaultCollection,
)


class TestCollectionDDLSync:
    @pytest.mark.describe("test of collection creation, get, and then drop, sync")
    def test_collection_lifecycle_sync(
        self,
        sync_database: Database,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_local_coll"
        TEST_LOCAL_COLLECTION_NAME_B = "test_local_coll_b"
        col1 = sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            definition=CollectionDefinition(
                vector=CollectionVectorOptions(
                    dimension=123,
                    metric=VectorMetric.EUCLIDEAN,
                ),
                indexing={"deny": ["a", "b", "c"]},
            ),
        )
        # test other creation methods (no-op since just created)
        col1_dict = sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            definition={
                "vector": {
                    "dimension": 123,
                    "metric": VectorMetric.EUCLIDEAN,
                },
                "indexing": {"deny": ["a", "b", "c"]},
            },
        )
        assert col1_dict == col1
        fluent_definition1 = (
            CollectionDefinition.builder()
            .set_vector_dimension(123)
            .set_vector_metric(VectorMetric.EUCLIDEAN)
            .set_indexing("deny", ["a", "b", "c"])
            .build()
        )
        col1_fluent = sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            definition=fluent_definition1,
        )
        assert col1_fluent == col1

        sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME_B,
            definition=CollectionDefinition(
                indexing={"allow": ["z"]},
            ),
        )
        lc_response = list(sync_database.list_collections())
        #
        # TODO: remove this ambiguity once lexical is a full citizen
        _farr_part = {
            "rerank": {
                "enabled": True,
                "service": {
                    "provider": "nvidia",
                    "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                },
            },
            "lexical": {
                "enabled": True,
                "analyzer": "standard",
            },
        }
        expected_coll_descriptor = CollectionDescriptor._from_dict(
            {
                "name": TEST_LOCAL_COLLECTION_NAME,
                "options": {
                    "vector": {
                        "dimension": 123,
                        "metric": "euclidean",
                        "sourceModel": "other",
                    },
                    "indexing": {"deny": ["a", "b", "c"]},
                    **(
                        _farr_part if "ASTRAPY_TEST_FINDANDRERANK" in os.environ else {}
                    ),
                },
            },
        )
        expected_coll_descriptor_b = CollectionDescriptor._from_dict(
            {
                "name": TEST_LOCAL_COLLECTION_NAME_B,
                "options": {
                    "indexing": {"allow": ["z"]},
                    **(
                        _farr_part if "ASTRAPY_TEST_FINDANDRERANK" in os.environ else {}
                    ),
                },
            },
        )
        assert expected_coll_descriptor in lc_response
        assert expected_coll_descriptor_b in lc_response
        #
        col2 = sync_database.get_collection(TEST_LOCAL_COLLECTION_NAME)
        assert col1 == col2
        sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME_B)

    @pytest.mark.describe("test of default_id_type in creating collections, sync")
    def test_collection_default_id_type_sync(
        self,
        sync_database: Database,
    ) -> None:
        ID_TEST_COLLECTION_NAME_ROOT = "id_type_test_"

        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUID,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.UUID,
                ),
            ),
        )
        col_options = col.options()
        assert col_options is not None
        assert col_options.default_id is not None
        assert col_options.default_id.default_id_type == DefaultIdType.UUID
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        doc = col.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], UUID)
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV6,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.UUIDV6,
                ),
            ),
        )
        col_options = col.options()
        assert col_options is not None
        assert col_options.default_id is not None
        assert col_options.default_id.default_id_type == DefaultIdType.UUIDV6
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 6
        doc = col.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 6
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV7,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.UUIDV7,
                ),
            ),
        )
        col_options = col.options()
        assert col_options is not None
        assert col_options.default_id is not None
        assert col_options.default_id.default_id_type == DefaultIdType.UUIDV7
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 7
        doc = col.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 7
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.DEFAULT,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.DEFAULT,
                ),
            ),
        )
        col_options = col.options()
        assert col_options is not None
        assert col_options.default_id is not None
        assert col_options.default_id.default_id_type == DefaultIdType.DEFAULT
        col.drop()

        time.sleep(2)
        col = sync_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.OBJECTID,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.OBJECTID,
                ),
            ),
        )
        col_options = col.options()
        assert col_options is not None
        assert col_options.default_id is not None
        assert col_options.default_id.default_id_type == DefaultIdType.OBJECTID
        i1res = col.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, ObjectId)
        doc = col.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], ObjectId)
        col.drop()

    @pytest.mark.describe("test of collection drop, sync")
    def test_collection_drop_sync(self, sync_database: Database) -> None:
        col = sync_database.create_collection(
            name="sync_collection_to_drop",
            definition=CollectionDefinition(
                vector=CollectionVectorOptions(
                    dimension=2,
                ),
            ),
        )
        col.drop()
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
        assert sync_database.keyspace == data_api_credentials_kwargs["keyspace"]
        assert isinstance(sync_database.info(), AstraDBDatabaseInfo)
        assert isinstance(sync_database.info().raw, dict)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of collection metainformation, sync")
    def test_get_collection_info_sync(
        self,
        sync_collection: DefaultCollection,
    ) -> None:
        info = sync_collection.info()
        assert info.keyspace == sync_collection.keyspace
        assert info.keyspace == sync_collection.database.keyspace

    @pytest.mark.describe("test of Database list_collections, sync")
    def test_database_list_collections_sync(
        self,
        sync_database: Database,
        sync_collection: DefaultCollection,
    ) -> None:
        assert TEST_COLLECTION_NAME in sync_database.list_collection_names()

    @pytest.mark.describe("test of Collection options, sync")
    def test_collection_options_sync(
        self,
        sync_collection: DefaultCollection,
    ) -> None:
        options = sync_collection.options()
        assert options.vector is not None
        assert options.vector.dimension == 2

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of Database list_collections on cross-keyspaces, sync")
    def test_database_list_collections_cross_keyspace_sync(
        self,
        sync_database: Database,
        sync_collection: DefaultCollection,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        assert TEST_COLLECTION_NAME not in sync_database.list_collection_names(
            keyspace=data_api_credentials_info["secondary_keyspace"]
        )

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of Database use_keyspace, sync")
    def test_database_use_keyspace_sync(
        self,
        sync_database: Database,
        sync_collection: DefaultCollection,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        # make a copy to avoid mutating the fixture
        t_database = sync_database._copy()
        assert t_database == sync_database
        assert t_database.keyspace == data_api_credentials_kwargs["keyspace"]
        assert TEST_COLLECTION_NAME in t_database.list_collection_names()

        t_database.use_keyspace(data_api_credentials_info["secondary_keyspace"])  # type: ignore[arg-type]
        assert t_database != sync_database
        assert t_database.keyspace == data_api_credentials_info["secondary_keyspace"]
        assert TEST_COLLECTION_NAME not in t_database.list_collection_names()

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of cross-keyspace collection lifecycle, sync")
    def test_collection_keyspace_sync(
        self,
        sync_database: Database,
        client: DataAPIClient,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME1 = "test_crossks_coll1"
        TEST_LOCAL_COLLECTION_NAME2 = "test_crossks_coll2"
        database_on_secondary = client.get_database(
            data_api_credentials_kwargs["api_endpoint"],
            token=data_api_credentials_kwargs["token"],
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME1,
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        col2_on_secondary = sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME2,
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME1 in database_on_secondary.list_collection_names()
        )
        database_on_secondary.drop_collection(TEST_LOCAL_COLLECTION_NAME1)
        sync_database.drop_collection(
            col2_on_secondary.name,
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
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
        sync_collection: DefaultCollection,
    ) -> None:
        cmd1 = sync_database.command(
            {"countDocuments": {}}, collection_or_table_name=sync_collection.name
        )
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)
        cmd2 = sync_database._copy(keyspace="...").command(
            {"countDocuments": {}},
            keyspace=sync_collection.keyspace,
            collection_or_table_name=sync_collection.name,
        )
        assert cmd2 == cmd1
        assert "count" in (cmd2.get("status") or {})

    @pytest.mark.describe("test of database command, sync")
    def test_database_command_sync(
        self,
        sync_database: Database,
    ) -> None:
        cmd1 = sync_database.command({"findCollections": {}})
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["collections"], list)
        cmd2 = sync_database._copy(keyspace="...").command(
            {"findCollections": {}}, keyspace=sync_database.keyspace
        )
        assert cmd2 == cmd1
        assert "collections" in (cmd2.get("status") or {})

        cmd_feps = sync_database.command(
            {"findEmbeddingProviders": {}},
            keyspace=None,
        )
        assert "embeddingProviders" in (cmd_feps.get("status") or {})

    @pytest.mark.describe("test of database command, sync")
    def test_collection_command_sync(
        self,
        sync_collection: DefaultCollection,
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
        database = client.get_database(
            api_endpoint,
            token=token,
            keyspace=data_api_credentials_kwargs["keyspace"],
        )
        assert isinstance(database.list_collection_names(), list)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe(
        "test database-from-admin default keyspace per environment, sync"
    )
    def test_database_from_admin_default_keyspace_per_environment_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        client = DataAPIClient(environment=data_api_credentials_info["environment"])
        admin = client.get_admin(token=data_api_credentials_kwargs["token"])
        db_m = admin.get_database(
            data_api_credentials_kwargs["api_endpoint"],
            keyspace="M",
        )
        assert db_m.keyspace == "M"
        db_n = admin.get_database(data_api_credentials_kwargs["api_endpoint"])
        assert isinstance(db_n.keyspace, str)  # i.e. resolution took place

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe(
        "test database-from-astradbadmin default keyspace per environment, sync"
    )
    def test_database_from_astradbadmin_default_keyspace_per_environment_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        client = DataAPIClient(environment=data_api_credentials_info["environment"])
        admin = client.get_admin(token=data_api_credentials_kwargs["token"])
        db_admin = admin.get_database_admin(data_api_credentials_kwargs["api_endpoint"])
        db_m = db_admin.get_database(keyspace="M")
        assert db_m.keyspace == "M"
        db_n = db_admin.get_database()
        assert isinstance(db_n.keyspace, str)  # i.e. resolution took place

    @pytest.mark.skipif(
        "ASTRAPY_TEST_FINDANDRERANK" not in os.environ,
        reason="No testing enabled on findAndRerank support",
    )
    @pytest.mark.describe("test of collection find-and-rerank lifecycle, sync")
    def test_collection_farr_lifecycle_sync(
        self,
        sync_database: Database,
    ) -> None:
        try:
            TEST_FARR_COLLECTION_NAME = "test_farr_coll"
            col1 = sync_database.create_collection(
                TEST_FARR_COLLECTION_NAME,
                definition=CollectionDefinition(
                    vector=CollectionVectorOptions(
                        dimension=10,
                        metric=VectorMetric.EUCLIDEAN,
                    ),
                    rerank=CollectionRerankingOptions(
                        service=RerankingServiceOptions(
                            provider="nvidia",
                            model_name="nvidia/llama-3.2-nv-rerankqa-1b-v2",
                        ),
                    ),
                    lexical=CollectionLexicalOptions(
                        analyzer="STANDARD",
                    ),
                ),
            )
            # test other creation methods (no-op since just created)
            col1_dict = sync_database.create_collection(
                TEST_FARR_COLLECTION_NAME,
                definition={
                    "vector": {
                        "dimension": 10,
                        "metric": VectorMetric.EUCLIDEAN,
                    },
                    "rerank": {
                        "enabled": True,
                        "service": {
                            "provider": "nvidia",
                            "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                        },
                    },
                    "lexical": {
                        "enabled": True,
                        "analyzer": "STANDARD",
                    },
                },
            )
            assert col1_dict == col1
            fluent_definition1 = (
                CollectionDefinition.builder()
                .set_vector_dimension(10)
                .set_vector_metric(VectorMetric.EUCLIDEAN)
                .set_rerank("nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2")
                .set_lexical("STANDARD")
                .build()
            )
            col1_fluent = sync_database.create_collection(
                TEST_FARR_COLLECTION_NAME,
                definition=fluent_definition1,
            )
            assert col1_fluent == col1

            lc_response = list(sync_database.list_collections())

            expected_coll_descriptor = CollectionDescriptor._from_dict(
                {
                    "name": TEST_FARR_COLLECTION_NAME,
                    "options": {
                        "vector": {
                            "dimension": 10,
                            "metric": "euclidean",
                            "sourceModel": "other",
                        },
                        "rerank": {
                            "enabled": True,
                            "service": {
                                "provider": "nvidia",
                                "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                            },
                        },
                        "lexical": {
                            "enabled": True,
                            "analyzer": "STANDARD",
                        },
                    },
                },
            )
            assert expected_coll_descriptor in lc_response

            col2 = sync_database.get_collection(TEST_FARR_COLLECTION_NAME)
            assert col1 == col2
        finally:
            sync_database.drop_collection(TEST_FARR_COLLECTION_NAME)
