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

import asyncio

import pytest

from astrapy import AsyncDatabase, DataAPIClient
from astrapy.constants import DefaultIdType, VectorMetric
from astrapy.ids import UUID, ObjectId
from astrapy.info import (
    AstraDBDatabaseInfo,
    CollectionDefaultIDOptions,
    CollectionDefinition,
    CollectionDescriptor,
    CollectionLexicalOptions,
    CollectionRerankOptions,
    CollectionVectorOptions,
    RerankServiceOptions,
)

from ..conftest import (
    IS_ASTRA_DB,
    SECONDARY_KEYSPACE,
    TEST_COLLECTION_NAME,
    DataAPICredentials,
    DataAPICredentialsInfo,
    DefaultAsyncCollection,
)


class TestCollectionDDLAsync:
    @pytest.mark.describe("test of collection creation, get, and then drop, async")
    async def test_collection_lifecycle_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_local_coll"
        TEST_LOCAL_COLLECTION_NAME_B = "test_local_coll_b"
        col1 = await async_database.create_collection(
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
        col1_dict = await async_database.create_collection(
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
        col1_fluent = await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            definition=fluent_definition1,
        )
        assert col1_fluent == col1

        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME_B,
            definition=CollectionDefinition(
                indexing={"allow": ["z"]},
            ),
        )
        lc_response = await async_database.list_collections()
        #
        rerank_lexical_portion = {
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
                    **rerank_lexical_portion,
                },
            },
        )
        expected_coll_descriptor_b = CollectionDescriptor._from_dict(
            {
                "name": TEST_LOCAL_COLLECTION_NAME_B,
                "options": {
                    "indexing": {"allow": ["z"]},
                    **rerank_lexical_portion,
                },
            },
        )
        assert expected_coll_descriptor in lc_response
        assert expected_coll_descriptor_b in lc_response
        #
        col2 = async_database.get_collection(TEST_LOCAL_COLLECTION_NAME)
        assert col1 == col2
        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME_B)

    @pytest.mark.describe("test of default_id_type in creating collections, async")
    async def test_collection_default_id_type_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        ID_TEST_COLLECTION_NAME_ROOT = "id_type_test_"

        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUID,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.UUID,
                ),
            ),
        )
        acol_options = await acol.options()
        assert acol_options is not None
        assert acol_options.default_id is not None
        assert acol_options.default_id.default_id_type == DefaultIdType.UUID
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        doc = await acol.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], UUID)
        await acol.drop()

        await asyncio.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV6,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.UUIDV6,
                ),
            ),
        )
        acol_options = await acol.options()
        assert acol_options is not None
        assert acol_options.default_id is not None
        assert acol_options.default_id.default_id_type == DefaultIdType.UUIDV6
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 6
        doc = await acol.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 6
        await acol.drop()

        await asyncio.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV7,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.UUIDV7,
                ),
            ),
        )
        acol_options = await acol.options()
        assert acol_options is not None
        assert acol_options.default_id is not None
        assert acol_options.default_id.default_id_type == DefaultIdType.UUIDV7
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 7
        doc = await acol.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 7
        await acol.drop()

        await asyncio.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.DEFAULT,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.DEFAULT,
                ),
            ),
        )
        acol_options = await acol.options()
        assert acol_options is not None
        assert acol_options.default_id is not None
        assert acol_options.default_id.default_id_type == DefaultIdType.DEFAULT
        await acol.drop()

        await asyncio.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.OBJECTID,
            definition=CollectionDefinition(
                default_id=CollectionDefaultIDOptions(
                    default_id_type=DefaultIdType.OBJECTID,
                ),
            ),
        )
        acol_options = await acol.options()
        assert acol_options is not None
        assert acol_options.default_id is not None
        assert acol_options.default_id.default_id_type == DefaultIdType.OBJECTID
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, ObjectId)
        doc = await acol.find_one({})
        assert doc is not None
        assert isinstance(doc["_id"], ObjectId)
        await acol.drop()

    @pytest.mark.describe("test of collection drop, async")
    async def test_collection_drop_async(self, async_database: AsyncDatabase) -> None:
        col = await async_database.create_collection(
            name="async_collection_to_drop",
            definition=CollectionDefinition(
                vector=CollectionVectorOptions(
                    dimension=2,
                ),
            ),
        )
        await col.drop()
        assert "async_collection_to_drop" not in (
            await async_database.list_collection_names()
        )

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of database metainformation, async")
    async def test_get_database_info_async(
        self,
        async_database: AsyncDatabase,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        assert isinstance(async_database.id, str)
        assert isinstance(await async_database.name(), str)
        assert async_database.keyspace == data_api_credentials_kwargs["keyspace"]
        assert isinstance(await async_database.info(), AstraDBDatabaseInfo)
        assert isinstance((await async_database.info()).raw, dict)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of collection metainformation, async")
    async def test_get_collection_info_async(
        self,
        async_collection: DefaultAsyncCollection,
    ) -> None:
        info = await async_collection.info()
        assert info.keyspace == async_collection.keyspace
        assert info.keyspace == async_collection.database.keyspace

    @pytest.mark.describe("test of Database list_collections, async")
    async def test_database_list_collections_async(
        self,
        async_database: AsyncDatabase,
        async_collection: DefaultAsyncCollection,
    ) -> None:
        assert TEST_COLLECTION_NAME in await async_database.list_collection_names()

    @pytest.mark.describe("test of Collection options, async")
    async def test_collection_options_async(
        self,
        async_collection: DefaultAsyncCollection,
    ) -> None:
        options = await async_collection.options()
        assert options.vector is not None
        assert options.vector.dimension == 2

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of Database list_collections on cross-keyspaces, async")
    async def test_database_list_collections_cross_keyspace_async(
        self,
        async_database: AsyncDatabase,
        async_collection: DefaultAsyncCollection,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        assert TEST_COLLECTION_NAME not in await async_database.list_collection_names(
            keyspace=data_api_credentials_info["secondary_keyspace"]
        )

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of Database use_keyspace, async")
    async def test_database_use_keyspace_async(
        self,
        async_database: AsyncDatabase,
        async_collection: DefaultAsyncCollection,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        # make a copy to avoid mutating the fixture
        at_database = async_database._copy()
        assert at_database == async_database
        assert at_database.keyspace == data_api_credentials_kwargs["keyspace"]
        assert TEST_COLLECTION_NAME in await at_database.list_collection_names()

        at_database.use_keyspace(data_api_credentials_info["secondary_keyspace"])  # type: ignore[arg-type]
        assert at_database != async_database
        assert at_database.keyspace == data_api_credentials_info["secondary_keyspace"]
        assert TEST_COLLECTION_NAME not in await at_database.list_collection_names()

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of cross-keyspace collection lifecycle, async")
    async def test_collection_keyspace_async(
        self,
        async_database: AsyncDatabase,
        client: DataAPIClient,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME1 = "test_crossks_coll1"
        TEST_LOCAL_COLLECTION_NAME2 = "test_crossks_coll2"
        database_on_secondary = client.get_async_database(
            data_api_credentials_kwargs["api_endpoint"],
            token=data_api_credentials_kwargs["token"],
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME1,
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        col2_on_secondary = await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME2,
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME1
            in await database_on_secondary.list_collection_names()
        )
        await database_on_secondary.drop_collection(TEST_LOCAL_COLLECTION_NAME1)
        await async_database.drop_collection(
            col2_on_secondary.name,
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME1
            not in await database_on_secondary.list_collection_names()
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME2
            not in await database_on_secondary.list_collection_names()
        )

    @pytest.mark.describe("test of database command targeting collection, async")
    async def test_database_command_on_collection_async(
        self,
        async_database: AsyncDatabase,
        async_collection: DefaultAsyncCollection,
    ) -> None:
        cmd1 = await async_database.command(
            {"countDocuments": {}}, collection_or_table_name=async_collection.name
        )
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)
        cmd2 = await async_database._copy(keyspace="...").command(
            {"countDocuments": {}},
            keyspace=async_collection.keyspace,
            collection_or_table_name=async_collection.name,
        )
        assert cmd2 == cmd1
        assert "count" in (cmd2.get("status") or {})

    @pytest.mark.describe("test of database command, async")
    async def test_database_command_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        cmd1 = await async_database.command({"findCollections": {}})
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["collections"], list)
        cmd2 = await async_database._copy(keyspace="...").command(
            {"findCollections": {}}, keyspace=async_database.keyspace
        )
        assert cmd2 == cmd1
        assert "collections" in (cmd2.get("status") or {})

        cmd_feps = await async_database.command(
            {"findEmbeddingProviders": {}},
            keyspace=None,
        )
        assert "embeddingProviders" in (cmd_feps.get("status") or {})

    @pytest.mark.describe("test of database command, async")
    async def test_collection_command_async(
        self,
        async_collection: DefaultAsyncCollection,
    ) -> None:
        cmd1 = await async_collection.command({"countDocuments": {}})
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)

    @pytest.mark.describe("test of tokenless client creation, async")
    async def test_tokenless_client_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        api_endpoint = data_api_credentials_kwargs["api_endpoint"]
        token = data_api_credentials_kwargs["token"]
        client = DataAPIClient(environment=data_api_credentials_info["environment"])
        a_database = client.get_async_database(
            api_endpoint,
            token=token,
            keyspace=data_api_credentials_kwargs["keyspace"],
        )
        coll_names = await a_database.list_collection_names()
        assert isinstance(coll_names, list)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe(
        "test database-from-admin default keyspace per environment, async"
    )
    def test_async_database_from_admin_default_keyspace_per_environment(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        client = DataAPIClient(environment=data_api_credentials_info["environment"])
        admin = client.get_admin(token=data_api_credentials_kwargs["token"])
        db_m = admin.get_async_database(
            data_api_credentials_kwargs["api_endpoint"],
            keyspace="M",
        )
        assert db_m.keyspace == "M"
        db_n = admin.get_async_database(data_api_credentials_kwargs["api_endpoint"])
        assert isinstance(db_n.keyspace, str)  # i.e. resolution took place

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe(
        "test database-from-astradbadmin default keyspace per environment, async"
    )
    def test_async_database_from_astradbadmin_default_keyspace_per_environment(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        client = DataAPIClient(environment=data_api_credentials_info["environment"])
        admin = client.get_admin(token=data_api_credentials_kwargs["token"])
        db_admin = admin.get_database_admin(data_api_credentials_kwargs["api_endpoint"])
        db_m = db_admin.get_async_database(keyspace="M")
        assert db_m.keyspace == "M"
        db_n = db_admin.get_async_database()
        assert isinstance(db_n.keyspace, str)  # i.e. resolution took place

    @pytest.mark.describe("test of collection find-and-rerank lifecycle, async")
    async def test_collection_farr_lifecycle_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            TEST_FARR_COLLECTION_NAME = "test_farr_coll"
            col1 = await async_database.create_collection(
                TEST_FARR_COLLECTION_NAME,
                definition=CollectionDefinition(
                    vector=CollectionVectorOptions(
                        dimension=10,
                        metric=VectorMetric.EUCLIDEAN,
                    ),
                    rerank=CollectionRerankOptions(
                        service=RerankServiceOptions(
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
            col1_dict = await async_database.create_collection(
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
            col1_fluent = await async_database.create_collection(
                TEST_FARR_COLLECTION_NAME,
                definition=fluent_definition1,
            )
            assert col1_fluent == col1

            lc_response = await async_database.list_collections()

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

            col2 = async_database.get_collection(TEST_FARR_COLLECTION_NAME)
            assert col1 == col2
        finally:
            await async_database.drop_collection(TEST_FARR_COLLECTION_NAME)

    @pytest.mark.describe("test of collection lexical detailed config, async")
    async def test_collection_lexical_detailedconfig_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        TEST_LEXICAL_DETAILEDCONFIG_COLLECTION_NAME = "test_lexical_detcfg_coll"
        try:
            await async_database.create_collection(
                TEST_LEXICAL_DETAILEDCONFIG_COLLECTION_NAME,
                definition=(
                    CollectionDefinition.builder()
                    .set_lexical(
                        {
                            "tokenizer": {"name": "standard", "args": {}},
                            "filters": [
                                {"name": "lowercase"},
                                {"name": "stop"},
                                {"name": "porterstem"},
                                {"name": "asciifolding"},
                            ],
                            "charFilters": [],
                        }
                    )
                    .build()
                ),
            )

            coll_ldc_definition = CollectionDefinition(
                lexical=CollectionLexicalOptions(
                    analyzer={
                        "tokenizer": {"name": "standard", "args": {}},
                        "filters": [
                            {"name": "lowercase"},
                            {"name": "stop"},
                            {"name": "porterstem"},
                            {"name": "asciifolding"},
                        ],
                        "charFilters": [],
                    },
                    enabled=True,
                ),
            )
            await async_database.create_collection(
                TEST_LEXICAL_DETAILEDCONFIG_COLLECTION_NAME,
                definition=coll_ldc_definition,
            )

        finally:
            await async_database.drop_collection(
                TEST_LEXICAL_DETAILEDCONFIG_COLLECTION_NAME,
            )

    @pytest.mark.describe("test of collection lexical omitted settings, async")
    async def test_collection_lexical_omittedsettings_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        TEST_LEXICAL_OMITTEDSETTINGS_COLLECTION_NAME = "test_lexical_nocfg_coll"
        try:
            await async_database.create_collection(
                TEST_LEXICAL_OMITTEDSETTINGS_COLLECTION_NAME,
                definition=(
                    CollectionDefinition.builder().set_lexical(enabled=True).build()
                ),
            )

            coll_ldc_definition = CollectionDefinition(
                lexical=CollectionLexicalOptions(enabled=True),
            )
            await async_database.create_collection(
                TEST_LEXICAL_OMITTEDSETTINGS_COLLECTION_NAME,
                definition=coll_ldc_definition,
            )

        finally:
            await async_database.drop_collection(
                TEST_LEXICAL_OMITTEDSETTINGS_COLLECTION_NAME,
            )
