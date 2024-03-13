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

from typing import List

import pytest

from astrapy import AsyncCollection, AsyncDatabase
from astrapy.exceptions import (
    CollectionAlreadyExistsException,
    CollectionNotFoundException,
    DataAPIResponseException,
    DevOpsAPIException,
    InsertManyException,
    TooManyDocumentsToCountException,
)
from astrapy.constants import DocumentType
from astrapy.cursors import AsyncCursor

from ..conftest import AstraDBCredentials


class TestExceptionsAsync:
    @pytest.mark.describe("test of collection insert_many failure modes, async")
    async def test_collection_insert_many_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:

        async def _alist(acursor: AsyncCursor) -> List[DocumentType]:
            return [doc async for doc in acursor]

        acol = async_empty_collection
        bad_docs = [{"_id": tid} for tid in ["a", "b", "c", ValueError, "e", "f"]]
        dup_docs = [{"_id": tid} for tid in ["a", "b", "b", "d", "a", "b", "e", "f"]]
        ok_docs = [{"_id": tid} for tid in ["a", "b", "c", "d", "e", "f"]]

        with pytest.raises(ValueError):
            await acol.insert_many([], ordered=True, concurrency=2)

        with pytest.raises(TypeError):
            await acol.insert_many(bad_docs, ordered=True, chunk_size=2)

        with pytest.raises(TypeError):
            await acol.insert_many(bad_docs, ordered=False, chunk_size=2, concurrency=1)

        with pytest.raises(TypeError):
            await acol.insert_many(bad_docs, ordered=False, chunk_size=2, concurrency=2)

        await acol.delete_all()
        im_result1 = await acol.insert_many(
            ok_docs, ordered=True, chunk_size=2, concurrency=1
        )
        assert len(im_result1.inserted_ids) == 6
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_all()
        im_result2 = await acol.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=1
        )
        assert len(im_result2.inserted_ids) == 6
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_all()
        im_result3 = await acol.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=2
        )
        assert len(im_result3.inserted_ids) == 6
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_all()
        with pytest.raises(InsertManyException) as exc:
            await acol.insert_many(dup_docs, ordered=True, chunk_size=2, concurrency=1)
        assert len(exc.value.error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert exc.value.partial_result.inserted_ids == ["a", "b"]
        assert len(exc.value.partial_result.raw_results) == 2
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b"}

        await acol.delete_all()
        with pytest.raises(InsertManyException) as exc:
            await acol.insert_many(dup_docs, ordered=False, chunk_size=2, concurrency=1)
        assert len(exc.value.error_descriptors) == 3
        assert len(exc.value.detailed_error_descriptors) == 2
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[1].error_descriptors) == 2
        assert set(exc.value.partial_result.inserted_ids) == {"a", "b", "d", "e", "f"}
        assert len(exc.value.partial_result.raw_results) == 4
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b", "d", "e", "f"}

        await acol.delete_all()
        with pytest.raises(InsertManyException) as exc:
            im_result3 = await acol.insert_many(
                dup_docs, ordered=False, chunk_size=2, concurrency=2
            )
        assert len(exc.value.error_descriptors) == 3
        assert len(exc.value.detailed_error_descriptors) == 2
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[1].error_descriptors) == 2
        assert set(exc.value.partial_result.inserted_ids) == {"a", "b", "d", "e", "f"}
        assert len(exc.value.partial_result.raw_results) == 4
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b", "d", "e", "f"}

    @pytest.mark.describe("test of collection insert_one failure modes, async")
    async def test_collection_insert_one_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection
        with pytest.raises(TypeError):
            await acol.insert_one({"a": ValueError})

        await acol.insert_one({"_id": "a"})
        with pytest.raises(DataAPIResponseException) as exc:
            await acol.insert_one({"_id": "a"})
        assert len(exc.value.error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert isinstance(exc.value.detailed_error_descriptors[0].command, dict)

    @pytest.mark.describe("test of collection options failure modes, async")
    async def test_collection_options_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection._copy()
        acol._astra_db_collection.collection_name = "hacked"
        with pytest.raises(CollectionNotFoundException) as exc:
            await acol.options()
        assert exc.value.collection_name == "hacked"
        assert exc.value.namespace == async_empty_collection.namespace

    @pytest.mark.describe("test of collection count_documents failure modes, async")
    async def test_collection_count_documents_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection._copy()
        acol._astra_db_collection.collection_name += "_hacked"
        acol._astra_db_collection.base_path += "_hacked"
        with pytest.raises(DataAPIResponseException):
            await acol.count_documents({}, upper_bound=1)
        await async_empty_collection.insert_one({"a": 1})
        await async_empty_collection.insert_one({"b": 2})
        assert await async_empty_collection.count_documents({}, upper_bound=3) == 2
        with pytest.raises(TooManyDocumentsToCountException) as exc:
            await async_empty_collection.count_documents({}, upper_bound=1)
        assert not exc.value.server_max_count_exceeded

    @pytest.mark.describe("test of collection one-doc DML failure modes, async")
    async def test_collection_monodoc_dml_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection._copy()
        acol._astra_db_collection.collection_name += "_hacked"
        acol._astra_db_collection.base_path += "_hacked"
        with pytest.raises(DataAPIResponseException):
            await acol.delete_all()
        with pytest.raises(DataAPIResponseException):
            await acol.delete_one({"a": 1})
        with pytest.raises(DataAPIResponseException):
            await acol.find_one_and_replace({"a": 1}, {"a": -1})
        with pytest.raises(DataAPIResponseException):
            await acol.find_one_and_update({"a": 1}, {"$set": {"a": -1}})
        with pytest.raises(DataAPIResponseException):
            await acol.find_one_and_delete({"a": 1})
        with pytest.raises(DataAPIResponseException):
            await acol.replace_one({"a": 1}, {"a": -1})
        with pytest.raises(DataAPIResponseException):
            await acol.update_one({"a": 1}, {"$set": {"a": -1}})

    @pytest.mark.describe("test of check_exists for database create_collection, async")
    async def test_database_create_collection_check_exists_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_check_exists"
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=3,
        )

        with pytest.raises(CollectionAlreadyExistsException):
            await async_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                dimension=3,
            )
        with pytest.raises(CollectionAlreadyExistsException):
            await async_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                indexing={"deny": ["a"]},
            )
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=3,
            check_exists=False,
        )
        with pytest.raises(DataAPIResponseException):
            await async_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                indexing={"deny": ["a"]},
                check_exists=False,
            )

        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)

    @pytest.mark.describe("test of database drop_collection failures, async")
    async def test_database_drop_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        f_database = async_database._copy(namespace="nonexisting")
        with pytest.raises(DataAPIResponseException):
            await f_database.drop_collection("nonexisting")
        with pytest.raises(DataAPIResponseException):
            await async_database.command(body={"myCommand": {"k": "v"}})
        with pytest.raises(DataAPIResponseException):
            await async_database.command(body={"myCommand": {"k": "v"}}, namespace="ns")
        with pytest.raises(DataAPIResponseException):
            await async_database.command(
                body={"myCommand": {"k": "v"}}, namespace="ns", collection_name="coll"
            )
        with pytest.raises(DataAPIResponseException):
            await async_database.list_collections(namespace="nonexisting")
        with pytest.raises(DataAPIResponseException):
            await async_database.list_collection_names(namespace="nonexisting")

    @pytest.mark.describe("test of database info failures, async")
    async def test_get_database_info_failures_async(
        self,
        async_database: AsyncDatabase,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        hacked_ns = (astra_db_credentials_kwargs["namespace"] or "") + "_hacked"
        with pytest.raises(DevOpsAPIException):
            async_database._copy(namespace=hacked_ns).info
