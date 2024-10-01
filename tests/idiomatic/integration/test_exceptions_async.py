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

import pytest

from astrapy import AsyncCollection, AsyncDatabase
from astrapy.constants import DocumentType
from astrapy.cursors import AsyncCursor
from astrapy.exceptions import (
    BulkWriteException,
    CollectionAlreadyExistsException,
    CollectionNotFoundException,
    CursorIsStartedException,
    DataAPIResponseException,
    DevOpsAPIException,
    InsertManyException,
    TooManyDocumentsToCountException,
)
from astrapy.operations import AsyncInsertOne

from ..conftest import IS_ASTRA_DB, DataAPICredentials, async_fail_if_not_removed


class TestExceptionsAsync:
    @pytest.mark.describe("test of collection insert_many type-failure modes, async")
    async def test_collection_insert_many_type_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection
        bad_docs = [{"_id": tid} for tid in ["a", "b", "c", ValueError, "e", "f"]]

        with pytest.raises(ValueError):
            await acol.insert_many([], ordered=True, concurrency=2)

        with pytest.raises(TypeError):
            await acol.insert_many(bad_docs, ordered=True, chunk_size=2)

        with pytest.raises(TypeError):
            await acol.insert_many(bad_docs, ordered=False, chunk_size=2, concurrency=1)

        with pytest.raises(TypeError):
            await acol.insert_many(bad_docs, ordered=False, chunk_size=2, concurrency=2)

    @pytest.mark.describe("test of collection insert_many insert-failure modes, async")
    async def test_collection_insert_many_insert_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        async def _alist(acursor: AsyncCursor) -> list[DocumentType]:
            return [doc async for doc in acursor]

        acol = async_empty_collection
        dup_docs = [{"_id": tid} for tid in ["a", "b", "b", "d", "a", "b", "e", "f"]]
        ok_docs = [{"_id": tid} for tid in ["a", "b", "c", "d", "e", "f"]]

        im_result1 = await acol.insert_many(
            ok_docs, ordered=True, chunk_size=2, concurrency=1
        )
        assert len(im_result1.inserted_ids) == 6
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_many({})
        im_result2 = await acol.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=1
        )
        assert len(im_result2.inserted_ids) == 6
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_many({})
        im_result3 = await acol.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=2
        )
        assert len(im_result3.inserted_ids) == 6
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_many({})
        with pytest.raises(InsertManyException) as exc:
            await acol.insert_many(dup_docs, ordered=True, chunk_size=2, concurrency=1)
        assert len(exc.value.error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert exc.value.partial_result.inserted_ids == ["a", "b"]
        assert len(exc.value.partial_result.raw_results) == 2
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b"}

        await acol.delete_many({})
        with pytest.raises(InsertManyException) as exc:
            await acol.insert_many(dup_docs, ordered=False, chunk_size=2, concurrency=1)
        assert len(exc.value.error_descriptors) == 3
        assert len(exc.value.detailed_error_descriptors) == 2
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[1].error_descriptors) == 2
        assert set(exc.value.partial_result.inserted_ids) == {"a", "b", "d", "e", "f"}
        assert len(exc.value.partial_result.raw_results) == 4
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b", "d", "e", "f"}

        await acol.delete_many({})
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
        acol._name += "_hacked"
        with pytest.raises(CollectionNotFoundException) as exc:
            await acol.options()
        assert exc.value.collection_name == acol._name
        assert exc.value.keyspace == async_empty_collection.keyspace

    @pytest.mark.describe("test of collection count_documents failure modes, async")
    async def test_collection_count_documents_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection._copy()
        acol._name += "_hacked"
        acol._api_commander.full_path += "_hacked"
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
        acol._name += "_hacked"
        acol._api_commander.full_path += "_hacked"
        with pytest.raises(DataAPIResponseException):
            await acol.delete_many({})
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

    @async_fail_if_not_removed
    @pytest.mark.describe("test of exceptions in ordered bulk_write, async")
    async def test_ordered_bulk_write_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        i1 = AsyncInsertOne({"_id": "a"})
        i3 = AsyncInsertOne({"_id": "z"})

        with pytest.raises(BulkWriteException) as exc:
            await async_empty_collection.bulk_write([i1, i1, i3], ordered=True)
        assert set(exc.value.partial_result.bulk_api_results.keys()) == {0}
        assert exc.value.partial_result.deleted_count == 0
        assert exc.value.partial_result.inserted_count == 1
        assert exc.value.partial_result.matched_count == 0
        assert exc.value.partial_result.modified_count == 0
        assert exc.value.partial_result.upserted_count == 0
        assert exc.value.partial_result.upserted_ids == {}
        assert await async_empty_collection.count_documents({}, upper_bound=10) == 1

    @async_fail_if_not_removed
    @pytest.mark.describe("test of hard exceptions in ordered bulk_write, async")
    async def test_ordered_bulk_write_error_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        i1 = AsyncInsertOne({"_id": "a"})
        i2 = AsyncInsertOne({"_id": ValueError("unserializable")})

        with pytest.raises(TypeError):
            await async_empty_collection.bulk_write([i1, i2])

    @async_fail_if_not_removed
    @pytest.mark.describe("test of exceptions in unordered bulk_write, async")
    async def test_unordered_bulk_write_failures_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        i1 = AsyncInsertOne({"_id": "a"})
        i3 = AsyncInsertOne({"_id": "z"})

        with pytest.raises(BulkWriteException) as exc:
            await async_empty_collection.bulk_write([i1, i1, i3], ordered=False)
        # whether '0' or '1' succeeds in inserting 'a' is random:
        assert set(exc.value.partial_result.bulk_api_results.keys()) in [{0, 2}, {1, 2}]
        assert exc.value.partial_result.deleted_count == 0
        assert exc.value.partial_result.inserted_count == 2
        assert exc.value.partial_result.matched_count == 0
        assert exc.value.partial_result.modified_count == 0
        assert exc.value.partial_result.upserted_count == 0
        assert exc.value.partial_result.upserted_ids == {}
        assert await async_empty_collection.count_documents({}, upper_bound=10) == 2

    @async_fail_if_not_removed
    @pytest.mark.describe("test of hard exceptions in unordered bulk_write, async")
    async def test_unordered_bulk_write_error_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        i1 = AsyncInsertOne({"_id": "a"})
        i2 = AsyncInsertOne({"_id": ValueError("unserializable")})

        with pytest.raises(TypeError):
            await async_empty_collection.bulk_write([i1, i2], ordered=False)

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

    @pytest.mark.describe("test of database one-request method failures, async")
    async def test_database_method_failures_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        f_database = async_database._copy(keyspace="nonexisting")
        if IS_ASTRA_DB:
            with pytest.raises(DataAPIResponseException):
                await f_database.drop_collection("nonexisting")
        with pytest.raises(DataAPIResponseException):
            await async_database.command(body={"myCommand": {"k": "v"}})
        with pytest.raises(DataAPIResponseException):
            await async_database.command(body={"myCommand": {"k": "v"}}, keyspace="ns")
        with pytest.raises(DataAPIResponseException):
            await async_database.command(
                body={"myCommand": {"k": "v"}}, keyspace="ns", collection_name="coll"
            )
        with pytest.raises(DataAPIResponseException):
            [
                coll
                async for coll in async_database.list_collections(
                    keyspace="nonexisting"
                )
            ]
        with pytest.raises(DataAPIResponseException):
            await async_database.list_collection_names(keyspace="nonexisting")

    @pytest.mark.describe("test of database info failures, async")
    async def test_get_database_info_failures_async(
        self,
        async_database: AsyncDatabase,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        hacked_ks = (data_api_credentials_kwargs["namespace"] or "") + "_hacked"
        with pytest.raises(DevOpsAPIException):
            async_database._copy(keyspace=hacked_ks).info()

    @pytest.mark.describe("test of hard exceptions in cursors, async")
    async def test_cursor_hard_exceptions_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        with pytest.raises(TypeError):
            await async_empty_collection.find(
                {},
                sort={"f": ValueError("nonserializable")},
            ).distinct("a")
        # # Note: this, i.e. cursor[i]/cursor[i:j], is disabled
        # # pending full skip/limit support by the Data API.
        # with pytest.raises(IndexError):
        #     async_empty_collection.find(
        #         {},
        #         sort={"f": SortDocuments.ASCENDING},
        #     )[100]

    @pytest.mark.describe("test of custom exceptions in cursors, async")
    async def test_cursor_custom_exceptions_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many([{"a": 1}] * 4)
        cur1 = async_empty_collection.find({})
        cur1.limit(10)

        await cur1.__anext__()
        with pytest.raises(CursorIsStartedException) as exc:
            cur1.limit(1)
        assert exc.value.cursor_state == "running"

        [doc async for doc in cur1]
        with pytest.raises(CursorIsStartedException) as exc:
            cur1.limit(1)
        assert exc.value.cursor_state == "exhausted"

    @pytest.mark.describe("test of standard exceptions in cursors, async")
    async def test_cursor_standard_exceptions_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        awcol = async_empty_collection._copy(keyspace="nonexisting")
        cur1 = awcol.find({})
        cur2 = awcol.find({})
        cur3 = awcol.find({})

        with pytest.raises(DataAPIResponseException):
            async for item in cur1:
                pass

        with pytest.raises(DataAPIResponseException):
            await cur2.__anext__()

        with pytest.raises(DataAPIResponseException):
            await cur3.distinct("f")

        with pytest.raises(DataAPIResponseException):
            await awcol.distinct("f")

        with pytest.raises(DataAPIResponseException):
            await awcol.find_one({})

    @pytest.mark.describe("test of exceptions in command-cursors, async")
    async def test_commandcursor_hard_exceptions_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        with pytest.raises(DataAPIResponseException):
            async_database.list_collections(keyspace="nonexisting")

        cur1 = async_database.list_collections()
        [doc async for doc in cur1]
        with pytest.raises(CursorIsStartedException) as exc:
            async for col in cur1:
                pass
        assert exc.value.cursor_state == "exhausted"
