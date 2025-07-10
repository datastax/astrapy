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

from astrapy import AsyncDatabase
from astrapy.constants import DefaultDocumentType
from astrapy.cursors import AsyncCollectionFindCursor, CursorState
from astrapy.exceptions import (
    CollectionDeleteManyException,
    CollectionInsertManyException,
    CollectionUpdateManyException,
    CursorException,
    DataAPIResponseException,
    TooManyDocumentsToCountException,
)

from ..conftest import IS_ASTRA_DB, DefaultAsyncCollection


class TestCollectionExceptionsAsync:
    @pytest.mark.describe("test of collection insert_many type-failure modes, async")
    async def test_collection_insert_many_type_failures_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
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
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        async def _alist(
            acursor: AsyncCollectionFindCursor[
                DefaultDocumentType, DefaultDocumentType
            ],
        ) -> list[DefaultDocumentType]:
            return [doc async for doc in acursor]

        acol = async_empty_collection
        ok_ids = ["a", "b", "c", "d", "e", "f"]
        ok_docs = [{"_id": tid, "i": i} for i, tid in enumerate(ok_ids)]
        dup_ids = ["a", "b", "b", "d", "a", "b", "e", "f"]
        dup_docs = [{"_id": tid, "i": i} for i, tid in enumerate(dup_ids)]

        im_result1 = await acol.insert_many(
            ok_docs, ordered=True, chunk_size=2, concurrency=1
        )
        assert set(im_result1.inserted_ids) == set(ok_ids)
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_many({})
        im_result2 = await acol.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=1
        )
        assert set(im_result2.inserted_ids) == set(ok_ids)
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_many({})
        im_result3 = await acol.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=2
        )
        assert set(im_result3.inserted_ids) == set(ok_ids)
        assert len(await _alist(acol.find({}))) == 6

        await acol.delete_many({})
        with pytest.raises(CollectionInsertManyException) as exc:
            await acol.insert_many(dup_docs, ordered=True, chunk_size=2, concurrency=1)
        assert len(exc.value.exceptions) == 1
        the_exception = exc.value.exceptions[0]
        assert isinstance(the_exception, DataAPIResponseException)
        assert len(the_exception.error_descriptors) == 1
        assert isinstance(the_exception.command, dict)
        assert isinstance(the_exception.raw_response, dict)
        assert exc.value.inserted_ids == ["a", "b"]
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b"}

        await acol.delete_many({})
        with pytest.raises(CollectionInsertManyException) as exc:
            await acol.insert_many(dup_docs, ordered=False, chunk_size=2, concurrency=1)
        assert len(exc.value.exceptions) == 2
        the_exceptions = exc.value.exceptions
        assert all(isinstance(exc, DataAPIResponseException) for exc in the_exceptions)
        assert all(isinstance(exc.command, dict) for exc in the_exceptions)  # type: ignore[attr-defined]
        assert all(isinstance(exc.raw_response, dict) for exc in the_exceptions)  # type: ignore[attr-defined]
        assert the_exceptions[0].command != the_exceptions[1].command  # type: ignore[attr-defined]
        assert the_exceptions[0].raw_response != the_exceptions[1].raw_response  # type: ignore[attr-defined]
        assert (
            len(the_exceptions[0].error_descriptors)  # type: ignore[attr-defined]
            + len(the_exceptions[1].error_descriptors)  # type: ignore[attr-defined]
            == 3
        )
        assert set(exc.value.inserted_ids) == {"a", "b", "d", "e", "f"}
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b", "d", "e", "f"}

        await acol.delete_many({})
        with pytest.raises(CollectionInsertManyException) as exc:
            im_result3 = await acol.insert_many(
                dup_docs, ordered=False, chunk_size=2, concurrency=2
            )
        assert len(exc.value.exceptions) == 2
        the_exceptions = exc.value.exceptions
        assert all(isinstance(exc, DataAPIResponseException) for exc in the_exceptions)
        assert all(isinstance(exc.command, dict) for exc in the_exceptions)  # type: ignore[attr-defined]
        assert all(isinstance(exc.raw_response, dict) for exc in the_exceptions)  # type: ignore[attr-defined]
        assert the_exceptions[0].command != the_exceptions[1].command  # type: ignore[attr-defined]
        assert the_exceptions[0].raw_response != the_exceptions[1].raw_response  # type: ignore[attr-defined]
        assert (
            len(the_exceptions[0].error_descriptors)  # type: ignore[attr-defined]
            + len(the_exceptions[1].error_descriptors)  # type: ignore[attr-defined]
            == 3
        )
        assert set(exc.value.inserted_ids) == {"a", "b", "d", "e", "f"}
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b", "d", "e", "f"}

    @pytest.mark.describe("test of collection insert_one failure modes, async")
    async def test_collection_insert_one_failures_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        acol = async_empty_collection
        with pytest.raises(TypeError):
            await acol.insert_one({"a": ValueError})

        await acol.insert_one({"_id": "a"})
        with pytest.raises(DataAPIResponseException) as exc:
            await acol.insert_one({"_id": "a"})
        assert len(exc.value.error_descriptors) == 1
        assert isinstance(exc.value.command, dict)

    @pytest.mark.describe("test of collection options failure modes, async")
    async def test_collection_options_failures_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        acol = async_empty_collection._copy()
        acol._name += "_hacked"
        with pytest.raises(RuntimeError, match="not found"):
            await acol.options()

    @pytest.mark.describe("test of collection count_documents failure modes, async")
    async def test_collection_count_documents_failures_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
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

    @pytest.mark.describe("test of collection bulk-method DML failure modes, async")
    async def test_collection_bulkmethods_dml_failures_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        acol = async_empty_collection._copy()
        acol._name += "_hacked"
        acol._api_commander.full_path += "_hacked"
        with pytest.raises(CollectionDeleteManyException):
            await acol.delete_many({})
        with pytest.raises(CollectionUpdateManyException):
            await acol.update_many({}, update={"$set": {"a": 1}})

    @pytest.mark.describe("test of collection one-doc DML failure modes, async")
    async def test_collection_monodoc_dml_failures_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        acol = async_empty_collection._copy()
        acol._name += "_hacked"
        acol._api_commander.full_path += "_hacked"
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
                body={"myCommand": {"k": "v"}},
                keyspace="ns",
                collection_or_table_name="coll",
            )
        with pytest.raises(DataAPIResponseException):
            [
                coll
                for coll in await async_database.list_collections(
                    keyspace="nonexisting"
                )
            ]
        with pytest.raises(DataAPIResponseException):
            await async_database.list_collection_names(keyspace="nonexisting")

    @pytest.mark.describe("test of hard exceptions in cursors, async")
    async def test_cursor_hard_exceptions_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        with pytest.raises(TypeError):
            await async_empty_collection.distinct(
                "a",
                filter={"f": ValueError("nonserializable")},
            )

    @pytest.mark.describe("test of custom exceptions in cursors, async")
    async def test_cursor_custom_exceptions_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many([{"a": 1}] * 4)
        cur1 = async_empty_collection.find({})
        cur1.limit(10)

        await cur1.__anext__()
        with pytest.raises(CursorException) as exc:
            cur1.limit(1)
        assert exc.value.cursor_state == CursorState.STARTED.value

        [doc async for doc in cur1]
        with pytest.raises(CursorException) as exc:
            cur1.limit(1)
        assert exc.value.cursor_state == CursorState.CLOSED.value

    @pytest.mark.describe("test of standard exceptions in cursors, async")
    async def test_cursor_standard_exceptions_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        awcol = async_database.get_collection("nonexisting")
        cur1 = awcol.find({})
        cur2 = awcol.find({})

        with pytest.raises(DataAPIResponseException):
            async for item in cur1:
                pass

        with pytest.raises(DataAPIResponseException):
            await cur2.__anext__()

        with pytest.raises(DataAPIResponseException):
            await awcol.distinct("f")

        with pytest.raises(DataAPIResponseException):
            await awcol.find_one({})

    @pytest.mark.describe("test of exceptions in list_collections, async")
    async def test_list_collections_hard_exceptions_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        with pytest.raises(DataAPIResponseException):
            await async_database.list_collections(keyspace="nonexisting")
