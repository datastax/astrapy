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

from astrapy import AsyncCollection
from astrapy.exceptions import (
    DataAPIResponseException,
    InsertManyException,
)
from astrapy.constants import DocumentType
from astrapy.cursors import AsyncCursor


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
