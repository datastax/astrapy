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

from typing import TypedDict

import pytest

from astrapy import AsyncCollection, AsyncDatabase, Collection, Database
from astrapy.constants import VectorMetric

from ..conftest import DefaultAsyncCollection, DefaultCollection


class TestDoc(TypedDict):
    p_bigint: int
    p_ascii: str


DOCUMENT = {"p_ascii": "abc", "p_bigint": 10000, "p_float": 0.123}
TYPED_DOCUMENT: TestDoc = {"p_ascii": "abc", "p_bigint": 10000}
FIND_FILTER = {"p_ascii": "abc", "p_bigint": 10000}


class TestCollectionTyping:
    @pytest.mark.describe("test of typing create_collection, sync")
    def test_create_collection_typing_sync(
        self,
        sync_database: Database,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        """Test of creating typed collections with generics (and not), sync."""

        # Untyped baseline
        c_co_untyped = sync_database.create_collection(
            sync_empty_collection.name,
            dimension=2,
            metric=VectorMetric.COSINE,
            indexing={"deny": ["not_indexed"]},
        )
        c_co_untyped.insert_one(DOCUMENT)
        cu_doc = c_co_untyped.find_one(FIND_FILTER)
        assert cu_doc is not None
        cu_a: str
        cu_b: int
        cu_a = cu_doc["p_ascii"]  # noqa: F841
        cu_b = cu_doc["p_bigint"]  # noqa: F841
        assert set(cu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        cu_x: int
        cu_y: float
        cu_x = cu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            cu_y = cu_doc["c"]  # noqa: F841
        c_co_untyped.delete_many({})

        # Typed
        c_co_typed: Collection[TestDoc] = sync_database.create_collection(
            sync_empty_collection.name,
            dimension=2,
            metric=VectorMetric.COSINE,
            indexing={"deny": ["not_indexed"]},
            document_type=TestDoc,
        )
        c_co_typed.insert_one(TYPED_DOCUMENT)
        ct_doc = c_co_typed.find_one(FIND_FILTER)
        assert ct_doc is not None
        ct_a: str
        ct_b: int
        ct_a = ct_doc["p_ascii"]  # noqa: F841
        ct_b = ct_doc["p_bigint"]  # noqa: F841
        assert set(ct_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ct_x: int
        ct_y: float
        ct_x = ct_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ct_y = ct_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841
        c_co_typed.delete_many({})

    @pytest.mark.describe("test of typing get_collection, sync")
    def test_get_collection_typing_sync(
        self,
        sync_database: Database,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        """Test of getting typed collections with generics (and not), sync."""

        # Untyped baseline
        g_co_untyped = sync_database.get_collection(sync_empty_collection.name)
        g_co_untyped.insert_one(DOCUMENT)
        gu_doc = g_co_untyped.find_one(FIND_FILTER)
        assert gu_doc is not None
        gu_a: str
        gu_b: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_b = gu_doc["p_bigint"]  # noqa: F841
        assert set(gu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        gu_x: int
        gu_y: float
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            gu_y = gu_doc["c"]  # noqa: F841
        g_co_untyped.delete_many({})

        # Typed
        g_co_typed: Collection[TestDoc] = sync_database.get_collection(
            sync_empty_collection.name,
            document_type=TestDoc,
        )
        g_co_typed.insert_one(TYPED_DOCUMENT)
        gt_doc = g_co_typed.find_one(FIND_FILTER)
        assert gt_doc is not None
        gt_a: str
        gt_b: int
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_b = gt_doc["p_bigint"]  # noqa: F841
        assert set(gt_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        gt_x: int
        gt_y: float
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841
        g_co_typed.delete_many({})

    @pytest.mark.describe("test of typing create_collection, async")
    async def test_create_collection_typing_async(
        self,
        async_database: AsyncDatabase,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        """Test of creating typed collections with generics (and not), async."""

        # Untyped baseline
        ac_co_untyped = await async_database.create_collection(
            async_empty_collection.name,
            dimension=2,
            metric=VectorMetric.COSINE,
            indexing={"deny": ["not_indexed"]},
        )
        await ac_co_untyped.insert_one(DOCUMENT)
        cu_doc = await ac_co_untyped.find_one(FIND_FILTER)
        assert cu_doc is not None
        cu_a: str
        cu_b: int
        cu_a = cu_doc["p_ascii"]  # noqa: F841
        cu_b = cu_doc["p_bigint"]  # noqa: F841
        assert set(cu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        cu_x: int
        cu_y: float
        cu_x = cu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            cu_y = cu_doc["c"]  # noqa: F841
        await ac_co_untyped.delete_many({})

        # Typed
        ac_co_typed: AsyncCollection[TestDoc] = await async_database.create_collection(
            async_empty_collection.name,
            dimension=2,
            metric=VectorMetric.COSINE,
            indexing={"deny": ["not_indexed"]},
            document_type=TestDoc,
        )
        await ac_co_typed.insert_one(TYPED_DOCUMENT)
        ct_doc = await ac_co_typed.find_one(FIND_FILTER)
        assert ct_doc is not None
        ct_a: str
        ct_b: int
        ct_a = ct_doc["p_ascii"]  # noqa: F841
        ct_b = ct_doc["p_bigint"]  # noqa: F841
        assert set(ct_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ct_x: int
        ct_y: float
        ct_x = ct_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ct_y = ct_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841
        await ac_co_typed.delete_many({})

    @pytest.mark.describe("test of typing get_collection, async")
    async def test_get_collection_typing_async(
        self,
        async_database: AsyncDatabase,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        """Test of getting typed collections with generics (and not), async."""

        # Untyped baseline
        ag_co_untyped = await async_database.get_collection(async_empty_collection.name)
        await ag_co_untyped.insert_one(DOCUMENT)
        gu_doc = await ag_co_untyped.find_one(FIND_FILTER)
        assert gu_doc is not None
        gu_a: str
        gu_b: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_b = gu_doc["p_bigint"]  # noqa: F841
        assert set(gu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        gu_x: int
        gu_y: float
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            gu_y = gu_doc["c"]  # noqa: F841
        await ag_co_untyped.delete_many({})

        # Typed
        ag_co_typed: AsyncCollection[TestDoc] = await async_database.get_collection(
            async_empty_collection.name,
            document_type=TestDoc,
        )
        await ag_co_typed.insert_one(TYPED_DOCUMENT)
        gt_doc = await ag_co_typed.find_one(FIND_FILTER)
        assert gt_doc is not None
        gt_a: str
        gt_b: int
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_b = gt_doc["p_bigint"]  # noqa: F841
        assert set(gt_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        gt_x: int
        gt_y: float
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841
        await ag_co_typed.delete_many({})
