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

# from ..conftest import is_nvidia_service_available
from astrapy import AsyncCollection, AsyncDatabase
from astrapy.exceptions import DataAPIResponseException
from astrapy.operations import (
    AsyncInsertOne,
    AsyncInsertMany,
    AsyncUpdateOne,
    AsyncReplaceOne,
    AsyncDeleteOne,
)


class TestVectorizeMethodsAsync:
    # @pytest.mark.skipif(
    #     not is_nvidia_service_available(), reason="No 'service' on this database"
    # )
    @pytest.mark.describe("test of vectorize in collection methods, async")
    async def test_collection_methods_vectorize_async(
        self,
        async_empty_service_collection: AsyncCollection,
        service_vector_dimension: int,
    ) -> None:
        acol = async_empty_service_collection

        await acol.insert_one({"t": "tower"}, vectorize="How high is this tower?")
        await acol.insert_one({"t": "vectorless"})
        await acol.insert_one(
            {"t": "vectorful"}, vector=[0.01] * service_vector_dimension
        )

        await acol.insert_many(
            [{"t": "guide"}, {"t": "seeds"}],
            vectorize=[
                "This is the instructions manual. Read it!",
                "Other plants rely on wind to propagate their seeds.",
            ],
        )
        await acol.insert_many(
            [{"t": "dog"}, {"t": "cat_novector"}, {"t": "spider"}],
            vectorize=[
                None,
                None,
                "The eye pattern is a primary criterion to the family.",
            ],
            vectors=[
                [0.01] * service_vector_dimension,
                None,
                None,
            ],
        )

        doc = await acol.find_one(
            {},
            vectorize="This building is five storeys tall.",
            projection={"$vector": False},
        )
        assert doc is not None
        assert doc["t"] == "tower"

        docs = [
            doc
            async for doc in acol.find(
                {},
                vectorize="This building is five storeys tall.",
                limit=2,
                projection={"$vector": False},
            )
        ]
        assert docs[0]["t"] == "tower"

        rdoc = await acol.find_one_and_replace(
            {},
            {"t": "spider", "$vectorize": "Check out the eyes!"},
            vectorize="The disposition of the eyes tells much",
            projection={"$vector": False},
        )
        assert rdoc["t"] == "spider"

        r1res = await acol.replace_one(
            {},
            {"t": "spider", "$vectorize": "Look at how the eyes are placed"},
            vectorize="The disposition of the eyes tells much",
        )
        assert r1res.update_info["nModified"] == 1

        udoc = await acol.find_one_and_update(
            {},
            {"$set": {"$vectorize": "Consider consulting the how-to"}},
            vectorize="Have a look at the user guide...",
            projection={"$vector": False},
        )
        assert udoc["t"] == "guide"

        u1res = await acol.update_one(
            {},
            {"$set": {"$vectorize": "Know how to operate it before doing so."}},
            vectorize="Have a look at the user guide...",
        )
        assert u1res.update_info["nModified"] == 1

        ddoc = await acol.find_one_and_delete(
            {},
            vectorize="Some trees have seeds that are dispersed in the air!",
            projection={"$vector": False},
        )
        assert ddoc["t"] == "seeds"

        d1res = await acol.delete_one(
            {},
            vectorize="yet another giant construction in this suburb.",
        )
        assert d1res.deleted_count == 1

    # @pytest.mark.skipif(
    #     not is_nvidia_service_available(), reason="No 'service' on this database"
    # )
    @pytest.mark.describe("test of bulk_write with vectorize, async")
    async def test_collection_bulk_write_vectorize_async(
        self,
        async_empty_service_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_service_collection

        bw_ops = [
            AsyncInsertOne({"a": 1}, vectorize="The cat is on the table."),
            AsyncInsertMany(
                [{"a": 2}, {"z": 0}],
                vectorize=[
                    "That is a fine spaghetti dish!",
                    "I am not debating the effectiveness of such approach...",
                ],
            ),
            AsyncUpdateOne(
                {},
                {"$set": {"b": 1}},
                vectorize="Oh, I love a nice bolognese pasta meal!",
            ),
            AsyncReplaceOne({}, {"a": 10}, vectorize="The kitty sits on the desk."),
            AsyncDeleteOne({}, vectorize="I don't argue with the proposed plan..."),
        ]
        await acol.bulk_write(bw_ops, ordered=True)
        found = [
            {k: v for k, v in doc.items() if k != "_id"}
            async for doc in acol.find({}, projection=["a", "b"])
        ]
        assert len(found) == 2
        assert {"a": 10} in found
        assert {"a": 2, "b": 1} in found

    # @pytest.mark.skipif(
    #     not is_nvidia_service_available(), reason="No 'service' on this database"
    # )
    @pytest.mark.describe(
        "test of database create_collection dimension-mismatch failure, async"
    )
    async def test_database_create_collection_dimension_mismatch_failure_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        with pytest.raises(DataAPIResponseException):
            await async_database.create_collection(
                "collection_name",
                dimension=123,
                service={"provider": "openai", "modelName": "text-embedding-ada-002"},
                # service={"provider": "nvidia", "modelName": "NV-Embed-QA"},
            )
