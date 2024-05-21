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
from astrapy import Collection, Database
from astrapy.exceptions import DataAPIResponseException
from astrapy.operations import (
    InsertOne,
    InsertMany,
    UpdateOne,
    ReplaceOne,
    DeleteOne,
)


class TestVectorizeMethodsSync:
    # @pytest.mark.skipif(
    #     not is_nvidia_service_available(), reason="No 'service' on this database"
    # )
    @pytest.mark.describe("test of vectorize in collection methods, sync")
    def test_collection_methods_vectorize_sync(
        self,
        sync_empty_service_collection: Collection,
        service_vector_dimension: int,
    ) -> None:
        col = sync_empty_service_collection

        col.insert_one({"t": "tower"}, vectorize="How high is this tower?")
        col.insert_one({"t": "vectorless"})
        col.insert_one({"t": "vectorful"}, vector=[0.01] * service_vector_dimension)

        col.insert_many(
            [{"t": "guide"}, {"t": "seeds"}],
            vectorize=[
                "This is the instructions manual. Read it!",
                "Other plants rely on wind to propagate their seeds.",
            ],
        )
        col.insert_many(
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

        doc = col.find_one(
            {},
            vectorize="This building is five storeys tall.",
            projection={"$vector": False},
        )
        assert doc is not None
        assert doc["t"] == "tower"

        docs = list(
            col.find(
                {},
                vectorize="This building is five storeys tall.",
                limit=2,
                projection={"$vector": False},
            )
        )
        assert docs[0]["t"] == "tower"

        rdoc = col.find_one_and_replace(
            {},
            {"t": "spider", "$vectorize": "Check out the eyes!"},
            vectorize="The disposition of the eyes tells much",
            projection={"$vector": False},
        )
        assert rdoc["t"] == "spider"

        r1res = col.replace_one(
            {},
            {"t": "spider", "$vectorize": "Look at how the eyes are placed"},
            vectorize="The disposition of the eyes tells much",
        )
        assert r1res.update_info["nModified"] == 1

        udoc = col.find_one_and_update(
            {},
            {"$set": {"$vectorize": "Consider consulting the how-to"}},
            vectorize="Have a look at the user guide...",
            projection={"$vector": False},
        )
        assert udoc["t"] == "guide"

        u1res = col.update_one(
            {},
            {"$set": {"$vectorize": "Know how to operate it before doing so."}},
            vectorize="Have a look at the user guide...",
        )
        assert u1res.update_info["nModified"] == 1

        ddoc = col.find_one_and_delete(
            {},
            vectorize="Some trees have seeds that are dispersed in the air!",
            projection={"$vector": False},
        )
        assert ddoc["t"] == "seeds"

        d1res = col.delete_one(
            {},
            vectorize="yet another giant construction in this suburb.",
        )
        assert d1res.deleted_count == 1

    # @pytest.mark.skipif(
    #     not is_nvidia_service_available(), reason="No 'service' on this database"
    # )
    @pytest.mark.describe("test of bulk_write with vectorize, sync")
    def test_collection_bulk_write_vectorize_sync(
        self,
        sync_empty_service_collection: Collection,
    ) -> None:
        col = sync_empty_service_collection

        bw_ops = [
            InsertOne({"a": 1}, vectorize="The cat is on the table."),
            InsertMany(
                [{"a": 2}, {"z": 0}],
                vectorize=[
                    "That is a fine spaghetti dish!",
                    "I am not debating the effectiveness of such approach...",
                ],
            ),
            UpdateOne(
                {},
                {"$set": {"b": 1}},
                vectorize="Oh, I love a nice bolognese pasta meal!",
            ),
            ReplaceOne({}, {"a": 10}, vectorize="The kitty sits on the desk."),
            DeleteOne({}, vectorize="I don't argue with the proposed plan..."),
        ]
        col.bulk_write(bw_ops)
        found = [
            {k: v for k, v in doc.items() if k != "_id"}
            for doc in col.find({}, projection=["a", "b"])
        ]
        assert len(found) == 2
        assert {"a": 10} in found
        assert {"a": 2, "b": 1} in found

    # @pytest.mark.skipif(
    #     not is_nvidia_service_available(), reason="No 'service' on this database"
    # )
    @pytest.mark.describe(
        "test of database create_collection dimension-mismatch failure, sync"
    )
    def test_database_create_collection_dimension_mismatch_failure_sync(
        self,
        sync_database: Database,
    ) -> None:
        with pytest.raises(DataAPIResponseException):
            sync_database.create_collection(
                "collection_name",
                dimension=123,
                # service={"provider": "nvidia", "modelName": "NV-Embed-QA"},
                service={"provider": "openai", "modelName": "text-embedding-ada-002"},
            )
