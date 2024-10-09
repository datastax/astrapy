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

from typing import Any

import pytest

from astrapy import Collection, Database
from astrapy.exceptions import DataAPIResponseException


class TestVectorizeMethodsSync:
    @pytest.mark.describe("test of vectorize in collection methods, sync")
    def test_collection_methods_vectorize_sync(
        self,
        sync_empty_service_collection: Collection,
        service_collection_parameters: dict[str, Any],
    ) -> None:
        col = sync_empty_service_collection
        service_vector_dimension = service_collection_parameters["dimension"]

        col.insert_one({"t": "tower", "$vectorize": "How high is this tower?"})
        col.insert_one({"t": "vectorless"})
        col.insert_one({"t": "vectorful", "$vector": [0.01] * service_vector_dimension})

        col.insert_many(
            [
                {
                    "t": "guide",
                    "$vectorize": "This is the instructions manual. Read it!",
                },
                {
                    "t": "seeds",
                    "$vectorize": "Other plants rely on wind to propagate their seeds.",
                },
            ],
        )
        with pytest.warns(DeprecationWarning):
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
            sort={"$vectorize": "This building is five storeys tall."},
            projection={"$vector": False},
        )
        assert doc is not None
        assert doc["t"] == "tower"

        docs = list(
            col.find(
                {},
                sort={"$vectorize": "This building is five storeys tall."},
                limit=2,
                projection={"$vector": False},
            )
        )
        assert docs[0]["t"] == "tower"

        rdoc = col.find_one_and_replace(
            {},
            {"t": "spider", "$vectorize": "Check out the eyes!"},
            sort={"$vectorize": "The disposition of the eyes tells much"},
            projection={"$vector": False},
        )
        assert rdoc is not None
        assert rdoc["t"] == "spider"

        r1res = col.replace_one(
            {},
            {"t": "spider", "$vectorize": "Look at how the eyes are placed"},
            sort={"$vectorize": "The disposition of the eyes tells much"},
        )
        assert r1res.update_info["nModified"] == 1

        udoc = col.find_one_and_update(
            {},
            {"$set": {"$vectorize": "Consider consulting the how-to"}},
            sort={"$vectorize": "Have a look at the user guide..."},
            projection={"$vector": False},
        )
        assert udoc is not None
        assert udoc["t"] == "guide"

        u1res = col.update_one(
            {},
            {"$set": {"$vectorize": "Know how to operate it before doing so."}},
            sort={"$vectorize": "Have a look at the user guide..."},
        )
        assert u1res.update_info["nModified"] == 1

        ddoc = col.find_one_and_delete(
            {},
            sort={"$vectorize": "Some trees have seeds that are dispersed in the air!"},
            projection={"$vector": False},
        )
        assert ddoc is not None
        assert ddoc["t"] == "seeds"

        d1res = col.delete_one(
            {},
            sort={"$vectorize": "yet another giant construction in this suburb."},
        )
        assert d1res.deleted_count == 1

    @pytest.mark.describe(
        "test of include_sort_vector in collection vectorize find, sync"
    )
    def test_collection_include_sort_vector_vectorize_find_sync(
        self,
        sync_empty_service_collection: Collection,
    ) -> None:
        # with empty collection
        q_text = "A sentence for searching."

        def _is_vector(v: Any) -> bool:
            return isinstance(v, list) and isinstance(v[0], float)

        for include_sv in [False, True]:
            for sort_cl_label in ["vze"]:
                sort_cl_e: dict[str, Any] = {"$vectorize": q_text}
                vec_expected = include_sv and sort_cl_label == "vze"
                # pristine iterator
                this_ite_1 = sync_empty_service_collection.find(
                    {}, sort=sort_cl_e, include_sort_vector=include_sv
                )
                if vec_expected:
                    assert _is_vector(this_ite_1.get_sort_vector())
                else:
                    assert this_ite_1.get_sort_vector() is None
                # after exhaustion with empty
                all_items_1 = list(this_ite_1)
                assert all_items_1 == []
                if vec_expected:
                    assert _is_vector(this_ite_1.get_sort_vector())
                else:
                    assert this_ite_1.get_sort_vector() is None
                # directly exhausted before calling get_sort_vector
                this_ite_2 = sync_empty_service_collection.find(
                    {}, sort=sort_cl_e, include_sort_vector=include_sv
                )
                all_items_2 = list(this_ite_2)
                assert all_items_2 == []
                if vec_expected:
                    assert _is_vector(this_ite_2.get_sort_vector())
                else:
                    assert this_ite_2.get_sort_vector() is None
        sync_empty_service_collection.insert_many(
            [
                {"seq": i, "$vectorize": f"This is sentence number {i}"}
                for i in range(10)
            ]
        )
        # with non-empty collection
        for include_sv in [False, True]:
            for sort_cl_label in ["vze"]:
                sort_cl_f: dict[str, Any] = {"$vectorize": q_text}
                vec_expected = include_sv and sort_cl_label == "vze"
                # pristine iterator
                this_ite_1 = sync_empty_service_collection.find(
                    {}, sort=sort_cl_f, include_sort_vector=include_sv
                )
                if vec_expected:
                    assert _is_vector(this_ite_1.get_sort_vector())
                else:
                    assert this_ite_1.get_sort_vector() is None
                # after consuming one item
                first_seqs = [
                    doc["seq"] for doc in [this_ite_1.__next__(), this_ite_1.__next__()]
                ]
                if vec_expected:
                    assert _is_vector(this_ite_1.get_sort_vector())
                else:
                    assert this_ite_1.get_sort_vector() is None
                # after exhaustion with the rest
                last_seqs = [doc["seq"] for doc in list(this_ite_1)]
                assert len(set(last_seqs + first_seqs)) == 10
                assert len(last_seqs + first_seqs) == 10
                if vec_expected:
                    assert _is_vector(this_ite_1.get_sort_vector())
                else:
                    assert this_ite_1.get_sort_vector() is None
                # directly exhausted before calling get_sort_vector
                this_ite_2 = sync_empty_service_collection.find(
                    {}, sort=sort_cl_f, include_sort_vector=include_sv
                )
                list(this_ite_2)
                if vec_expected:
                    assert _is_vector(this_ite_2.get_sort_vector())
                else:
                    assert this_ite_2.get_sort_vector() is None

    @pytest.mark.describe(
        "test of database create_collection dimension-mismatch failure, sync"
    )
    def test_database_create_collection_dimension_mismatch_failure_sync(
        self,
        sync_database: Database,
        service_collection_parameters: dict[str, Any],
    ) -> None:
        with pytest.raises(DataAPIResponseException):
            sync_database.create_collection(
                "collection_name",
                dimension=service_collection_parameters["dimension"] + 10,
                service={
                    "provider": service_collection_parameters["provider"],
                    "modelName": service_collection_parameters["modelName"],
                },
            )
