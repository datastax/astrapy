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

from astrapy import Collection, Database
from astrapy.exceptions import (
    CollectionAlreadyExistsException,
    CollectionNotFoundException,
    DataAPIResponseException,
    InsertManyException,
    TooManyDocumentsToCountException,
)


class TestExceptionsSync:
    @pytest.mark.describe("test of collection insert_many failure modes, sync")
    def test_collection_insert_many_failures_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection
        bad_docs = [{"_id": tid} for tid in ["a", "b", "c", ValueError, "e", "f"]]
        dup_docs = [{"_id": tid} for tid in ["a", "b", "b", "d", "a", "b", "e", "f"]]
        ok_docs = [{"_id": tid} for tid in ["a", "b", "c", "d", "e", "f"]]

        with pytest.raises(ValueError):
            col.insert_many([], ordered=True, concurrency=2)

        with pytest.raises(TypeError):
            col.insert_many(bad_docs, ordered=True, chunk_size=2)

        with pytest.raises(TypeError):
            col.insert_many(bad_docs, ordered=False, chunk_size=2, concurrency=1)

        with pytest.raises(TypeError):
            col.insert_many(bad_docs, ordered=False, chunk_size=2, concurrency=2)

        col.delete_all()
        im_result1 = col.insert_many(ok_docs, ordered=True, chunk_size=2, concurrency=1)
        assert len(im_result1.inserted_ids) == 6
        assert len(list(col.find({}))) == 6

        col.delete_all()
        im_result2 = col.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=1
        )
        assert len(im_result2.inserted_ids) == 6
        assert len(list(col.find({}))) == 6

        col.delete_all()
        im_result3 = col.insert_many(
            ok_docs, ordered=False, chunk_size=2, concurrency=2
        )
        assert len(im_result3.inserted_ids) == 6
        assert len(list(col.find({}))) == 6

        col.delete_all()
        with pytest.raises(InsertManyException) as exc:
            col.insert_many(dup_docs, ordered=True, chunk_size=2, concurrency=1)
        assert len(exc.value.error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert exc.value.partial_result.inserted_ids == ["a", "b"]
        assert len(exc.value.partial_result.raw_results) == 2
        assert {doc["_id"] for doc in col.find()} == {"a", "b"}

        col.delete_all()
        with pytest.raises(InsertManyException) as exc:
            col.insert_many(dup_docs, ordered=False, chunk_size=2, concurrency=1)
        assert len(exc.value.error_descriptors) == 3
        assert len(exc.value.detailed_error_descriptors) == 2
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[1].error_descriptors) == 2
        assert set(exc.value.partial_result.inserted_ids) == {"a", "b", "d", "e", "f"}
        assert len(exc.value.partial_result.raw_results) == 4
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "d", "e", "f"}

        col.delete_all()
        with pytest.raises(InsertManyException) as exc:
            im_result3 = col.insert_many(
                dup_docs, ordered=False, chunk_size=2, concurrency=2
            )
        assert len(exc.value.error_descriptors) == 3
        assert len(exc.value.detailed_error_descriptors) == 2
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[1].error_descriptors) == 2
        assert set(exc.value.partial_result.inserted_ids) == {"a", "b", "d", "e", "f"}
        assert len(exc.value.partial_result.raw_results) == 4
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "d", "e", "f"}

    @pytest.mark.describe("test of collection insert_one failure modes, sync")
    def test_collection_insert_one_failures_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection
        with pytest.raises(TypeError):
            col.insert_one({"a": ValueError})

        col.insert_one({"_id": "a"})
        with pytest.raises(DataAPIResponseException) as exc:
            col.insert_one({"_id": "a"})
        assert len(exc.value.error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors) == 1
        assert len(exc.value.detailed_error_descriptors[0].error_descriptors) == 1
        assert isinstance(exc.value.detailed_error_descriptors[0].command, dict)

    @pytest.mark.describe("test of collection options failure modes, sync")
    def test_collection_options_failures_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection._copy()
        col._astra_db_collection.collection_name += "_hacked"
        col._astra_db_collection.base_path += "_hacked"
        with pytest.raises(CollectionNotFoundException) as exc:
            col.options()
        assert exc.value.collection_name == col._astra_db_collection.collection_name
        assert exc.value.namespace == sync_empty_collection.namespace

    @pytest.mark.describe("test of collection count_documents failure modes, sync")
    def test_collection_count_documents_failures_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection._copy()
        col._astra_db_collection.collection_name += "_hacked"
        col._astra_db_collection.base_path += "_hacked"
        with pytest.raises(DataAPIResponseException):
            col.count_documents({}, upper_bound=1)
        sync_empty_collection.insert_one({"a": 1})
        sync_empty_collection.insert_one({"b": 2})
        assert sync_empty_collection.count_documents({}, upper_bound=3) == 2
        with pytest.raises(TooManyDocumentsToCountException) as exc:
            sync_empty_collection.count_documents({}, upper_bound=1)
        assert not exc.value.server_max_count_exceeded

    @pytest.mark.describe("test of collection one-doc DML failure modes, sync")
    def test_collection_monodoc_dml_failures_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection._copy()
        col._astra_db_collection.collection_name += "_hacked"
        col._astra_db_collection.base_path += "_hacked"
        with pytest.raises(DataAPIResponseException):
            col.delete_all()
        with pytest.raises(DataAPIResponseException):
            col.delete_one({"a": 1})
        with pytest.raises(DataAPIResponseException):
            col.find_one_and_replace({"a": 1}, {"a": -1})
        with pytest.raises(DataAPIResponseException):
            col.find_one_and_update({"a": 1}, {"$set": {"a": -1}})
        with pytest.raises(DataAPIResponseException):
            col.find_one_and_delete({"a": 1})
        with pytest.raises(DataAPIResponseException):
            col.replace_one({"a": 1}, {"a": -1})
        with pytest.raises(DataAPIResponseException):
            col.update_one({"a": 1}, {"$set": {"a": -1}})

    @pytest.mark.describe("test of check_exists for create_collection, sync")
    def test_create_collection_check_exists_sync(
        self,
        sync_database: Database,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_check_exists"
        sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=3,
        )

        with pytest.raises(CollectionAlreadyExistsException):
            sync_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                dimension=3,
            )
        with pytest.raises(CollectionAlreadyExistsException):
            sync_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                indexing={"deny": ["a"]},
            )
        sync_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=3,
            check_exists=False,
        )
        with pytest.raises(DataAPIResponseException):
            sync_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                indexing={"deny": ["a"]},
                check_exists=False,
            )

        sync_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
