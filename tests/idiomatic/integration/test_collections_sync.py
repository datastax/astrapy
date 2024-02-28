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


class TestCollectionsSync:
    @pytest.mark.describe("test of instantiating Collection, sync")
    def test_instantiate_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        col2 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 == col2

    @pytest.mark.describe("test of Collection conversions, sync")
    def test_convert_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 == col1.copy()
        assert col1 == col1.to_async().to_sync()

    @pytest.mark.describe("test of Collection set_caller, sync")
    def test_collection_set_caller_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n1",
            caller_version="c_v1",
        )
        col2 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n2",
            caller_version="c_v2",
        )
        col2.set_caller(
            caller_name="c_n1",
            caller_version="c_v1",
        )
        assert col1 == col2

    @pytest.mark.describe("test errors for unsupported Collection methods, sync")
    def test_collection_unsupported_methods_sync(
        self,
        sync_collection: Collection,
    ) -> None:
        with pytest.raises(TypeError):
            sync_collection.find_raw_batches(1, "x")
        with pytest.raises(TypeError):
            sync_collection.aggregate(1, "x")
        with pytest.raises(TypeError):
            sync_collection.aggregate_raw_batches(1, "x")
        with pytest.raises(TypeError):
            sync_collection.watch(1, "x")
        with pytest.raises(TypeError):
            sync_collection.rename(1, "x")
        with pytest.raises(TypeError):
            sync_collection.create_index(1, "x")
        with pytest.raises(TypeError):
            sync_collection.create_indexes(1, "x")
        with pytest.raises(TypeError):
            sync_collection.drop_index(1, "x")
        with pytest.raises(TypeError):
            sync_collection.drop_indexes(1, "x")
        with pytest.raises(TypeError):
            sync_collection.list_indexes(1, "x")
        with pytest.raises(TypeError):
            sync_collection.index_information(1, "x")
        with pytest.raises(TypeError):
            sync_collection.create_search_index(1, "x")
        with pytest.raises(TypeError):
            sync_collection.create_search_indexes(1, "x")
        with pytest.raises(TypeError):
            sync_collection.drop_search_index(1, "x")
        with pytest.raises(TypeError):
            sync_collection.list_search_indexes(1, "x")
        with pytest.raises(TypeError):
            sync_collection.update_search_index(1, "x")
        with pytest.raises(TypeError):
            sync_collection.distinct(1, "x")
