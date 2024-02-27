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

from astrapy import Collection, AsyncCollection


@pytest.mark.describe("test errors for unsupported Collection methods, sync")
def test_collection_unsupported_methods_sync() -> None:
    col = Collection(
        collection_name="mock",
        token="t",
        api_endpoint="a",
    )
    with pytest.raises(TypeError):
        col.find_raw_batches(1, "x")
    with pytest.raises(TypeError):
        col.aggregate(1, "x")
    with pytest.raises(TypeError):
        col.aggregate_raw_batches(1, "x")
    with pytest.raises(TypeError):
        col.watch(1, "x")
    with pytest.raises(TypeError):
        col.rename(1, "x")
    with pytest.raises(TypeError):
        col.create_index(1, "x")
    with pytest.raises(TypeError):
        col.create_indexes(1, "x")
    with pytest.raises(TypeError):
        col.drop_index(1, "x")
    with pytest.raises(TypeError):
        col.drop_indexes(1, "x")
    with pytest.raises(TypeError):
        col.list_indexes(1, "x")
    with pytest.raises(TypeError):
        col.index_information(1, "x")
    with pytest.raises(TypeError):
        col.create_search_index(1, "x")
    with pytest.raises(TypeError):
        col.create_search_indexes(1, "x")
    with pytest.raises(TypeError):
        col.drop_search_index(1, "x")
    with pytest.raises(TypeError):
        col.list_search_indexes(1, "x")
    with pytest.raises(TypeError):
        col.update_search_index(1, "x")
    with pytest.raises(TypeError):
        col.distinct(1, "x")


@pytest.mark.describe("test errors for unsupported Collection methods, async")
async def test_collection_unsupported_methods_async() -> None:
    col = AsyncCollection(
        collection_name="mock",
        token="t",
        api_endpoint="a",
    )
    with pytest.raises(TypeError):
        await col.find_raw_batches(1, "x")
    with pytest.raises(TypeError):
        await col.aggregate(1, "x")
    with pytest.raises(TypeError):
        await col.aggregate_raw_batches(1, "x")
    with pytest.raises(TypeError):
        await col.watch(1, "x")
    with pytest.raises(TypeError):
        await col.rename(1, "x")
    with pytest.raises(TypeError):
        await col.create_index(1, "x")
    with pytest.raises(TypeError):
        await col.create_indexes(1, "x")
    with pytest.raises(TypeError):
        await col.drop_index(1, "x")
    with pytest.raises(TypeError):
        await col.drop_indexes(1, "x")
    with pytest.raises(TypeError):
        await col.list_indexes(1, "x")
    with pytest.raises(TypeError):
        await col.index_information(1, "x")
    with pytest.raises(TypeError):
        await col.create_search_index(1, "x")
    with pytest.raises(TypeError):
        await col.create_search_indexes(1, "x")
    with pytest.raises(TypeError):
        await col.drop_search_index(1, "x")
    with pytest.raises(TypeError):
        await col.list_search_indexes(1, "x")
    with pytest.raises(TypeError):
        await col.update_search_index(1, "x")
    with pytest.raises(TypeError):
        await col.distinct(1, "x")
