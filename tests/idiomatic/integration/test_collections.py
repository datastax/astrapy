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

from ..conftest import AstraDBCredentials
from astrapy import Collection, AsyncCollection


@pytest.mark.describe("test of instantiating Collection, sync")
def test_instantiate_collection_sync(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> None:
    col1 = Collection(
        collection_name="id_test_collection",
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    col2 = Collection(
        collection_name="id_test_collection",
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    assert col1 == col2


@pytest.mark.describe("test of instantiating Collection, async")
async def test_instantiate_collection_async(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> None:
    col1 = AsyncCollection(
        collection_name="id_test_collection",
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    col2 = AsyncCollection(
        collection_name="id_test_collection",
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    assert col1 == col2
