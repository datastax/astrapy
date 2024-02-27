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
from astrapy import Database, AsyncDatabase


@pytest.mark.describe("test of instantiating Database, sync")
def test_instantiate_database_sync(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> None:
    db1 = Database(
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    db2 = Database(
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    assert db1 == db2


@pytest.mark.describe("test of instantiating Database, async")
async def test_instantiate_database_async(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> None:
    db1 = AsyncDatabase(
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    db2 = AsyncDatabase(
        caller_name="c_n",
        caller_version="c_v",
        **astra_db_credentials_kwargs,
    )
    assert db1 == db2
