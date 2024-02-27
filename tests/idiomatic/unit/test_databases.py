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

from astrapy import Database, AsyncDatabase


@pytest.mark.describe("test errors for unsupported Database methods, sync")
def test_database_unsupported_methods_sync() -> None:
    db = Database(
        token="t",
        api_endpoint="a",
    )
    with pytest.raises(TypeError):
        db.aggregate(1, "x")
    with pytest.raises(TypeError):
        db.cursor_command(1, "x")
    with pytest.raises(TypeError):
        db.dereference(1, "x")
    with pytest.raises(TypeError):
        db.watch(1, "x")
    with pytest.raises(TypeError):
        db.validate_collection(1, "x")


@pytest.mark.describe("test errors for unsupported Database methods, async")
async def test_database_unsupported_methods_async() -> None:
    db = AsyncDatabase(
        token="t",
        api_endpoint="a",
    )
    with pytest.raises(TypeError):
        await db.aggregate(1, "x")
    with pytest.raises(TypeError):
        await db.cursor_command(1, "x")
    with pytest.raises(TypeError):
        await db.dereference(1, "x")
    with pytest.raises(TypeError):
        await db.watch(1, "x")
    with pytest.raises(TypeError):
        await db.validate_collection(1, "x")
