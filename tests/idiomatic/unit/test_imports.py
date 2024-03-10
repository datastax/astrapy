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


@pytest.mark.describe("test of idiomatic imports")
def test_imports() -> None:
    from astrapy import (  # noqa: F401
        AsyncCollection,
        AsyncDatabase,
        Collection,
        Database,
    )
    from astrapy.idiomatic.cursors import AsyncCursor, BaseCursor, Cursor  # noqa: F401
    from astrapy.idiomatic.info import CollectionInfo, DatabaseInfo  # noqa: F401
    from astrapy.idiomatic.operations import (  # noqa: F401
        BaseOperation,
        InsertOne,
        InsertMany,
        UpdateOne,
        UpdateMany,
        ReplaceOne,
        DeleteOne,
        DeleteMany,
        AsyncBaseOperation,
        AsyncInsertOne,
        AsyncInsertMany,
        AsyncUpdateOne,
        AsyncUpdateMany,
        AsyncReplaceOne,
        AsyncDeleteOne,
        AsyncDeleteMany,
    )
    from astrapy.idiomatic.results import (  # noqa: F401
        DeleteResult,
        InsertOneResult,
        InsertManyResult,
        UpdateResult,
        BulkWriteResult,
    )
    from astrapy.idiomatic.types import (  # noqa: F401
        ReturnDocument,
        SortDocuments,
        VectorMetric,
    )
