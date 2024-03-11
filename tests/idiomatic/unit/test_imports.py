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


@pytest.mark.describe("test imports")
def test_imports() -> None:
    from astrapy.results import (  # noqa: F401
        DeleteResult,
        InsertOneResult,
        InsertManyResult,
        UpdateResult,
        BulkWriteResult,
    )
    from astrapy.operations import (  # noqa: F401
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
    from astrapy.constants import (  # noqa: F401
        ReturnDocument,
        SortDocuments,
        VectorMetric,
    )
    from astrapy.info import (  # noqa: F401
        DatabaseInfo,
        CollectionInfo,
    )
    from astrapy.cursors import (  # noqa: F401
        BaseCursor,
        Cursor,
        AsyncCursor,
    )
    from astrapy import (  # noqa: F401
        Database,
        AsyncDatabase,
        Collection,
        AsyncCollection,
    )

    # The import pattern above for database and collection is to be preferred.
    from astrapy.database import (  # noqa: F401
        Database as Database2,
        AsyncDatabase as AsyncDatabase2,
    )
    from astrapy.collection import (  # noqa: F401
        Collection as Collection2,
        AsyncCollection as AsyncCollection2,
    )