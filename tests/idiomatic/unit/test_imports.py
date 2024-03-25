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
        OperationResult,
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
        DefaultIdType,
    )
    from astrapy.info import (  # noqa: F401
        AdminDatabaseInfo,
        DatabaseInfo,
        CollectionInfo,
    )
    from astrapy.admin import (  # noqa: F401
        Environment,
        ParsedAPIEndpoint,
    )
    from astrapy.cursors import (  # noqa: F401
        BaseCursor,
        Cursor,
        AsyncCursor,
        CommandCursor,
        AsyncCommandCursor,
    )
    from astrapy.exceptions import (  # noqa: F401
        DevOpsAPIException,
        DataAPIErrorDescriptor,
        DataAPIDetailedErrorDescriptor,
        DataAPIException,
        DataAPITimeoutException,
        CursorIsStartedException,
        CollectionNotFoundException,
        CollectionAlreadyExistsException,
        TooManyDocumentsToCountException,
        DataAPIFaultyResponseException,
        DataAPIResponseException,
        CumulativeOperationException,
        InsertManyException,
        DeleteManyException,
        UpdateManyException,
        BulkWriteException,
    )
    from astrapy.ids import (  # noqa: F401
        ObjectId,
        uuid1,
        uuid3,
        uuid4,
        uuid5,
        uuid6,
        uuid7,
        uuid8,
        UUID,
    )
    from astrapy.collection import (  # noqa: F401
        EmbeddingService,
    )
    from astrapy import (  # noqa: F401
        Database,
        AsyncDatabase,
        Collection,
        AsyncCollection,
        AstraDBAdmin,
        AstraDBDatabaseAdmin,
        DataAPIClient,
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
    from astrapy.admin import (  # noqa: F401
        AstraDBAdmin as AstraDBAdmin2,
        AstraDBDatabaseAdmin as AstraDBDatabaseAdmin2,
    )
    from astrapy.client import (  # noqa: F401
        DataAPIClient as DataAPIClient2,
    )
