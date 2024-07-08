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


@pytest.mark.describe("test namespace")
def test_namespace() -> None:
    import astrapy

    assert str(astrapy.admin) != ""
    assert str(astrapy.client) != ""
    assert str(astrapy.collection) != ""
    assert str(astrapy.constants) != ""
    assert str(astrapy.cursors) != ""
    assert str(astrapy.database) != ""
    assert str(astrapy.exceptions) != ""
    assert str(astrapy.ids) != ""
    assert str(astrapy.info) != ""
    assert str(astrapy.operations) != ""
    assert str(astrapy.results) != ""

    assert str(astrapy.admin.AstraDBAdmin) != ""
    assert str(astrapy.client.DataAPIClient) != ""
    assert str(astrapy.collection.Collection) != ""
    assert str(astrapy.constants.VectorMetric.DOT_PRODUCT) != ""
    assert str(astrapy.cursors.BaseCursor) != ""
    assert str(astrapy.database.Database) != ""
    assert str(astrapy.exceptions.DevOpsAPIException) != ""
    assert str(astrapy.ids.uuid6) != ""
    assert str(astrapy.info.DatabaseInfo) != ""
    assert str(astrapy.operations.InsertMany) != ""
    assert str(astrapy.results.DeleteResult) != ""


@pytest.mark.describe("test imports")
def test_imports() -> None:
    from astrapy import (  # noqa: F401
        AstraDBAdmin,
        AstraDBDatabaseAdmin,
        AsyncCollection,
        AsyncDatabase,
        Collection,
        DataAPIClient,
        DataAPIDatabaseAdmin,
        Database,
    )
    from astrapy.admin import AstraDBAdmin as AstraDBAdmin2  # noqa: F401
    from astrapy.admin import (  # noqa: F401
        AstraDBDatabaseAdmin as AstraDBDatabaseAdmin2,
    )
    from astrapy.admin import (  # noqa: F401
        DataAPIDatabaseAdmin as DataAPIDatabaseAdmin2,
    )
    from astrapy.admin import ParsedAPIEndpoint  # noqa: F401
    from astrapy.client import DataAPIClient as DataAPIClient2  # noqa: F401
    from astrapy.collection import AsyncCollection as AsyncCollection2  # noqa: F401
    from astrapy.collection import Collection as Collection2  # noqa: F401
    from astrapy.constants import (  # noqa: F401
        DefaultIdType,
        Environment,
        ReturnDocument,
        SortDocuments,
        VectorMetric,
    )
    from astrapy.cursors import (  # noqa: F401
        AsyncCommandCursor,
        AsyncCursor,
        BaseCursor,
        CommandCursor,
        Cursor,
    )

    # The import pattern above for database and collection is to be preferred.
    from astrapy.database import AsyncDatabase as AsyncDatabase2  # noqa: F401
    from astrapy.database import Database as Database2  # noqa: F401
    from astrapy.exceptions import (  # noqa: F401
        BulkWriteException,
        CollectionAlreadyExistsException,
        CollectionNotFoundException,
        CumulativeOperationException,
        CursorIsStartedException,
        DataAPIDetailedErrorDescriptor,
        DataAPIErrorDescriptor,
        DataAPIException,
        DataAPIFaultyResponseException,
        DataAPIResponseException,
        DataAPITimeoutException,
        DeleteManyException,
        DevOpsAPIErrorDescriptor,
        DevOpsAPIException,
        DevOpsAPIResponseException,
        InsertManyException,
        TooManyDocumentsToCountException,
        UpdateManyException,
    )
    from astrapy.ids import (  # noqa: F401
        UUID,
        ObjectId,
        uuid1,
        uuid3,
        uuid4,
        uuid5,
        uuid6,
        uuid7,
        uuid8,
    )
    from astrapy.info import (  # noqa: F401
        AdminDatabaseInfo,
        CollectionDefaultIDOptions,
        CollectionDescriptor,
        CollectionInfo,
        CollectionOptions,
        CollectionVectorOptions,
        CollectionVectorServiceOptions,
        DatabaseInfo,
        EmbeddingProvider,
        EmbeddingProviderAuthentication,
        EmbeddingProviderModel,
        EmbeddingProviderParameter,
        EmbeddingProviderToken,
    )
    from astrapy.operations import (  # noqa: F401
        AsyncBaseOperation,
        AsyncDeleteMany,
        AsyncDeleteOne,
        AsyncInsertMany,
        AsyncInsertOne,
        AsyncReplaceOne,
        AsyncUpdateMany,
        AsyncUpdateOne,
        BaseOperation,
        DeleteMany,
        DeleteOne,
        InsertMany,
        InsertOne,
        ReplaceOne,
        UpdateMany,
        UpdateOne,
    )
    from astrapy.results import (  # noqa: F401
        BulkWriteResult,
        DeleteResult,
        InsertManyResult,
        InsertOneResult,
        OperationResult,
        UpdateResult,
    )
