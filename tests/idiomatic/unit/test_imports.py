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

from __future__ import annotations

import pytest


@pytest.mark.describe("test namespace")
def test_namespace() -> None:
    import astrapy

    assert str(astrapy.admin) != ""
    assert str(astrapy.api_options) != ""
    assert str(astrapy.authentication) != ""
    assert str(astrapy.client) != ""
    assert str(astrapy.collection) != ""
    assert str(astrapy.constants) != ""
    assert str(astrapy.cursors) != ""
    assert str(astrapy.data) != ""
    assert str(astrapy.database) != ""
    assert str(astrapy.exceptions) != ""
    assert str(astrapy.ids) != ""
    assert str(astrapy.info) != ""
    assert str(astrapy.results) != ""
    assert str(astrapy.settings) != ""
    assert str(astrapy.utils) != ""

    assert str(astrapy.admin.AstraDBAdmin) != ""
    assert str(astrapy.api_options.APIOptions) != ""
    assert str(astrapy.authentication.TokenProvider) != ""
    assert str(astrapy.client.DataAPIClient) != ""
    assert str(astrapy.collection.Collection) != ""
    assert str(astrapy.constants.VectorMetric.DOT_PRODUCT) != ""
    assert str(astrapy.cursors.Cursor) != ""
    assert str(astrapy.data.collection) != ""
    assert str(astrapy.database.Database) != ""
    assert str(astrapy.exceptions.DevOpsAPIException) != ""
    assert str(astrapy.ids.uuid6) != ""
    assert str(astrapy.info.DatabaseInfo) != ""
    assert str(astrapy.results.DeleteResult) != ""
    assert str(astrapy.settings.defaults) != ""
    assert str(astrapy.utils.request_tools) != ""


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
    from astrapy.admin import (  # noqa: F401
        AstraDBAdmin as AstraDBAdmin2,
    )
    from astrapy.api_options import (  # noqa: F401
        APIOptions,
        DataAPIURLOptions,
        DevOpsAPIURLOptions,
        PayloadTransformOptions,
        TimeoutOptions,
    )
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
        CommandCursor,
        Cursor,
        CursorState,
    )
    from astrapy.exceptions import (  # noqa: F401
        CollectionAlreadyExistsException,
        CollectionNotFoundException,
        CumulativeOperationException,
        CursorIsStartedException,
        DataAPIDetailedErrorDescriptor,
        DataAPIErrorDescriptor,
        DataAPIException,
        DataAPIFaultyResponseException,
        DataAPIHttpException,
        DataAPIResponseException,
        DataAPITimeoutException,
        DeleteManyException,
        DevOpsAPIErrorDescriptor,
        DevOpsAPIException,
        DevOpsAPIFaultyResponseException,
        DevOpsAPIHttpException,
        DevOpsAPIResponseException,
        DevOpsAPITimeoutException,
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
        DatabaseInfo,
        EmbeddingProvider,
        EmbeddingProviderAuthentication,
        EmbeddingProviderModel,
        EmbeddingProviderParameter,
        EmbeddingProviderToken,
        FindEmbeddingProvidersResult,
        VectorServiceOptions,
    )
    from astrapy.results import (  # noqa: F401
        DeleteResult,
        InsertManyResult,
        InsertOneResult,
        OperationResult,
        UpdateResult,
    )
