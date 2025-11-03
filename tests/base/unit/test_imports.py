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

# ruff: noqa: F401

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
    assert str(astrapy.data.collection) != ""
    assert str(astrapy.database.Database) != ""
    assert str(astrapy.exceptions.DevOpsAPIException) != ""
    assert str(astrapy.ids.uuid6) != ""
    assert str(astrapy.info.AstraDBDatabaseInfo) != ""
    assert str(astrapy.results.CollectionDeleteResult) != ""
    assert str(astrapy.settings.defaults) != ""
    assert str(astrapy.utils.request_tools) != ""


@pytest.mark.describe("test imports")
def test_imports() -> None:
    from astrapy import (
        AstraDBAdmin,
        AstraDBDatabaseAdmin,
        AsyncCollection,
        AsyncDatabase,
        AsyncTable,
        Collection,
        DataAPIClient,
        DataAPIDatabaseAdmin,
        Database,
        Table,
    )
    from astrapy.admin import (
        ParsedAPIEndpoint,
        parse_api_endpoint,
    )
    from astrapy.api_options import (
        APIOptions,
        DataAPIURLOptions,
        DevOpsAPIURLOptions,
        SerdesOptions,
        TimeoutOptions,
    )
    from astrapy.authentication import (
        AWSEmbeddingHeadersProvider,
        EmbeddingAPIKeyHeaderProvider,
        EmbeddingHeadersProvider,
        StaticTokenProvider,
        UsernamePasswordTokenProvider,
    )
    from astrapy.constants import (
        DefaultIdType,
        Environment,
        MapEncodingMode,
        ReturnDocument,
        SortMode,
        VectorMetric,
    )
    from astrapy.cursors import (
        AbstractCursor,
        AsyncCollectionFindAndRerankCursor,
        AsyncCollectionFindCursor,
        AsyncTableFindCursor,
        CollectionFindAndRerankCursor,
        CollectionFindCursor,
        CursorState,
        RerankedResult,
        TableFindCursor,
    )
    from astrapy.data_types import (
        DataAPIDate,
        DataAPIDictUDT,
        DataAPIDuration,
        DataAPIMap,
        DataAPISet,
        DataAPITime,
        DataAPITimestamp,
        DataAPIVector,
    )
    from astrapy.exceptions import (
        CollectionDeleteManyException,
        CollectionInsertManyException,
        CollectionUpdateManyException,
        CursorException,
        DataAPIErrorDescriptor,
        DataAPIException,
        DataAPIHttpException,
        DataAPIResponseException,
        DataAPITimeoutException,
        DevOpsAPIErrorDescriptor,
        DevOpsAPIException,
        DevOpsAPIHttpException,
        DevOpsAPIResponseException,
        DevOpsAPITimeoutException,
        MultiCallTimeoutManager,
        TableInsertManyException,
        TooManyDocumentsToCountException,
        TooManyRowsToCountException,
        UnexpectedDataAPIResponseException,
        UnexpectedDevOpsAPIResponseException,
    )
    from astrapy.event_observers import (
        event_collector,
        ObservableEventType,
        ObservableEvent,
        ObservableError,
        ObservableWarning,
        ObservableRequest,
        ObservableResponse,
        Observer,
    )
    from astrapy.ids import (
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
    from astrapy.info import (
        AlterTableAddColumns,
        AlterTableAddVectorize,
        AlterTableDropColumns,
        AlterTableDropVectorize,
        AlterTypeAddFields,
        AlterTypeOperation,
        AlterTypeRenameFields,
        AstraDBAdminDatabaseInfo,
        AstraDBDatabaseInfo,
        CollectionDefaultIDOptions,
        CollectionDefinition,
        CollectionDescriptor,
        CollectionInfo,
        CollectionLexicalOptions,
        CollectionRerankOptions,
        CollectionVectorOptions,
        ColumnType,
        CreateTableDefinition,
        CreateTypeDefinition,
        EmbeddingProvider,
        EmbeddingProviderAuthentication,
        EmbeddingProviderModel,
        EmbeddingProviderParameter,
        EmbeddingProviderToken,
        FindEmbeddingProvidersResult,
        FindRerankingProvidersResult,
        ListTableDefinition,
        ListTableDescriptor,
        ListTypeDescriptor,
        RerankingProvider,
        RerankingProviderAuthentication,
        RerankingProviderModel,
        RerankingProviderParameter,
        RerankingProviderToken,
        RerankServiceOptions,
        TableAPIIndexSupportDescriptor,
        TableAPISupportDescriptor,
        TableBaseIndexDefinition,
        TableIndexDefinition,
        TableIndexDescriptor,
        TableIndexOptions,
        TableInfo,
        TableKeyValuedColumnType,
        TableKeyValuedColumnTypeDescriptor,
        TablePrimaryKeyDescriptor,
        TableScalarColumnTypeDescriptor,
        TableTextIndexDefinition,
        TableTextIndexOptions,
        TableUDTColumnDescriptor,
        TableUnsupportedColumnTypeDescriptor,
        TableUnsupportedIndexDefinition,
        TableValuedColumnType,
        TableValuedColumnTypeDescriptor,
        TableVectorColumnTypeDescriptor,
        TableVectorIndexDefinition,
        TableVectorIndexOptions,
        VectorServiceOptions,
    )
    from astrapy.results import (
        CollectionDeleteResult,
        CollectionInsertManyResult,
        CollectionInsertOneResult,
        CollectionUpdateResult,
        OperationResult,
        TableInsertManyResult,
        TableInsertOneResult,
    )
    from astrapy.utils.api_options import defaultAPIOptions
    from astrapy.utils.document_paths import (
        escape_field_names,
        unescape_field_path,
    )
