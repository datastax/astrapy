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

from astrapy.data.info.collection_descriptor import (
    CollectionDefaultIDOptions,
    CollectionDefinition,
    CollectionDescriptor,
    CollectionInfo,
    CollectionLexicalOptions,
    CollectionRerankOptions,
    CollectionVectorOptions,
)
from astrapy.data.info.database_info import (
    AstraDBAdminDatabaseInfo,
    AstraDBDatabaseInfo,
)
from astrapy.data.info.reranking import (
    FindRerankingProvidersResult,
    RerankingProvider,
    RerankingProviderAuthentication,
    RerankingProviderModel,
    RerankingProviderParameter,
    RerankingProviderToken,
    RerankServiceOptions,
)
from astrapy.data.info.table_descriptor.table_altering import (
    AlterTableAddColumns,
    AlterTableAddVectorize,
    AlterTableDropColumns,
    AlterTableDropVectorize,
)
from astrapy.data.info.table_descriptor.table_columns import (
    TableAPISupportDescriptor,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableUnsupportedColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
)
from astrapy.data.info.table_descriptor.table_creation import (
    CreateTableDefinition,
)
from astrapy.data.info.table_descriptor.table_indexes import (
    TableAPIIndexSupportDescriptor,
    TableBaseIndexDefinition,
    TableIndexDefinition,
    TableIndexDescriptor,
    TableIndexOptions,
    TableIndexType,
    TableUnsupportedIndexDefinition,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
)
from astrapy.data.info.table_descriptor.table_listing import (
    ListTableDefinition,
    ListTableDescriptor,
    TableInfo,
)
from astrapy.data.info.vectorize import (
    EmbeddingProvider,
    EmbeddingProviderAuthentication,
    EmbeddingProviderModel,
    EmbeddingProviderParameter,
    EmbeddingProviderToken,
    FindEmbeddingProvidersResult,
    VectorServiceOptions,
)
from astrapy.data.utils.table_types import (
    ColumnType,
    TableKeyValuedColumnType,
    TableValuedColumnType,
)

__all__ = [
    "AlterTableAddColumns",
    "AlterTableAddVectorize",
    "AlterTableDropColumns",
    "AlterTableDropVectorize",
    "AstraDBAdminDatabaseInfo",
    "AstraDBDatabaseInfo",
    "CollectionDefaultIDOptions",
    "CollectionDefinition",
    "CollectionDescriptor",
    "CollectionInfo",
    "CollectionLexicalOptions",
    "CollectionRerankOptions",
    "CollectionVectorOptions",
    "ColumnType",
    "CreateTableDefinition",
    "EmbeddingProvider",
    "EmbeddingProviderAuthentication",
    "EmbeddingProviderModel",
    "EmbeddingProviderParameter",
    "EmbeddingProviderToken",
    "FindEmbeddingProvidersResult",
    "FindRerankingProvidersResult",
    "ListTableDefinition",
    "ListTableDescriptor",
    "RerankingProvider",
    "RerankingProviderAuthentication",
    "RerankingProviderModel",
    "RerankingProviderParameter",
    "RerankingProviderToken",
    "RerankServiceOptions",
    "TableAPIIndexSupportDescriptor",
    "TableAPISupportDescriptor",
    "TableBaseIndexDefinition",
    "TableIndexDefinition",
    "TableIndexDescriptor",
    "TableIndexOptions",
    "TableIndexType",
    "TableInfo",
    "TableKeyValuedColumnType",
    "TableKeyValuedColumnTypeDescriptor",
    "TablePrimaryKeyDescriptor",
    "TableScalarColumnTypeDescriptor",
    "TableUnsupportedColumnTypeDescriptor",
    "TableUnsupportedIndexDefinition",
    "TableValuedColumnType",
    "TableValuedColumnTypeDescriptor",
    "TableVectorColumnTypeDescriptor",
    "TableVectorIndexDefinition",
    "TableVectorIndexOptions",
    "VectorServiceOptions",
]
