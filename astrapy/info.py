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
    CollectionVectorOptions,
)
from astrapy.data.info.database_info import (
    AstraDBAdminDatabaseInfo,
    AstraDBDatabaseInfo,
)
from astrapy.data.info.table_descriptor import (
    AlterTableAddColumns,
    AlterTableAddVectorize,
    AlterTableDropColumns,
    AlterTableDropVectorize,
    CreateTableDefinition,
    CreateTableDescriptor,
    ListTableDefinition,
    ListTableDescriptor,
    TableAPIIndexSupportDescriptor,
    TableAPISupportDescriptor,
    TableBaseIndexDefinition,
    TableIndexDefinition,
    TableIndexDescriptor,
    TableIndexOptions,
    TableInfo,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableUnsupportedColumnTypeDescriptor,
    TableUnsupportedIndexDefinition,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
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
    TableKeyValuedColumnType,
    TableScalarColumnType,
    TableValuedColumnType,
)

__all__ = [
    "AlterTableAddColumns",
    "AlterTableAddVectorize",
    "AlterTableDropColumns",
    "AlterTableDropVectorize",
    "AstraDBAdminDatabaseInfo",
    "CollectionDefaultIDOptions",
    "CollectionDefinition",
    "CollectionDescriptor",
    "CollectionInfo",
    "CollectionVectorOptions",
    "CreateTableDefinition",
    "CreateTableDescriptor",
    "VectorServiceOptions",
    "AstraDBDatabaseInfo",
    "EmbeddingProviderParameter",
    "EmbeddingProviderModel",
    "EmbeddingProviderToken",
    "EmbeddingProviderAuthentication",
    "EmbeddingProvider",
    "FindEmbeddingProvidersResult",
    "ListTableDefinition",
    "ListTableDescriptor",
    "TableAPIIndexSupportDescriptor",
    "TableAPISupportDescriptor",
    "TableBaseIndexDefinition",
    "TableIndexDefinition",
    "TableIndexDescriptor",
    "TableIndexOptions",
    "TableInfo",
    "TableKeyValuedColumnTypeDescriptor",
    "TablePrimaryKeyDescriptor",
    "TableScalarColumnType",
    "TableValuedColumnType",
    "TableKeyValuedColumnType",
    "TableScalarColumnTypeDescriptor",
    "TableUnsupportedColumnTypeDescriptor",
    "TableUnsupportedIndexDefinition",
    "TableValuedColumnTypeDescriptor",
    "TableVectorColumnTypeDescriptor",
    "TableVectorIndexDefinition",
    "TableVectorIndexOptions",
]
