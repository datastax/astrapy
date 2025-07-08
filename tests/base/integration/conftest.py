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

from ..conftest import (
    ADMIN_ENV_LIST,
    ADMIN_ENV_VARIABLE_MAP,
    CQL_AVAILABLE,
    HEADER_EMBEDDING_API_KEY_OPENAI,
    IS_ASTRA_DB,
    SECONDARY_KEYSPACE,
    TEST_COLLECTION_NAME,
    VECTORIZE_TEXTS,
    DataAPICredentials,
    DataAPICredentialsInfo,
    DefaultAsyncCollection,
    DefaultAsyncTable,
    DefaultCollection,
    DefaultTable,
    _repaint_NaNs,
    _typify_tuple,
    async_fail_if_not_removed,
    clean_nulls_from_dict,
    sync_fail_if_not_removed,
)
from ..table_udt_assets import (
    EXTENDED_PLAYER_TYPE_NAME,
    PLAYER_TYPE_NAME,
    THE_BYTES,
    THE_DATETIME,
    THE_TIMESTAMP,
    THE_TIMEZONE,
    ExtendedPlayer,
    NullablePlayer,
    Player,
    _extended_player_from_dict,
    _extended_player_serializer,
    _nullable_player_from_dict,
    _nullable_player_serializer,
    _player_from_dict,
    _player_serializer,
    dict_equal_same_class,
)

__all__ = [
    "DataAPICredentials",
    "DataAPICredentialsInfo",
    "async_fail_if_not_removed",
    "clean_nulls_from_dict",
    "sync_fail_if_not_removed",
    "HEADER_EMBEDDING_API_KEY_OPENAI",
    "IS_ASTRA_DB",
    "ADMIN_ENV_LIST",
    "ADMIN_ENV_VARIABLE_MAP",
    "CQL_AVAILABLE",
    "EXTENDED_PLAYER_TYPE_NAME",
    "PLAYER_TYPE_NAME",
    "SECONDARY_KEYSPACE",
    "TEST_COLLECTION_NAME",
    "THE_BYTES",
    "THE_DATETIME",
    "THE_TIMEZONE",
    "THE_TIMESTAMP",
    "VECTORIZE_TEXTS",
    "dict_equal_same_class",
    "_extended_player_from_dict",
    "_extended_player_serializer",
    "_nullable_player_from_dict",
    "_nullable_player_serializer",
    "_player_from_dict",
    "_player_serializer",
    "_repaint_NaNs",
    "_typify_tuple",
    "DefaultCollection",
    "DefaultAsyncCollection",
    "DefaultAsyncTable",
    "DefaultTable",
    "ExtendedPlayer",
    "NullablePlayer",
    "Player",
]
