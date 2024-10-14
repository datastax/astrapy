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

from dataclasses import dataclass
from typing import Sequence

from astrapy.authentication import (
    EmbeddingAPIKeyHeaderProvider,
    EmbeddingHeadersProvider,
    StaticTokenProvider,
    TokenProvider,
)
from astrapy.constants import Environment
from astrapy.settings.defaults import (
    API_PATH_ENV_MAP,
    API_VERSION_ENV_MAP,
    DEFAULT_DATA_METHOD_TIMEOUT_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    DEV_OPS_URL_ENV_MAP,
    DEV_OPS_VERSION_ENV_MAP,
)
from astrapy.utils.unset import _UNSET, UnsetType


@dataclass
class TimeoutOptions:
    request_timeout_ms: int | UnsetType = _UNSET
    data_method_timeout_ms: int | UnsetType = _UNSET
    schema_operation_timeout_ms: int | UnsetType = _UNSET
    database_admin_timeout_ms: int | UnsetType = _UNSET
    keyspace_admin_timeout_ms: int | UnsetType = _UNSET


@dataclass
class FullTimeoutOptions(TimeoutOptions):
    request_timeout_ms: int
    data_method_timeout_ms: int
    schema_operation_timeout_ms: int
    database_admin_timeout_ms: int
    keyspace_admin_timeout_ms: int

    def with_override(self, other: TimeoutOptions) -> FullTimeoutOptions:
        return FullTimeoutOptions(
            request_timeout_ms=(
                other.request_timeout_ms
                if not isinstance(other.request_timeout_ms, UnsetType)
                else self.request_timeout_ms
            ),
            data_method_timeout_ms=(
                other.data_method_timeout_ms
                if not isinstance(other.data_method_timeout_ms, UnsetType)
                else self.data_method_timeout_ms
            ),
            schema_operation_timeout_ms=(
                other.schema_operation_timeout_ms
                if not isinstance(other.schema_operation_timeout_ms, UnsetType)
                else self.schema_operation_timeout_ms
            ),
            database_admin_timeout_ms=(
                other.database_admin_timeout_ms
                if not isinstance(other.database_admin_timeout_ms, UnsetType)
                else self.database_admin_timeout_ms
            ),
            keyspace_admin_timeout_ms=(
                other.keyspace_admin_timeout_ms
                if not isinstance(other.keyspace_admin_timeout_ms, UnsetType)
                else self.keyspace_admin_timeout_ms
            ),
        )


@dataclass
class PayloadTransformOptions:
    binary_encode_vectors: bool | UnsetType = _UNSET


@dataclass
class FullPayloadTransformOptions(PayloadTransformOptions):
    binary_encode_vectors: bool

    def with_override(
        self, other: PayloadTransformOptions
    ) -> FullPayloadTransformOptions:
        return FullPayloadTransformOptions(
            binary_encode_vectors=(
                other.binary_encode_vectors
                if not isinstance(other.binary_encode_vectors, UnsetType)
                else self.binary_encode_vectors
            ),
        )


@dataclass
class DataAPIURLOptions:
    api_path: str | None | UnsetType = _UNSET
    api_version: str | None | UnsetType = _UNSET


@dataclass
class FullDataAPIURLOptions(DataAPIURLOptions):
    api_path: str | None
    api_version: str | None

    def with_override(self, other: DataAPIURLOptions) -> FullDataAPIURLOptions:
        return FullDataAPIURLOptions(
            api_path=(
                other.api_path
                if not isinstance(other.api_path, UnsetType)
                else self.api_path
            ),
            api_version=(
                other.api_version
                if not isinstance(other.api_version, UnsetType)
                else self.api_version
            ),
        )


@dataclass
class DevOpsAPIURLOptions:
    dev_ops_url: str | UnsetType = _UNSET
    dev_ops_api_version: str | None | UnsetType = _UNSET


@dataclass
class FullDevOpsAPIURLOptions(DevOpsAPIURLOptions):
    dev_ops_url: str
    dev_ops_api_version: str | None

    def with_override(self, other: DevOpsAPIURLOptions) -> FullDevOpsAPIURLOptions:
        return FullDevOpsAPIURLOptions(
            dev_ops_url=(
                other.dev_ops_url
                if not isinstance(other.dev_ops_url, UnsetType)
                else self.dev_ops_url
            ),
            dev_ops_api_version=(
                other.dev_ops_api_version
                if not isinstance(other.dev_ops_api_version, UnsetType)
                else self.dev_ops_api_version
            ),
        )


@dataclass
class APIOptions:
    environment: str | UnsetType = _UNSET
    callers: Sequence[tuple[str | None, str | None]] | UnsetType = _UNSET
    database_additional_headers: dict[str, str | None] | UnsetType = _UNSET
    admin_additional_headers: dict[str, str | None] | UnsetType = _UNSET
    token: TokenProvider | UnsetType = _UNSET
    embedding_api_key: EmbeddingHeadersProvider | UnsetType = _UNSET

    timeout_options: TimeoutOptions | UnsetType = _UNSET
    payload_transform_options: PayloadTransformOptions | UnsetType = _UNSET
    data_api_url_options: DataAPIURLOptions | UnsetType = _UNSET
    dev_ops_api_url_options: DevOpsAPIURLOptions | UnsetType = _UNSET


@dataclass
class FullAPIOptions(APIOptions):
    environment: str
    callers: Sequence[tuple[str | None, str | None]]
    database_additional_headers: dict[str, str | None]
    admin_additional_headers: dict[str, str | None]
    token: TokenProvider
    embedding_api_key: EmbeddingHeadersProvider

    timeout_options: FullTimeoutOptions
    payload_transform_options: FullPayloadTransformOptions
    data_api_url_options: FullDataAPIURLOptions
    dev_ops_api_url_options: FullDevOpsAPIURLOptions

    def with_override(self, other: APIOptions | None | UnsetType) -> FullAPIOptions:
        if isinstance(other, UnsetType) or other is None:
            return self

        database_additional_headers: dict[str, str | None]
        admin_additional_headers: dict[str, str | None]

        timeout_options: FullTimeoutOptions
        payload_transform_options: FullPayloadTransformOptions
        data_api_url_options: FullDataAPIURLOptions
        dev_ops_api_url_options: FullDevOpsAPIURLOptions

        if isinstance(other.database_additional_headers, UnsetType):
            database_additional_headers = self.database_additional_headers
        else:
            database_additional_headers = {
                **self.database_additional_headers,
                **other.database_additional_headers,
            }
        if isinstance(other.admin_additional_headers, UnsetType):
            admin_additional_headers = self.admin_additional_headers
        else:
            admin_additional_headers = {
                **self.admin_additional_headers,
                **other.admin_additional_headers,
            }

        if isinstance(other.timeout_options, FullTimeoutOptions):
            timeout_options = self.timeout_options.with_override(other.timeout_options)
        else:
            timeout_options = self.timeout_options
        if isinstance(other.payload_transform_options, FullPayloadTransformOptions):
            payload_transform_options = self.payload_transform_options.with_override(
                other.payload_transform_options
            )
        else:
            payload_transform_options = self.payload_transform_options
        if isinstance(other.data_api_url_options, FullDataAPIURLOptions):
            data_api_url_options = self.data_api_url_options.with_override(
                other.data_api_url_options
            )
        else:
            data_api_url_options = self.data_api_url_options
        if isinstance(other.dev_ops_api_url_options, FullDevOpsAPIURLOptions):
            dev_ops_api_url_options = self.dev_ops_api_url_options.with_override(
                other.dev_ops_api_url_options
            )
        else:
            dev_ops_api_url_options = self.dev_ops_api_url_options

        return FullAPIOptions(
            environment=(
                other.environment
                if not isinstance(other.environment, UnsetType)
                else self.environment
            ),
            callers=(
                other.callers
                if not isinstance(other.callers, UnsetType)
                else self.callers
            ),
            database_additional_headers=database_additional_headers,
            admin_additional_headers=admin_additional_headers,
            token=other.token if not isinstance(other.token, UnsetType) else self.token,
            embedding_api_key=(
                other.embedding_api_key
                if not isinstance(other.embedding_api_key, UnsetType)
                else self.embedding_api_key
            ),
            timeout_options=timeout_options,
            payload_transform_options=payload_transform_options,
            data_api_url_options=data_api_url_options,
            dev_ops_api_url_options=dev_ops_api_url_options,
        )


defaultTimeoutOptions = FullTimeoutOptions(
    request_timeout_ms=DEFAULT_REQUEST_TIMEOUT_MS,
    data_method_timeout_ms=DEFAULT_DATA_METHOD_TIMEOUT_MS,
    schema_operation_timeout_ms=45000,
    database_admin_timeout_ms=600000,
    keyspace_admin_timeout_ms=20000,
)
defaultPayloadTransformOptions = FullPayloadTransformOptions(
    binary_encode_vectors=False,
)


def defaultAPIOptions(environment: str) -> FullAPIOptions:
    defaultDataAPIURLOptions = FullDataAPIURLOptions(
        api_path=API_PATH_ENV_MAP[environment],
        api_version=API_VERSION_ENV_MAP[environment],
    )
    defaultDevOpsAPIURLOptions: FullDevOpsAPIURLOptions
    if environment in Environment.astra_db_values:
        defaultDevOpsAPIURLOptions = FullDevOpsAPIURLOptions(
            dev_ops_url=DEV_OPS_URL_ENV_MAP[environment],
            dev_ops_api_version=DEV_OPS_VERSION_ENV_MAP[environment],
        )
    else:
        defaultDevOpsAPIURLOptions = FullDevOpsAPIURLOptions(
            dev_ops_url="never used",
            dev_ops_api_version=None,
        )
    return FullAPIOptions(
        environment=environment,
        callers=[],
        database_additional_headers={},
        admin_additional_headers={},
        token=StaticTokenProvider(None),
        embedding_api_key=EmbeddingAPIKeyHeaderProvider(None),
        timeout_options=defaultTimeoutOptions,
        payload_transform_options=defaultPayloadTransformOptions,
        data_api_url_options=defaultDataAPIURLOptions,
        dev_ops_api_url_options=defaultDevOpsAPIURLOptions,
    )
