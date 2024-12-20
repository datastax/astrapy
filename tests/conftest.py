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

"""
Main conftest for shared fixtures (if any).
"""

from __future__ import annotations

import functools
import warnings
from collections.abc import Iterator
from typing import Any, Awaitable, Callable, Iterable, TypedDict

import pytest
from blockbuster import BlockBuster, blockbuster_ctx
from deprecation import UnsupportedWarning

from astrapy import AsyncDatabase, DataAPIClient, Database
from astrapy.admin import parse_api_endpoint
from astrapy.authentication import TokenProvider
from astrapy.constants import Environment
from astrapy.settings.defaults import DEFAULT_ASTRA_DB_KEYSPACE

from .preprocess_env import (
    ADMIN_ENV_LIST,
    ADMIN_ENV_VARIABLE_MAP,
    ASTRA_DB_API_ENDPOINT,
    ASTRA_DB_APPLICATION_TOKEN,
    ASTRA_DB_KEYSPACE,
    ASTRA_DB_TOKEN_PROVIDER,
    DOCKER_COMPOSE_LOCAL_DATA_API,
    HEADER_EMBEDDING_API_KEY_OPENAI,
    IS_ASTRA_DB,
    LOCAL_DATA_API_APPLICATION_TOKEN,
    LOCAL_DATA_API_ENDPOINT,
    LOCAL_DATA_API_KEYSPACE,
    LOCAL_DATA_API_PASSWORD,
    LOCAL_DATA_API_TOKEN_PROVIDER,
    LOCAL_DATA_API_USERNAME,
    SECONDARY_KEYSPACE,
)


@pytest.fixture(autouse=True)
def blockbuster() -> Iterator[BlockBuster]:
    with blockbuster_ctx() as bb:
        # TODO: follow discussion in https://github.com/encode/httpx/discussions/3456
        bb.functions["os.stat"].can_block_in("httpx/_client.py", "_init_transport")
        yield bb


class DataAPICredentials(TypedDict):
    token: str | TokenProvider
    api_endpoint: str
    keyspace: str


class DataAPICredentialsInfo(TypedDict):
    environment: str
    region: str
    secondary_keyspace: str | None


def env_region_from_endpoint(api_endpoint: str) -> tuple[str, str]:
    parsed = parse_api_endpoint(api_endpoint)
    if parsed is not None:
        return (parsed.environment, parsed.region)
    else:
        return (Environment.OTHER, "no-region")


def async_fail_if_not_removed(
    method: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """
    Decorate a test async method to track removal of deprecated code.

    This is a customized+typed version of the deprecation package's
    `fail_if_not_removed` decorator (see), hastily put together to
    handle async test functions.

    See https://github.com/briancurtin/deprecation/issues/61 for reference.
    """

    @functools.wraps(method)
    async def test_inner(*args: Any, **kwargs: Any) -> Any:
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always")
            rv = await method(*args, **kwargs)

        for warning in caught_warnings:
            if warning.category == UnsupportedWarning:
                raise AssertionError(
                    f"{method} uses a function that should be removed: {str(warning.message)}"
                )
        return rv

    return test_inner


def sync_fail_if_not_removed(method: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorate a test sync method to track removal of deprecated code.

    This is a typed version of the deprecation package's
    `fail_if_not_removed` decorator (see), with added minimal typing.
    """

    @functools.wraps(method)
    def test_inner(*args: Any, **kwargs: Any) -> Any:
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always")
            rv = method(*args, **kwargs)

        for warning in caught_warnings:
            if warning.category == UnsupportedWarning:
                raise AssertionError(
                    f"{method} uses a function that should be removed: {str(warning.message)}"
                )
        return rv

    return test_inner


def clean_nulls_from_dict(in_dict: dict[str, Any]) -> dict[str, Any]:
    def _cleand(_in: Any) -> Any:
        if isinstance(_in, list):
            return [_cleand(itm) for itm in _in]
        elif isinstance(_in, dict):
            return {k: _cleand(v) for k, v in _in.items() if v is not None}
        else:
            return _in

    return _cleand(in_dict)  # type: ignore[no-any-return]


@pytest.fixture(scope="session")
def data_api_credentials_kwargs() -> DataAPICredentials:
    if IS_ASTRA_DB:
        if ASTRA_DB_API_ENDPOINT is None:
            raise ValueError("No endpoint data for local Data API")
        astra_db_creds: DataAPICredentials = {
            "token": ASTRA_DB_TOKEN_PROVIDER or "",
            "api_endpoint": ASTRA_DB_API_ENDPOINT or "",
            "keyspace": ASTRA_DB_KEYSPACE or DEFAULT_ASTRA_DB_KEYSPACE,
        }
        return astra_db_creds
    else:
        if LOCAL_DATA_API_ENDPOINT is None:
            raise ValueError("No endpoint data for local Data API")
        local_db_creds: DataAPICredentials = {
            "token": LOCAL_DATA_API_TOKEN_PROVIDER or "",
            "api_endpoint": LOCAL_DATA_API_ENDPOINT or "",
            "keyspace": LOCAL_DATA_API_KEYSPACE or DEFAULT_ASTRA_DB_KEYSPACE,
        }

        # ensure keyspace(s) exist at this point
        # (we have to bypass the fixture hierarchy as the ..._info fixture
        # comes later, so this part instantiates and uses throwaway objects)
        _env, _ = env_region_from_endpoint(local_db_creds["api_endpoint"])
        _client = DataAPIClient(environment=_env)
        _database = _client.get_database(
            local_db_creds["api_endpoint"],
            token=local_db_creds["token"],
        )
        _database_admin = _database.get_database_admin()
        _database_admin.create_keyspace(local_db_creds["keyspace"])
        if SECONDARY_KEYSPACE:
            _database_admin.create_keyspace(SECONDARY_KEYSPACE)
        # end of keyspace-ensuring block

        return local_db_creds


@pytest.fixture(scope="session")
def data_api_credentials_info(
    data_api_credentials_kwargs: DataAPICredentials,
) -> DataAPICredentialsInfo:
    api_endpoint = data_api_credentials_kwargs["api_endpoint"]
    env, reg = env_region_from_endpoint(api_endpoint)

    astra_db_cred_info: DataAPICredentialsInfo = {
        "environment": env,
        "region": reg,
        "secondary_keyspace": SECONDARY_KEYSPACE,
    }

    return astra_db_cred_info


@pytest.fixture(scope="session")
def client(
    data_api_credentials_info: DataAPICredentialsInfo,
) -> Iterable[DataAPIClient]:
    env = data_api_credentials_info["environment"]
    client = DataAPIClient(environment=env)
    yield client


@pytest.fixture(scope="session")
def sync_database(
    data_api_credentials_kwargs: DataAPICredentials,
    data_api_credentials_info: DataAPICredentialsInfo,
    client: DataAPIClient,
) -> Iterable[Database]:
    database = client.get_database(
        data_api_credentials_kwargs["api_endpoint"],
        token=data_api_credentials_kwargs["token"],
        keyspace=data_api_credentials_kwargs["keyspace"],
    )

    yield database


@pytest.fixture(scope="function")
def async_database(
    sync_database: Database,
) -> Iterable[AsyncDatabase]:
    yield sync_database.to_async()


__all__ = [
    "ASTRA_DB_API_ENDPOINT",
    "ASTRA_DB_APPLICATION_TOKEN",
    "ASTRA_DB_KEYSPACE",
    "DOCKER_COMPOSE_LOCAL_DATA_API",
    "HEADER_EMBEDDING_API_KEY_OPENAI",
    "IS_ASTRA_DB",
    "LOCAL_DATA_API_APPLICATION_TOKEN",
    "LOCAL_DATA_API_ENDPOINT",
    "LOCAL_DATA_API_KEYSPACE",
    "LOCAL_DATA_API_PASSWORD",
    "LOCAL_DATA_API_USERNAME",
    "SECONDARY_KEYSPACE",
    "ADMIN_ENV_LIST",
    "ADMIN_ENV_VARIABLE_MAP",
    "ASTRA_DB_TOKEN_PROVIDER",
    "LOCAL_DATA_API_TOKEN_PROVIDER",
]
