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
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, TypedDict

import pytest
from blockbuster import BlockBuster, blockbuster_ctx
from deprecation import UnsupportedWarning

if TYPE_CHECKING:
    from cassandra.cluster import Session
    from cassio.config import get_session_and_keyspace

import astrapy
from astrapy import AsyncDatabase, DataAPIClient, Database
from astrapy.admin import parse_api_endpoint
from astrapy.authentication import TokenProvider
from astrapy.constants import Environment
from astrapy.settings.defaults import DEFAULT_ASTRA_DB_KEYSPACE, DEV_OPS_URL_ENV_MAP

from .preprocess_env import (
    ADMIN_ENV_LIST,
    ADMIN_ENV_VARIABLE_MAP,
    ASTRA_DB_API_ENDPOINT,
    ASTRA_DB_APPLICATION_TOKEN,
    ASTRA_DB_KEYSPACE,
    ASTRA_DB_TOKEN_PROVIDER,
    DOCKER_COMPOSE_LOCAL_DATA_API,
    HEADER_EMBEDDING_API_KEY_OPENAI,
    HEADER_RERANKING_API_KEY_NVIDIA,
    IS_ASTRA_DB,
    LOCAL_CASSANDRA_CONTACT_POINT,
    LOCAL_CASSANDRA_PORT,
    LOCAL_DATA_API_ENDPOINT,
    LOCAL_DATA_API_KEYSPACE,
    LOCAL_DATA_API_PASSWORD,
    LOCAL_DATA_API_TOKEN_PROVIDER,
    LOCAL_DATA_API_USERNAME,
    RUN_SHARED_SECRET_VECTORIZE_TESTS,
    SECONDARY_KEYSPACE,
    USE_RERANKER_API_KEY_HEADER,
)

CQL_AVAILABLE = False
try:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster, Session
    from cassio.config import get_session_and_keyspace

    CQL_AVAILABLE = True
except ImportError:
    pass


@pytest.fixture(autouse=True)
def blockbuster() -> Iterator[BlockBuster]:
    with blockbuster_ctx("astrapy") as bb:
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
    secondary_keyspace: str


def env_region_from_endpoint(api_endpoint: str) -> tuple[str, str]:
    parsed = parse_api_endpoint(api_endpoint)
    if parsed is not None:
        return (parsed.environment, parsed.region)
    else:
        return (Environment.OTHER, "no-region")


def database_id_from_endpoint(api_endpoint: str) -> str | None:
    parsed = parse_api_endpoint(api_endpoint)
    if parsed is not None:
        return parsed.database_id
    else:
        return None


def is_future_version(v_string: str) -> bool:
    def _my_int(st: str) -> int:
        rpos = st.find("rc")
        if rpos >= 0:
            return int(st[:rpos])
        return int(st)

    current_tuple = tuple(_my_int(pc) for pc in astrapy.__version__.split("."))
    v_tuple = tuple(_my_int(pc) for pc in v_string.split("."))
    return v_tuple > current_tuple


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
    api_creds: DataAPICredentials
    if IS_ASTRA_DB:
        if ASTRA_DB_API_ENDPOINT is None:
            raise ValueError("No endpoint data for local Data API")
        api_creds = {
            "token": ASTRA_DB_TOKEN_PROVIDER or "",
            "api_endpoint": ASTRA_DB_API_ENDPOINT or "",
            "keyspace": ASTRA_DB_KEYSPACE or DEFAULT_ASTRA_DB_KEYSPACE,
        }
    else:
        if LOCAL_DATA_API_ENDPOINT is None:
            raise ValueError("No endpoint data for local Data API")
        api_creds = {
            "token": LOCAL_DATA_API_TOKEN_PROVIDER or "",
            "api_endpoint": LOCAL_DATA_API_ENDPOINT or "",
            "keyspace": LOCAL_DATA_API_KEYSPACE or DEFAULT_ASTRA_DB_KEYSPACE,
        }

    # ensure keyspace(s) exist at this point
    # (we have to bypass the fixture hierarchy as the ..._info fixture
    # comes later, so this part instantiates and uses throwaway objects)
    _env, _ = env_region_from_endpoint(api_creds["api_endpoint"])
    _client = DataAPIClient(environment=_env)
    _database = _client.get_database(
        api_creds["api_endpoint"],
        token=api_creds["token"],
    )
    _database_admin = _database.get_database_admin()
    found_keyspaces = _database_admin.list_keyspaces()
    # This is an ugly way to reduce the risk of collision from concurrent
    # unit tests in CI/CD (which shows as "409 Conflict")
    if api_creds["keyspace"] not in found_keyspaces:
        _database_admin.create_keyspace(api_creds["keyspace"])
    found_keyspaces_2 = _database_admin.list_keyspaces()
    if SECONDARY_KEYSPACE not in found_keyspaces_2:
        _database_admin.create_keyspace(SECONDARY_KEYSPACE)
    # end of keyspace-ensuring block

    return api_creds


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


@pytest.fixture(scope="session")
def cql_session(
    data_api_credentials_kwargs: DataAPICredentials,
) -> Iterable[Session]:
    if IS_ASTRA_DB:
        _env, _ = env_region_from_endpoint(data_api_credentials_kwargs["api_endpoint"])
        _db_id = database_id_from_endpoint(data_api_credentials_kwargs["api_endpoint"])

        if _db_id is None:
            raise ValueError("Could not extract database id for cql_session")

        _bundle_template = (
            f"{DEV_OPS_URL_ENV_MAP[_env]}/v2/databases/{{database_id}}/secureBundleURL"
        )

        _token: str
        if isinstance(data_api_credentials_kwargs["token"], TokenProvider):
            _token = data_api_credentials_kwargs["token"].get_token() or ""
        else:
            _token = data_api_credentials_kwargs["token"]

        if _token == "":
            raise ValueError("Token not found for cql_session")

        session, _ = get_session_and_keyspace(
            token=_token,
            database_id=_db_id,
            bundle_url_template=_bundle_template,
        )

        if session is None:
            raise ValueError("No CQL 'Session' was obtained")
        session.execute(f"USE {data_api_credentials_kwargs['keyspace']};")
        yield session
    else:
        if LOCAL_CASSANDRA_CONTACT_POINT is None:
            raise ValueError("No Cassandra contact point defined")

        auth_provider = PlainTextAuthProvider(
            username=LOCAL_DATA_API_USERNAME,
            password=LOCAL_DATA_API_PASSWORD,
        )
        additional_kwargs = {
            argk: argv
            for argk, argv in {
                "port": LOCAL_CASSANDRA_PORT,
            }.items()
            if argv is not None
        }
        cluster = Cluster(
            contact_points=[LOCAL_CASSANDRA_CONTACT_POINT],
            auth_provider=auth_provider,
            **additional_kwargs,
        )
        session = cluster.connect()
        session.execute(f"USE {data_api_credentials_kwargs['keyspace']};")
        yield session


__all__ = [
    "ASTRA_DB_API_ENDPOINT",
    "ASTRA_DB_APPLICATION_TOKEN",
    "ASTRA_DB_KEYSPACE",
    "CQL_AVAILABLE",
    "DOCKER_COMPOSE_LOCAL_DATA_API",
    "HEADER_EMBEDDING_API_KEY_OPENAI",
    "HEADER_RERANKING_API_KEY_NVIDIA",
    "IS_ASTRA_DB",
    "LOCAL_DATA_API_ENDPOINT",
    "LOCAL_DATA_API_KEYSPACE",
    "SECONDARY_KEYSPACE",
    "ADMIN_ENV_LIST",
    "ADMIN_ENV_VARIABLE_MAP",
    "ASTRA_DB_TOKEN_PROVIDER",
    "LOCAL_DATA_API_TOKEN_PROVIDER",
    "RUN_SHARED_SECRET_VECTORIZE_TESTS",
    "USE_RERANKER_API_KEY_HEADER",
]
