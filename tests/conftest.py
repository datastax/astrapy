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

import functools
import os
import time
import warnings
from typing import Any, Awaitable, Callable, Optional, Tuple, TypedDict

import pytest
from deprecation import UnsupportedWarning
from testcontainers.compose import DockerCompose

from astrapy.admin import parse_api_endpoint
from astrapy.authentication import (
    StaticTokenProvider,
    TokenProvider,
    UsernamePasswordTokenProvider,
)
from astrapy.constants import Environment
from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME

from .preprocess_env import (
    ADMIN_ENV_LIST,
    ADMIN_ENV_VARIABLE_MAP,
    ASTRA_DB_API_ENDPOINT,
    ASTRA_DB_APPLICATION_TOKEN,
    ASTRA_DB_ID,
    ASTRA_DB_KEYSPACE,
    ASTRA_DB_OPS_APPLICATION_TOKEN,
    ASTRA_DB_REGION,
    DO_IDIOMATIC_ADMIN_TESTS,
    DOCKER_COMPOSE_LOCAL_DATA_API,
    IS_ASTRA_DB,
    LOCAL_DATA_API_APPLICATION_TOKEN,
    LOCAL_DATA_API_ENDPOINT,
    LOCAL_DATA_API_KEYSPACE,
    LOCAL_DATA_API_PASSWORD,
    LOCAL_DATA_API_USERNAME,
    SECONDARY_NAMESPACE,
    TEST_ASTRADBOPS,
    TEST_SKIP_COLLECTION_DELETE,
)

DOCKER_COMPOSE_SLEEP_TIME_SECONDS = 20

base_dir = os.path.abspath(os.path.dirname(__file__))
docker_compose_filepath = os.path.join(base_dir, "hcd_compose")


class DataAPICredentials(TypedDict):
    token: str | TokenProvider
    api_endpoint: str
    namespace: str


# to be used for 'core' testing, derived from above
class DataAPICoreCredentials(TypedDict):
    token: str
    api_endpoint: str
    namespace: str


class DataAPICredentialsInfo(TypedDict):
    environment: str
    region: str
    secondary_namespace: Optional[str]


def env_region_from_endpoint(api_endpoint: str) -> Tuple[str, str]:
    parsed = parse_api_endpoint(api_endpoint)
    if parsed is not None:
        return (parsed.environment, parsed.region)
    else:
        return (Environment.OTHER, "no-region")


def async_fail_if_not_removed(
    method: Callable[..., Awaitable[Any]]
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
                    (
                        "%s uses a function that should be removed: %s"
                        % (method, str(warning.message))
                    )
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
                    (
                        "%s uses a function that should be removed: %s"
                        % (method, str(warning.message))
                    )
                )
        return rv

    return test_inner


@pytest.fixture(scope="session")
def data_api_credentials_kwargs() -> DataAPICredentials:
    if IS_ASTRA_DB:
        if ASTRA_DB_API_ENDPOINT is None:
            raise ValueError("No endpoint data for local Data API")
        astra_db_creds: DataAPICredentials = {
            "token": StaticTokenProvider(ASTRA_DB_APPLICATION_TOKEN),
            "api_endpoint": ASTRA_DB_API_ENDPOINT or "",
            "namespace": ASTRA_DB_KEYSPACE or DEFAULT_KEYSPACE_NAME,
        }
        return astra_db_creds
    else:
        # if "DOCKER_COMPOSE_LOCAL_DATA_API", must spin the whole environment:
        # (it is started and then thrown away)
        if DOCKER_COMPOSE_LOCAL_DATA_API:
            compose = DockerCompose(filepath=docker_compose_filepath)
            compose.start()
            # and override some environment variables:
            time.sleep(DOCKER_COMPOSE_SLEEP_TIME_SECONDS)

        # either token or user/pwd pair (the latter having precedence)
        local_data_api_token_provider: TokenProvider
        if LOCAL_DATA_API_USERNAME and LOCAL_DATA_API_PASSWORD:
            local_data_api_token_provider = UsernamePasswordTokenProvider(
                username=LOCAL_DATA_API_USERNAME,
                password=LOCAL_DATA_API_PASSWORD,
            )
        elif LOCAL_DATA_API_APPLICATION_TOKEN:
            local_data_api_token_provider = StaticTokenProvider(
                LOCAL_DATA_API_APPLICATION_TOKEN
            )
        else:
            raise ValueError("No full authentication data for local Data API")
        if LOCAL_DATA_API_ENDPOINT is None:
            raise ValueError("No endpoint data for local Data API")
        local_db_creds: DataAPICredentials = {
            "token": local_data_api_token_provider,
            "api_endpoint": LOCAL_DATA_API_ENDPOINT or "",
            "namespace": LOCAL_DATA_API_KEYSPACE or DEFAULT_KEYSPACE_NAME,
        }
        return local_db_creds


@pytest.fixture(scope="session")
def data_api_core_credentials_kwargs(
    data_api_credentials_kwargs: DataAPICredentials,
) -> DataAPICoreCredentials:
    token_str: str
    if isinstance(data_api_credentials_kwargs["token"], str):
        token_str = data_api_credentials_kwargs["token"]
    elif isinstance(data_api_credentials_kwargs["token"], TokenProvider):
        token_str0 = data_api_credentials_kwargs["token"].get_token()
        if token_str0 is None:
            raise ValueError("Token cannot be made into a string in fixture")
        else:
            token_str = token_str0
    else:
        # this should not happen
        token_str = str(data_api_credentials_kwargs["token"])
    return {
        "token": token_str,
        "api_endpoint": data_api_credentials_kwargs["api_endpoint"],
        "namespace": data_api_credentials_kwargs["namespace"],
    }


@pytest.fixture(scope="session")
def data_api_credentials_info(
    data_api_credentials_kwargs: DataAPICredentials,
) -> DataAPICredentialsInfo:
    api_endpoint = data_api_credentials_kwargs["api_endpoint"]
    env, reg = env_region_from_endpoint(api_endpoint)

    astra_db_cred_info: DataAPICredentialsInfo = {
        "environment": env,
        "region": reg,
        "secondary_namespace": SECONDARY_NAMESPACE,
    }

    return astra_db_cred_info


@pytest.fixture(scope="session")
def data_api_core_bad_credentials_kwargs(
    data_api_core_credentials_kwargs: DataAPICoreCredentials,
) -> DataAPICoreCredentials:
    astra_db_creds: DataAPICoreCredentials = {
        "token": data_api_core_credentials_kwargs["token"],
        "namespace": data_api_core_credentials_kwargs["namespace"],
        "api_endpoint": "http://localhost:1234",
    }

    return astra_db_creds


__all__ = [
    "ASTRA_DB_API_ENDPOINT",
    "ASTRA_DB_APPLICATION_TOKEN",
    "ASTRA_DB_ID",
    "ASTRA_DB_KEYSPACE",
    "ASTRA_DB_OPS_APPLICATION_TOKEN",
    "ASTRA_DB_REGION",
    "DOCKER_COMPOSE_LOCAL_DATA_API",
    "IS_ASTRA_DB",
    "LOCAL_DATA_API_APPLICATION_TOKEN",
    "LOCAL_DATA_API_ENDPOINT",
    "LOCAL_DATA_API_KEYSPACE",
    "LOCAL_DATA_API_PASSWORD",
    "LOCAL_DATA_API_USERNAME",
    "SECONDARY_NAMESPACE",
    "TEST_ASTRADBOPS",
    "TEST_SKIP_COLLECTION_DELETE",
    "ADMIN_ENV_LIST",
    "ADMIN_ENV_VARIABLE_MAP",
    "DO_IDIOMATIC_ADMIN_TESTS",
]
