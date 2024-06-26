# main conftest for shared fixtures (if any).
import functools
import os
import warnings
from typing import Any, Awaitable, Callable, Optional, Tuple, TypedDict

import pytest
from deprecation import UnsupportedWarning

from astrapy.admin import parse_api_endpoint
from astrapy.authentication import (
    StaticTokenProvider,
    TokenProvider,
    UsernamePasswordTokenProvider,
)
from astrapy.constants import Environment
from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME

IS_ASTRA_DB: bool
SECONDARY_NAMESPACE: Optional[str]
if "LOCAL_DATA_API_ENDPOINT" in os.environ:
    IS_ASTRA_DB = False
    SECONDARY_NAMESPACE = os.environ.get("LOCAL_DATA_API_SECONDARY_KEYSPACE")
elif "ASTRA_DB_API_ENDPOINT" in os.environ:
    IS_ASTRA_DB = True
    SECONDARY_NAMESPACE = os.environ.get("ASTRA_DB_SECONDARY_KEYSPACE")
else:
    raise ValueError("No credentials.")


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
        ASTRA_DB_API_ENDPOINT = os.environ["ASTRA_DB_API_ENDPOINT"]
        ASTRA_DB_APPLICATION_TOKEN = StaticTokenProvider(
            os.environ["ASTRA_DB_APPLICATION_TOKEN"],
        )
        ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)
        astra_db_creds: DataAPICredentials = {
            "token": ASTRA_DB_APPLICATION_TOKEN,
            "api_endpoint": ASTRA_DB_API_ENDPOINT,
            "namespace": ASTRA_DB_KEYSPACE,
        }
        return astra_db_creds
    else:
        # either token or user/pwd pair (the latter having precedence)
        LOCAL_DATA_API_APPLICATION_TOKEN: TokenProvider
        if (
            "LOCAL_DATA_API_USERNAME" in os.environ
            and "LOCAL_DATA_API_PASSWORD" in os.environ
        ):
            LOCAL_DATA_API_APPLICATION_TOKEN = UsernamePasswordTokenProvider(
                username=os.environ["LOCAL_DATA_API_USERNAME"],
                password=os.environ["LOCAL_DATA_API_PASSWORD"],
            )
        elif "LOCAL_DATA_API_APPLICATION_TOKEN" in os.environ:
            LOCAL_DATA_API_APPLICATION_TOKEN = StaticTokenProvider(
                os.environ["LOCAL_DATA_API_APPLICATION_TOKEN"],
            )
        else:
            raise ValueError("Cannot find authentication data for local Data API")
        LOCAL_DATA_API_ENDPOINT = os.environ["LOCAL_DATA_API_ENDPOINT"]
        LOCAL_DATA_API_KEYSPACE = os.environ.get(
            "LOCAL_DATA_API_KEYSPACE", "default_keyspace"
        )
        local_db_creds: DataAPICredentials = {
            "token": LOCAL_DATA_API_APPLICATION_TOKEN,
            "api_endpoint": LOCAL_DATA_API_ENDPOINT,
            "namespace": LOCAL_DATA_API_KEYSPACE,
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
