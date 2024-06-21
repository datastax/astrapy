# main conftest for shared fixtures (if any).
import functools
import os
import pytest
import warnings
from deprecation import UnsupportedWarning
from typing import Any, Awaitable, Callable, Optional, Tuple, TypedDict

from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME
from astrapy.constants import Environment
from astrapy.admin import parse_api_endpoint


class AstraDBCredentials(TypedDict):
    token: str
    api_endpoint: str
    namespace: Optional[str]


class AstraDBCredentialsInfo(TypedDict):
    environment: str
    region: str


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
def astra_db_credentials_kwargs() -> AstraDBCredentials:
    ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
    ASTRA_DB_API_ENDPOINT = os.environ["ASTRA_DB_API_ENDPOINT"]
    ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)
    astra_db_creds: AstraDBCredentials = {
        "token": ASTRA_DB_APPLICATION_TOKEN,
        "api_endpoint": ASTRA_DB_API_ENDPOINT,
        "namespace": ASTRA_DB_KEYSPACE,
    }

    return astra_db_creds


@pytest.fixture(scope="session")
def astra_db_credentials_info(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> AstraDBCredentialsInfo:
    api_endpoint = astra_db_credentials_kwargs["api_endpoint"]
    env, reg = env_region_from_endpoint(api_endpoint)

    astra_db_cred_info: AstraDBCredentialsInfo = {
        "environment": env,
        "region": reg,
    }

    return astra_db_cred_info


@pytest.fixture(scope="session")
def astra_invalid_db_credentials_kwargs(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> AstraDBCredentials:
    astra_db_creds: AstraDBCredentials = {
        "token": astra_db_credentials_kwargs["token"],
        "namespace": astra_db_credentials_kwargs["namespace"],
        "api_endpoint": "http://localhost:1234",
    }

    return astra_db_creds
