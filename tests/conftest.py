# main conftest for shared fixtures (if any).
import functools
import os
import pytest
import warnings
from deprecation import UnsupportedWarning
from typing import Any, Awaitable, Callable, Optional, TypedDict

from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME


class AstraDBCredentials(TypedDict):
    token: str
    api_endpoint: str
    namespace: Optional[str]


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
def astra_invalid_db_credentials_kwargs() -> AstraDBCredentials:
    ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
    ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)
    astra_db_creds: AstraDBCredentials = {
        "token": ASTRA_DB_APPLICATION_TOKEN,
        "api_endpoint": "http://localhost:1234",
        "namespace": ASTRA_DB_KEYSPACE,
    }

    return astra_db_creds
