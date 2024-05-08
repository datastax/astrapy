# main conftest for shared fixtures (if any).
import os
import pytest
from typing import Optional, TypedDict

from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME


class AstraDBCredentials(TypedDict):
    token: str
    api_endpoint: str
    namespace: Optional[str]


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
