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

"""Fixtures specific to the idiomatic testing."""

from __future__ import annotations

from typing import Any, Iterable

import pytest

from astrapy import AsyncDatabase, AsyncTable, DataAPIClient, Database, Table
from astrapy.api_options import APIOptions
from astrapy.constants import SortDocuments
from astrapy.info import (
    TableDefinition,
    # TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
)

from ..conftest import (
    IS_ASTRA_DB,
    SECONDARY_KEYSPACE,
    DataAPICredentials,
    DataAPICredentialsInfo,
)

DefaultTable = Table[dict[str, Any]]
DefaultAsyncTable = AsyncTable[dict[str, Any]]

TEST_ALL_RETURNS_TABLE_NAME = "test_table_all_returns"
TEST_ALL_RETURNS_TABLE_DEFINITION = TableDefinition(
    columns={
        "p_ascii": TableScalarColumnTypeDescriptor(column_type="ascii"),
        "p_bigint": TableScalarColumnTypeDescriptor(column_type="bigint"),
        "p_blob": TableScalarColumnTypeDescriptor(column_type="blob"),
        "p_boolean": TableScalarColumnTypeDescriptor(column_type="boolean"),
        "p_date": TableScalarColumnTypeDescriptor(column_type="date"),
        "p_decimal": TableScalarColumnTypeDescriptor(column_type="decimal"),
        "p_double": TableScalarColumnTypeDescriptor(column_type="double"),
        "p_duration": TableScalarColumnTypeDescriptor(column_type="duration"),
        "p_float": TableScalarColumnTypeDescriptor(column_type="float"),
        "p_inet": TableScalarColumnTypeDescriptor(column_type="inet"),
        "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
        "p_smallint": TableScalarColumnTypeDescriptor(column_type="smallint"),
        "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_text_nulled": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_text_omitted": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_time": TableScalarColumnTypeDescriptor(column_type="time"),
        "p_timestamp": TableScalarColumnTypeDescriptor(column_type="timestamp"),
        "p_uuid": TableScalarColumnTypeDescriptor(column_type="uuid"),
        "p_vector": TableVectorColumnTypeDescriptor(
            column_type="vector",
            dimension=3,
            service=None,
        ),
        "p_list_int": TableValuedColumnTypeDescriptor(
            column_type="list",
            value_type="int",
        ),
        # TODO reinstate once maps fixed
        # "p_map_text_text": TableKeyValuedColumnTypeDescriptor(
        #     column_type="map", key_type="text", value_type="text"
        # ),
        "p_set_int": TableValuedColumnTypeDescriptor(
            column_type="set",
            value_type="int",
        ),
        "p_double_minf": TableScalarColumnTypeDescriptor(column_type="double"),
        "p_double_pinf": TableScalarColumnTypeDescriptor(column_type="double"),
        "p_float_nan": TableScalarColumnTypeDescriptor(column_type="float"),
    },
    primary_key=TablePrimaryKeyDescriptor(
        partition_by=["p_ascii", "p_bigint"],
        partition_sort={
            "p_int": SortDocuments.ASCENDING,
            "p_boolean": SortDocuments.DESCENDING,
        },
    ),
)


@pytest.fixture(scope="session")
def client(
    data_api_credentials_info: DataAPICredentialsInfo,
) -> Iterable[DataAPIClient]:
    env = data_api_credentials_info["environment"]
    client = DataAPIClient(
        environment=env,
        api_options=APIOptions(
            database_additional_headers={"Feature-Flag-tables": "true"}
        ),
    )
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
def sync_table_all_returns(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_ALL_RETURNS_TABLE_NAME,
        definition=TEST_ALL_RETURNS_TABLE_DEFINITION,
    )
    yield table

    sync_database.drop_table(TEST_ALL_RETURNS_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_all_returns(
    sync_table_all_returns: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    # sync_table_all_returns.delete_many({}) TODO reinstate once available
    yield sync_table_all_returns


@pytest.fixture(scope="function")
def async_table_all_returns(
    sync_table_all_returns: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """An actual table on DB, the same as the sync counterpart"""
    yield sync_table_all_returns.to_async()


@pytest.fixture(scope="function")
def async_empty_table_all_returns(
    sync_empty_table_all_returns: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_all_returns.to_async()


__all__ = [
    "DataAPICredentials",
    "DataAPICredentialsInfo",
    "sync_database",
    "async_database",
    "IS_ASTRA_DB",
    "SECONDARY_KEYSPACE",
]
