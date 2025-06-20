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

import os
import re
from typing import TYPE_CHECKING

import pytest

from astrapy import Database
from astrapy.exceptions import DataAPIResponseException
from astrapy.info import (
    AlterTypeAddFields,
    AlterTypeRenameFields,
    CreateTypeDefinition,
)

from ..conftest import CQL_AVAILABLE, DataAPICredentials

if TYPE_CHECKING:
    from cassandra.cluster import Session

try:
    from cassandra.cluster import Session
except ImportError:
    pass

UDT_NAME = "test_udt"
UDT_DEF0 = CreateTypeDefinition(
    fields={
        "f_float0": "float",
        "f_float1": "float",
        "f_float2": "float",
    }
)
UDT_ALTER_OPS = [
    AlterTypeAddFields(fields={"f_text0": "text"}),
    AlterTypeRenameFields(fields={"f_float0": "z_float0"}),
]
UDT_DEF1 = CreateTypeDefinition(
    fields={
        "z_float0": "float",
        "f_float1": "float",
        "f_float2": "float",
        "f_text0": "text",
    }
)


def _extract_udt_definition(
    session: Session, keyspace: str, udt_name: str
) -> CreateTypeDefinition | None:
    udt_names: list[str] = [
        row.name
        for row in session.execute("desc types;")
        if row.keyspace_name == "default_keyspace"
    ]
    if udt_name not in udt_names:
        return None
    udt_create_stmt = session.execute(f"desc type {udt_name};").one().create_statement

    full_type_name = f"{keyspace}.{udt_name}"
    pattern = re.compile(
        rf"(?i)\bCREATE\s+TYPE\s+{re.escape(full_type_name)}\s*\(\s*(.*?)\s*\);",
        re.DOTALL,
    )
    match = pattern.search(udt_create_stmt)

    fields: list[tuple[str, str]]
    if match:
        fields_str = match.group(1)
        fields = [
            tuple(map(str.strip, line.split(None, 1)))  # type: ignore[misc]
            for line in re.split(r",\s*(?![^(]*\))", fields_str.strip())
            if line.strip()
        ]
    else:
        fields = []

    return CreateTypeDefinition(fields=dict(fields))


@pytest.mark.skipif(
    "ASTRAPY_TEST_UDT" not in os.environ,
    reason="UDT testing not enabled",
)
@pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
class TestTableUserDefinedTypes:
    @pytest.mark.describe("Test of UDT lifecycle, sync")
    def test_table_udt_lifecycle_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        cql_session: Session,
        sync_database: Database,
    ) -> None:
        try:
            sync_database.create_type(UDT_NAME, definition=UDT_DEF0)
            with pytest.raises(DataAPIResponseException):
                sync_database.create_type(UDT_NAME, definition=UDT_DEF0)
            sync_database.create_type(UDT_NAME, definition=UDT_DEF0, if_not_exists=True)

            assert UDT_DEF0 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            sync_database.alter_type(UDT_NAME, operations=UDT_ALTER_OPS)
            with pytest.raises(DataAPIResponseException):
                sync_database.alter_type(UDT_NAME, operations=UDT_ALTER_OPS)

            assert UDT_DEF1 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            sync_database.drop_type(UDT_NAME)
            assert (
                _extract_udt_definition(
                    cql_session,
                    data_api_credentials_kwargs["keyspace"],
                    UDT_NAME,
                )
                is None
            )
            with pytest.raises(DataAPIResponseException):
                sync_database.drop_type(UDT_NAME)
            sync_database.drop_type(UDT_NAME, if_exists=True)

            sync_database.create_type(UDT_NAME, definition=UDT_DEF0)
            sync_database.drop_type(UDT_NAME)
        finally:
            sync_database.drop_type(UDT_NAME, if_exists=True)
