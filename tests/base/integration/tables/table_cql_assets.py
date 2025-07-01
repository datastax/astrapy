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

import re
from typing import TYPE_CHECKING

from astrapy.data_types import DataAPITimestamp
from astrapy.ids import UUID
from astrapy.info import CreateTypeDefinition

if TYPE_CHECKING:
    from cassandra.cluster import Session


TABLE_NAME_COUNTER = "test_table_counter"
CREATE_TABLE_COUNTER = (
    f"CREATE TABLE {TABLE_NAME_COUNTER} (a TEXT PRIMARY KEY, col_counter COUNTER);"
)
DROP_TABLE_COUNTER = f"DROP TABLE {TABLE_NAME_COUNTER};"
INSERTS_TABLE_COUNTER = [
    f"UPDATE {TABLE_NAME_COUNTER} SET col_counter=col_counter+137 WHERE a='a';",
    f"UPDATE {TABLE_NAME_COUNTER} SET col_counter=col_counter+1 WHERE a='z';",
]
FILTER_COUNTER = {"a": "a"}
EXPECTED_ROW_COUNTER = {"a": "a", "col_counter": 137}

TABLE_NAME_LOWSUPPORT = "test_table_lowsupport"
CREATE_TABLE_LOWSUPPORT = f"""CREATE TABLE {TABLE_NAME_LOWSUPPORT} (
  a TEXT,
  b TEXT,
  col_timeuuid TIMEUUID,
  col_static_timestamp TIMESTAMP STATIC,
  col_static_list LIST<INT> STATIC,
  col_static_list_exotic LIST<BLOB> STATIC,
  col_static_set SET<INT> STATIC,
  col_static_set_exotic SET<BLOB> STATIC,
  col_static_map MAP<INT, TEXT> STATIC,
  col_static_map_exotic MAP<BLOB,BLOB> STATIC,
  col_unsupported FROZEN<LIST<FROZEN<MAP<FROZEN<SET<FLOAT>>, SMALLINT>>>>,
  PRIMARY KEY ((A),B)
);"""
DROP_TABLE_LOWSUPPORT = f"DROP TABLE {TABLE_NAME_LOWSUPPORT};"
INSERTS_TABLE_LOWSUPPORT = [
    (
        f"INSERT INTO {TABLE_NAME_LOWSUPPORT}"
        "    ("
        "    a,"
        "    b,"
        "    col_timeuuid,"
        "    col_static_timestamp,"
        "    col_static_list,"
        "    col_static_list_exotic,"
        "    col_static_set,"
        "    col_static_set_exotic,"
        "    col_static_map,"
        "    col_static_map_exotic,"
        "    col_unsupported"
        ") VALUES ("
        "    'a',"
        "    'b',"
        "    caba4d50-5078-11f0-b63e-bf76cc9ee780,"
        "    '2022-01-01T12:34:56.000',"
        "    [1, 2, 3],"
        "    [0xff, 0xff],"
        "    {1, 2, 3},"
        "    {0xff, 0xff},"
        "    {1: 'one'},"
        "    {0xff: 0xff},"
        "    [{{0.1, 0.2}: 3}, {{0.3, 0.4}: 6}]"
        ");"
    ),
]
FILTER_LOWSUPPORT = {"a": "a", "b": "b"}
ILLEGAL_PROJECTIONS_LOWSUPPORT = [
    {"col_unsupported": True},
]
PROJECTION_LOWSUPPORT = {"col_unsupported": False}
EXPECTED_ROW_LOWSUPPORT = {
    "a": "a",
    "b": "b",
    "col_timeuuid": UUID("caba4d50-5078-11f0-b63e-bf76cc9ee780"),
    "col_static_list_exotic": [
        {"$binary": "/w=="},
        {"$binary": "/w=="},
    ],
    "col_static_set": [1, 2, 3],
    "col_static_map_exotic": [
        [
            {"$binary": "/w=="},
            {"$binary": "/w=="},
        ]
    ],
    "col_static_timestamp": DataAPITimestamp.from_string("2022-01-01T12:34:56Z"),
    "col_static_map": [[1, "one"]],
    "col_static_list": [1, 2, 3],
    "col_static_set_exotic": [{"$binary": "/w=="}],
}
LOWSUPPORT_TIMEUUID_STRING = "dd5907d0-520c-11f0-a406-bf76cc9ee780"
LOWSUPPORT_TIMEUUID_PK0 = {
    "a": "xA0",
    "b": "xB0",
}
LOWSUPPORT_TIMEUUID_DOC0 = {
    "col_timeuuid": UUID(LOWSUPPORT_TIMEUUID_STRING),
    **LOWSUPPORT_TIMEUUID_PK0,
}
LOWSUPPORT_TIMEUUID_PK1 = {
    "a": "xA1",
    "b": "xB1",
}
LOWSUPPORT_TIMEUUID_DOC1 = {
    "col_timeuuid": LOWSUPPORT_TIMEUUID_STRING,
    **LOWSUPPORT_TIMEUUID_PK1,
}


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
