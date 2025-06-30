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

from dataclasses import dataclass
from typing import Any

import pytest

from astrapy.data.utils.extended_json_converters import convert_to_ejson_bytes
from astrapy.data.utils.table_converters import preprocess_table_payload
from astrapy.data_types import (
    DataAPIMap,
    DataAPISet,
    DataAPITimestamp,
    DataAPIUserDefinedType,
    DictDataAPIUserDefinedType,
    create_dataclass_userdefinedtype,
)
from astrapy.utils.api_options import defaultSerdesOptions

THE_BYTES = b"\xa6"
THE_TIMESTAMP = DataAPITimestamp.from_string("2025-10-29T01:25:37.123Z")


@dataclass
class ExtendedPlayer:
    """
    An example dataclass which may be used to represent a user-defined type (UDT)
    such as one would define, and create on the database, with this code:

    .. code-block:: python

        from astrapy.info import CreateTypeDefinition, ColumnType

        xplayer_udt_def = CreateTypeDefinition(fields={
            "name": ColumnType.TEXT,
            "age": ColumnType.INT,
            "blb": ColumnType.BLOB,
            "ts": ColumnType.TIMESTAMP,
        })

        database.create_type("xplayer_udt", definition=xplayer_udt_def)
    """

    name: str
    age: int
    blb: bytes
    ts: DataAPITimestamp


class TestTPreprocessorsUserDefinedTypes:
    @pytest.mark.parametrize(
        ("wrapped_object",),
        [
            (
                DictDataAPIUserDefinedType(
                    {
                        "name": "John",
                        "age": 40,
                        "blb": THE_BYTES,
                        "ts": THE_TIMESTAMP,
                    },
                ),
            ),
            (
                create_dataclass_userdefinedtype(ExtendedPlayer)(
                    ExtendedPlayer(
                        name="John",
                        age=40,
                        blb=THE_BYTES,
                        ts=THE_TIMESTAMP,
                    ),
                ),
            ),
        ],
        ids=["DictDataAPIUserDefinedType", "dataclass-factory-wrapper"],
    )
    @pytest.mark.describe("test of udt conversion in preprocessing, from dict wrapper")
    def test_udt_dict_preprocessing(
        self, wrapped_object: DataAPIUserDefinedType[Any]
    ) -> None:
        test_serialized_dict = {
            "name": "John",
            "age": 40,
            "blb": convert_to_ejson_bytes(THE_BYTES),
            "ts": THE_TIMESTAMP.to_string(),
        }

        # as scalar column
        payload_s = {"scalar_udt_column": wrapped_object}
        expected_s = {"scalar_udt_column": test_serialized_dict}
        converted_s = preprocess_table_payload(
            payload_s,
            defaultSerdesOptions,
            map2tuple_checker=None,
        )
        assert expected_s == converted_s

        # within collection columns
        payload_c = {
            "da_set_udt_column": DataAPISet([wrapped_object]),
            "list_udt_column": [wrapped_object],
            "map_udt_column": {"k": wrapped_object},
            "da_map_udt_column": DataAPIMap([("k", wrapped_object)]),
        }
        expected_c = {
            "da_set_udt_column": [test_serialized_dict],
            "list_udt_column": [test_serialized_dict],
            "map_udt_column": {"k": test_serialized_dict},
            "da_map_udt_column": {"k": test_serialized_dict},
        }
        converted_c = preprocess_table_payload(
            payload_c,
            defaultSerdesOptions,
            map2tuple_checker=None,
        )
        assert expected_c == converted_c
