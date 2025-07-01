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

from typing import Any

import pytest

from astrapy.data.table import map2tuple_checker_insert_one
from astrapy.data.utils.extended_json_converters import convert_to_ejson_bytes
from astrapy.data.utils.table_converters import preprocess_table_payload
from astrapy.data_types import (
    DataAPIMap,
    DataAPISet,
    DataAPITimestamp,
    DataAPIUserDefinedType,
    DictDataAPIUserDefinedType,
)
from astrapy.utils.api_options import SerdesOptions, defaultSerdesOptions

from ..table_udt_assets import (
    ExtendedPlayer,
    ExtendedPlayerUDTWrapper,
    NullablePlayer,
    NullablePlayerUDTWrapper,
)

THE_BYTES = b"\xa6"
THE_TIMESTAMP = DataAPITimestamp.from_string("2025-10-29T01:25:37.123Z")


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
                ExtendedPlayerUDTWrapper(
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
    @pytest.mark.describe("test of udt conversion in preprocessing, from a wrapper")
    def test_udt_wrapper_preprocessing(
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

        # maps udt-valued maps, as list-of-pairs and as-dictionaries
        payload_m = {
            "insertOne": {
                "document": {
                    "map_udt_column": {"k": wrapped_object},
                    "da_map_udt_column": DataAPIMap([("k", wrapped_object)]),
                },
            },
        }
        expected_m_never = {
            "insertOne": {
                "document": {
                    "map_udt_column": {"k": test_serialized_dict},
                    "da_map_udt_column": {"k": test_serialized_dict},
                },
            },
        }
        expected_m_dataapimaps = {
            "insertOne": {
                "document": {
                    "map_udt_column": {"k": test_serialized_dict},
                    "da_map_udt_column": [["k", test_serialized_dict]],
                },
            },
        }
        expected_m_always = {
            "insertOne": {
                "document": {
                    "map_udt_column": [["k", test_serialized_dict]],
                    "da_map_udt_column": [["k", test_serialized_dict]],
                },
            },
        }
        converted_m_never = preprocess_table_payload(
            payload_m,
            defaultSerdesOptions.with_override(
                SerdesOptions(
                    encode_maps_as_lists_in_tables="NEVER",
                )
            ),
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_m_never == converted_m_never

        converted_m_dataapimaps = preprocess_table_payload(
            payload_m,
            defaultSerdesOptions.with_override(
                SerdesOptions(
                    encode_maps_as_lists_in_tables="DATAAPIMAPS",
                )
            ),
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_m_dataapimaps == converted_m_dataapimaps

        converted_m_always = preprocess_table_payload(
            payload_m,
            defaultSerdesOptions.with_override(
                SerdesOptions(
                    encode_maps_as_lists_in_tables="ALWAYS",
                )
            ),
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_m_always == converted_m_always

    @pytest.mark.describe(
        "test of udt conversion in preprocessing, from a partial dict"
    )
    def test_udt_partialdict_preprocessing(self) -> None:
        wrapped_object = NullablePlayerUDTWrapper(NullablePlayer(name="JustJohn"))
        test_serialized_dict = {"name": "JustJohn"}

        payload_s = {"scalar_udt_column": wrapped_object}
        expected_s = {"scalar_udt_column": test_serialized_dict}
        converted_s = preprocess_table_payload(
            payload_s,
            defaultSerdesOptions,
            map2tuple_checker=None,
        )
        assert expected_s == converted_s
