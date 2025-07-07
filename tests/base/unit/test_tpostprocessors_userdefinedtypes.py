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

import pytest

from astrapy.data.utils.table_converters import create_row_tpostprocessor
from astrapy.data_types import (
    DataAPIDictUDT,
    DataAPIMap,
    DataAPISet,
)
from astrapy.info import ListTableDescriptor
from astrapy.utils.api_options import SerdesOptions, defaultSerdesOptions

from ..table_udt_assets import (
    PLAYER_TYPE_DEFINITION,
    PLAYER_TYPE_NAME,
    THE_BYTES,
    THE_DATETIME,
    THE_SERIALIZED_BYTES,
    THE_SERIALIZED_TIMESTAMP,
    THE_TIMESTAMP,
    THE_TIMEZONE,
    UNIT_EXTENDED_PLAYER_TYPE_DEFINITION,
    UNIT_EXTENDED_PLAYER_TYPE_NAME,
    UnitExtendedPlayer,
    _unit_extended_player_from_dict,
    dict_equal_same_class,
)

TABLE_DESCRIPTION = {
    "name": "table_unit_udt_deserialize_test",
    "definition": {
        "columns": {
            "p_text": {"type": "text"},
            "scalar_udt": {
                "type": "userDefined",
                "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
            },
            "list_udt": {
                "type": "list",
                "valueType": {
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "set_udt": {
                "type": "set",
                "valueType": {
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "map_str_udt": {
                "type": "map",
                "keyType": "text",
                "valueType": {
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "map_int_udt_aslist": {
                "type": "map",
                "keyType": "int",
                "valueType": {
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
        },
        "primaryKey": {"partitionBy": [], "partitionSort": {}},
    },
}
COLUMNS = ListTableDescriptor.coerce(TABLE_DESCRIPTION).definition.columns

MINI_TABLE_DESCRIPTION = {
    "name": "table_unit_udt_deserialize_test",
    "definition": {
        "columns": {
            "p_text": {"type": "text"},
            "scalar_udt": {
                "type": "userDefined",
                "udtName": PLAYER_TYPE_NAME,
                "definition": PLAYER_TYPE_DEFINITION.as_dict(),
            },
        },
        "primaryKey": {"partitionBy": [], "partitionSort": {}},
    },
}
MINI_COLUMNS = ListTableDescriptor.coerce(MINI_TABLE_DESCRIPTION).definition.columns
MINI_RESPONSE_PARTIAL_UDT_DICT = {"age": 101}
MINI_PARTIAL_OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "base",
    "scalar_udt": MINI_RESPONSE_PARTIAL_UDT_DICT,
}
# TODO - adjust expectations if different behaviour is discussed
MINI_EXPECTED_PARTIAL_ROW_DICTUDT = {
    "p_text": "base",
    "scalar_udt": DataAPIDictUDT(
        {
            # TODO: no `"name": None` here expected so far.
            "age": 101,
        }
    ),
}

RAW_RESPONSE_UDT_DICT = {
    "name": "John",
    "age": 40,
    "blb": THE_SERIALIZED_BYTES,
    "ts": THE_SERIALIZED_TIMESTAMP,
}
EXPECTED_STDLIB_DICT = {
    "name": "John",
    "age": 40,
    "blb": THE_BYTES,
    "ts": THE_DATETIME,
}
EXPECTED_DICTUDT = DataAPIDictUDT(
    {
        "name": "John",
        "age": 40,
        "blb": THE_BYTES,
        "ts": THE_TIMESTAMP,
    }
)
EXPECTED_STDLIB_CUSTOMUDT = UnitExtendedPlayer(
    name="John",
    age=40,
    blb=THE_BYTES,
    ts=THE_DATETIME,
)
EXPECTED_CUSTOMUDT = UnitExtendedPlayer(
    name="John",
    age=40,
    blb=THE_BYTES,
    ts=THE_TIMESTAMP,
)

OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "base",
    "scalar_udt": RAW_RESPONSE_UDT_DICT,
    "list_udt": [RAW_RESPONSE_UDT_DICT],
    "set_udt": [RAW_RESPONSE_UDT_DICT],
    "map_str_udt": {"k": RAW_RESPONSE_UDT_DICT},
    "map_int_udt_aslist": [[101, RAW_RESPONSE_UDT_DICT]],
}

EXPECTED_ROW_STDLIB_DICT = {
    "p_text": "base",
    "scalar_udt": EXPECTED_STDLIB_DICT,
    "list_udt": [EXPECTED_STDLIB_DICT],
    "set_udt": "ignored-for-this-test",
    "map_str_udt": {"k": EXPECTED_STDLIB_DICT},
    "map_int_udt_aslist": {101: EXPECTED_STDLIB_DICT},
}
EXPECTED_ROW_DICTUDT = {
    "p_text": "base",
    "scalar_udt": EXPECTED_DICTUDT,
    "list_udt": [EXPECTED_DICTUDT],
    "set_udt": DataAPISet([EXPECTED_DICTUDT]),
    "map_str_udt": DataAPIMap([("k", EXPECTED_DICTUDT)]),
    "map_int_udt_aslist": DataAPIMap([(101, EXPECTED_DICTUDT)]),
}
EXPECTED_ROW_STDLIB_CUSTOMUDT = {
    "p_text": "base",
    "scalar_udt": EXPECTED_STDLIB_CUSTOMUDT,
    "list_udt": [EXPECTED_STDLIB_CUSTOMUDT],
    "set_udt": "ignored-for-this-test",
    "map_str_udt": {"k": EXPECTED_STDLIB_CUSTOMUDT},
    "map_int_udt_aslist": {101: EXPECTED_STDLIB_CUSTOMUDT},
}
EXPECTED_ROW_CUSTOMUDT = {
    "p_text": "base",
    "scalar_udt": EXPECTED_CUSTOMUDT,
    "list_udt": [EXPECTED_CUSTOMUDT],
    "set_udt": DataAPISet([EXPECTED_CUSTOMUDT]),
    "map_str_udt": DataAPIMap([("k", EXPECTED_CUSTOMUDT)]),
    "map_int_udt_aslist": DataAPIMap([(101, EXPECTED_CUSTOMUDT)]),
}

BASE_OPTIONS = defaultSerdesOptions.with_override(
    SerdesOptions(datetime_tzinfo=THE_TIMEZONE),
)
OPTIONS_STDLIB = BASE_OPTIONS.with_override(
    SerdesOptions(custom_datatypes_in_reading=False),
)
OPTIONS_CUSTOM = BASE_OPTIONS.with_override(
    SerdesOptions(custom_datatypes_in_reading=True),
)
OPTIONS_STDLIB_CCLASS = OPTIONS_STDLIB.with_override(
    SerdesOptions(
        deserializer_by_udt={
            UNIT_EXTENDED_PLAYER_TYPE_NAME: _unit_extended_player_from_dict,
        }
    ),
)
OPTIONS_CUSTOM_CCLASS = OPTIONS_CUSTOM.with_override(
    SerdesOptions(
        deserializer_by_udt={
            UNIT_EXTENDED_PLAYER_TYPE_NAME: _unit_extended_player_from_dict,
        }
    ),
)


class TestTPostProcessorsUserDefinedTypes:
    @pytest.mark.describe(
        "test of UDTs in row postprocessors: custom datatypes, custom class"
    )
    def test_row_postprocessors_udts_customdt_customclass(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=COLUMNS,
            options=OPTIONS_CUSTOM_CCLASS,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(OUTPUT_ROW_TO_POSTPROCESS)
        dict_equal_same_class(deserialized_row, EXPECTED_ROW_CUSTOMUDT)

    @pytest.mark.describe(
        "test of UDTs in row postprocessors: stdlib datatypes, custom class"
    )
    def test_row_postprocessors_udts_stdlibdt_customclass(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=COLUMNS,
            options=OPTIONS_STDLIB_CCLASS,
            similarity_pseudocolumn=None,
        )
        # remove set column due to non-hashability
        deserialized_row = tpostprocessor(
            {k: v for k, v in OUTPUT_ROW_TO_POSTPROCESS.items() if "set" not in k}
        )
        expected_row = {
            **EXPECTED_ROW_STDLIB_CUSTOMUDT,
            **{"set_udt": set()},
        }
        dict_equal_same_class(deserialized_row, expected_row)

    @pytest.mark.describe(
        "test of UDTs in row postprocessors: custom datatypes, dict-wrapper class"
    )
    def test_row_postprocessors_udts_customdt_dictwrapperclass(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=COLUMNS,
            options=OPTIONS_CUSTOM,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(OUTPUT_ROW_TO_POSTPROCESS)
        dict_equal_same_class(deserialized_row, EXPECTED_ROW_DICTUDT)

    @pytest.mark.describe(
        "test of UDTs in row postprocessors: stdlib datatypes, dict-wrapper class"
    )
    def test_row_postprocessors_udts_stdlibdt_dictwrapperclass(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=COLUMNS,
            options=OPTIONS_STDLIB,
            similarity_pseudocolumn=None,
        )
        # remove set column due to non-hashability
        deserialized_row = tpostprocessor(
            {k: v for k, v in OUTPUT_ROW_TO_POSTPROCESS.items() if "set" not in k}
        )
        expected_row = {
            **EXPECTED_ROW_STDLIB_DICT,
            **{"set_udt": set()},
        }
        dict_equal_same_class(deserialized_row, expected_row)

    @pytest.mark.describe("test of row postprocessors with partial UDT provided")
    def test_row_postprocessors_partial_udt(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=MINI_COLUMNS,
            options=OPTIONS_CUSTOM,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(MINI_PARTIAL_OUTPUT_ROW_TO_POSTPROCESS)
        dict_equal_same_class(
            deserialized_row,
            MINI_EXPECTED_PARTIAL_ROW_DICTUDT,
        )
