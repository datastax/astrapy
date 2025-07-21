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

from ..table_structure_assets import dict_equal_same_class
from ..table_udt_assets import (
    THE_BYTES,
    THE_DATETIME,
    THE_SERIALIZED_BYTES,
    THE_SERIALIZED_TIMESTAMP,
    THE_TIMESTAMP,
    THE_TIMEZONE,
    UNIT_EXTENDED_PLAYER_TYPE_DEFINITION,
    UNIT_EXTENDED_PLAYER_TYPE_NAME,
    UNIT_OPTLST_PLAYER_TYPE_DEFINITION,
    UNIT_OPTLST_PLAYER_TYPE_NAME,
    UnitExtendedPlayer,
    UnitNullableRequiringPlayer,
    _unit_extended_player_from_dict,
    _unit_optlst_player_from_dict,
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
                    "type": "userDefined",
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "set_udt": {
                "type": "set",
                "valueType": {
                    "type": "userDefined",
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "map_str_udt": {
                "type": "map",
                "keyType": "text",
                "valueType": {
                    "type": "userDefined",
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "map_int_udt_aslist": {
                "type": "map",
                "keyType": "int",
                "valueType": {
                    "type": "userDefined",
                    "udtName": UNIT_EXTENDED_PLAYER_TYPE_NAME,
                    "definition": UNIT_EXTENDED_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
        },
        "primaryKey": {"partitionBy": [], "partitionSort": {}},
    },
}
COLUMNS = ListTableDescriptor.coerce(TABLE_DESCRIPTION).definition.columns

INCOMPLETE_UDTS_TABLE_DESCRIPTION = {
    "name": "table_unit_incomplete_udt_deserialize_test",
    "definition": {
        "columns": {
            "p_text": {"type": "text"},
            "scalar_udt": {
                "type": "userDefined",
                "udtName": UNIT_OPTLST_PLAYER_TYPE_NAME,
                "definition": UNIT_OPTLST_PLAYER_TYPE_DEFINITION.as_dict(),
            },
            "list_udts": {
                "type": "list",
                "valueType": {
                    "type": "userDefined",
                    "udtName": UNIT_OPTLST_PLAYER_TYPE_NAME,
                    "definition": UNIT_OPTLST_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "set_udts": {
                "type": "set",
                "valueType": {
                    "type": "userDefined",
                    "udtName": UNIT_OPTLST_PLAYER_TYPE_NAME,
                    "definition": UNIT_OPTLST_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
            "map_udts": {
                "type": "map",
                "keyType": "int",
                "valueType": {
                    "type": "userDefined",
                    "udtName": UNIT_OPTLST_PLAYER_TYPE_NAME,
                    "definition": UNIT_OPTLST_PLAYER_TYPE_DEFINITION.as_dict(),
                },
            },
        },
        "primaryKey": {"partitionBy": ["p_text"], "partitionSort": {}},
    },
}
INCOMPLETE_UDTS_TABLE_COLUMNS = ListTableDescriptor.coerce(
    INCOMPLETE_UDTS_TABLE_DESCRIPTION,
).definition.columns
INCOMPLETE_UDTS_PARTIAL_UDT_INPUT = {"age": 42}
INCOMPLETE_UDTS_FULL_UDT_INPUT = {"name": "Kyle", "age": 18, "victories": ["x", "y"]}
INCOMPLETE_UDTS_NULL_UDT_RESULT = UnitNullableRequiringPlayer(
    name=None,
    age=None,
    victories=[],
)
INCOMPLETE_UDTS_PARTIAL_UDT_RESULT = UnitNullableRequiringPlayer(
    name=None,
    age=42,
    victories=[],
)
INCOMPLETE_UDTS_FULL_UDT_RESULT = UnitNullableRequiringPlayer(
    name="Kyle",
    age=18,
    victories=["x", "y"],
)
#
INCOMPLETE_UDTS_OMITTED_OUTPUT_ROW_TO_POSTPROCESS = {"p_text": "omitteds"}
INCOMPLETE_UDTS_NULLFILLED_OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "nulls",
    "scalar_udt": None,
    "list_udts": [None],
    "set_udts": [None],
    "map_udts": {123: None},
}
INCOMPLETE_UDTS_NULLFILLED_OUTPUT_ROW_TO_POSTPROCESS_TUPLES = {
    "p_text": "nulls",
    "scalar_udt": None,
    "list_udts": [None],
    "set_udts": [None],
    "map_udts": [[123, None]],
}
INCOMPLETE_UDTS_EMPTIES_OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "empties",
    "scalar_udt": {},
    "list_udts": [{}],
    "set_udts": [{}],
    "map_udts": {321: {}},
}
INCOMPLETE_UDTS_EMPTIES_OUTPUT_ROW_TO_POSTPROCESS_TUPLES = {
    "p_text": "empties",
    "scalar_udt": {},
    "list_udts": [{}],
    "set_udts": [{}],
    "map_udts": [[321, {}]],
}
INCOMPLETE_UDTS_PARTIAL_OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "partials",
    "scalar_udt": INCOMPLETE_UDTS_PARTIAL_UDT_INPUT,
    "list_udts": [INCOMPLETE_UDTS_PARTIAL_UDT_INPUT],
    "set_udts": [INCOMPLETE_UDTS_PARTIAL_UDT_INPUT],
    "map_udts": {456: INCOMPLETE_UDTS_PARTIAL_UDT_INPUT},
}
INCOMPLETE_UDTS_PARTIAL_OUTPUT_ROW_TO_POSTPROCESS_TUPLES = {
    "p_text": "partials",
    "scalar_udt": INCOMPLETE_UDTS_PARTIAL_UDT_INPUT,
    "list_udts": [INCOMPLETE_UDTS_PARTIAL_UDT_INPUT],
    "set_udts": [INCOMPLETE_UDTS_PARTIAL_UDT_INPUT],
    "map_udts": [[456, INCOMPLETE_UDTS_PARTIAL_UDT_INPUT]],
}
INCOMPLETE_UDTS_FULL_OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "fulls",
    "scalar_udt": INCOMPLETE_UDTS_FULL_UDT_INPUT,
    "list_udts": [INCOMPLETE_UDTS_FULL_UDT_INPUT],
    "set_udts": [INCOMPLETE_UDTS_FULL_UDT_INPUT],
    "map_udts": {789: INCOMPLETE_UDTS_FULL_UDT_INPUT},
}
INCOMPLETE_UDTS_FULL_OUTPUT_ROW_TO_POSTPROCESS_TUPLES = {
    "p_text": "fulls",
    "scalar_udt": INCOMPLETE_UDTS_FULL_UDT_INPUT,
    "list_udts": [INCOMPLETE_UDTS_FULL_UDT_INPUT],
    "set_udts": [INCOMPLETE_UDTS_FULL_UDT_INPUT],
    "map_udts": [[789, INCOMPLETE_UDTS_FULL_UDT_INPUT]],
}
#
INCOMPLETE_UDTS_OMITTED_OUTPUT_EXPECTED_ROW = {
    "p_text": "omitteds",
    "scalar_udt": INCOMPLETE_UDTS_NULL_UDT_RESULT,
    "list_udts": [],
    "set_udts": DataAPISet([]),
    "map_udts": DataAPIMap({}),
}
INCOMPLETE_UDTS_NULLFILLED_OUTPUT_EXPECTED_ROW = {
    "p_text": "nulls",
    "scalar_udt": INCOMPLETE_UDTS_NULL_UDT_RESULT,
    "list_udts": [INCOMPLETE_UDTS_NULL_UDT_RESULT],
    "set_udts": DataAPISet([INCOMPLETE_UDTS_NULL_UDT_RESULT]),
    "map_udts": DataAPIMap({123: INCOMPLETE_UDTS_NULL_UDT_RESULT}),
}
INCOMPLETE_UDTS_EMPTIES_OUTPUT_EXPECTED_ROW = {
    "p_text": "empties",
    "scalar_udt": INCOMPLETE_UDTS_NULL_UDT_RESULT,
    "list_udts": [INCOMPLETE_UDTS_NULL_UDT_RESULT],
    "set_udts": DataAPISet([INCOMPLETE_UDTS_NULL_UDT_RESULT]),
    "map_udts": DataAPIMap({321: INCOMPLETE_UDTS_NULL_UDT_RESULT}),
}
INCOMPLETE_UDTS_PARTIAL_OUTPUT_EXPECTED_ROW = {
    "p_text": "partials",
    "scalar_udt": INCOMPLETE_UDTS_PARTIAL_UDT_RESULT,
    "list_udts": [INCOMPLETE_UDTS_PARTIAL_UDT_RESULT],
    "set_udts": DataAPISet([INCOMPLETE_UDTS_PARTIAL_UDT_RESULT]),
    "map_udts": DataAPIMap({456: INCOMPLETE_UDTS_PARTIAL_UDT_RESULT}),
}
INCOMPLETE_UDTS_FULL_OUTPUT_EXPECTED_ROW = {
    "p_text": "fulls",
    "scalar_udt": INCOMPLETE_UDTS_FULL_UDT_RESULT,
    "list_udts": [INCOMPLETE_UDTS_FULL_UDT_RESULT],
    "set_udts": DataAPISet([INCOMPLETE_UDTS_FULL_UDT_RESULT]),
    "map_udts": DataAPIMap({789: INCOMPLETE_UDTS_FULL_UDT_RESULT}),
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
INCOMPLETE_DESERIALIZER_OPTIONS = OPTIONS_CUSTOM.with_override(
    SerdesOptions(
        deserializer_by_udt={
            UNIT_OPTLST_PLAYER_TYPE_NAME: _unit_optlst_player_from_dict,
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

    @pytest.mark.describe("test of row postprocessors, incomplete UDT: omitted data")
    def test_row_postprocessors_incomplete_omitted_udts(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=INCOMPLETE_UDTS_TABLE_COLUMNS,
            options=INCOMPLETE_DESERIALIZER_OPTIONS,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(
            INCOMPLETE_UDTS_OMITTED_OUTPUT_ROW_TO_POSTPROCESS,
        )
        dict_equal_same_class(
            deserialized_row,
            INCOMPLETE_UDTS_OMITTED_OUTPUT_EXPECTED_ROW,
        )

    @pytest.mark.describe("test of row postprocessors, incomplete UDT: nullfilled data")
    def test_row_postprocessors_incomplete_nullfilled_udts(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=INCOMPLETE_UDTS_TABLE_COLUMNS,
            options=INCOMPLETE_DESERIALIZER_OPTIONS,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(
            INCOMPLETE_UDTS_NULLFILLED_OUTPUT_ROW_TO_POSTPROCESS,
        )
        dict_equal_same_class(
            deserialized_row,
            INCOMPLETE_UDTS_NULLFILLED_OUTPUT_EXPECTED_ROW,
        )
        deserialized_row_tuples = tpostprocessor(
            INCOMPLETE_UDTS_NULLFILLED_OUTPUT_ROW_TO_POSTPROCESS_TUPLES,
        )
        dict_equal_same_class(
            deserialized_row_tuples,
            INCOMPLETE_UDTS_NULLFILLED_OUTPUT_EXPECTED_ROW,
        )

    @pytest.mark.describe("test of row postprocessors, incomplete UDT: emptydict data")
    def test_row_postprocessors_incomplete_emptydict_udts(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=INCOMPLETE_UDTS_TABLE_COLUMNS,
            options=INCOMPLETE_DESERIALIZER_OPTIONS,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(
            INCOMPLETE_UDTS_EMPTIES_OUTPUT_ROW_TO_POSTPROCESS,
        )
        dict_equal_same_class(
            deserialized_row,
            INCOMPLETE_UDTS_EMPTIES_OUTPUT_EXPECTED_ROW,
        )
        deserialized_row_tuples = tpostprocessor(
            INCOMPLETE_UDTS_EMPTIES_OUTPUT_ROW_TO_POSTPROCESS_TUPLES,
        )
        dict_equal_same_class(
            deserialized_row_tuples,
            INCOMPLETE_UDTS_EMPTIES_OUTPUT_EXPECTED_ROW,
        )

    @pytest.mark.describe("test of row postprocessors, incomplete UDT: partial data")
    def test_row_postprocessors_incomplete_partial_udts(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=INCOMPLETE_UDTS_TABLE_COLUMNS,
            options=INCOMPLETE_DESERIALIZER_OPTIONS,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(
            INCOMPLETE_UDTS_PARTIAL_OUTPUT_ROW_TO_POSTPROCESS,
        )
        dict_equal_same_class(
            deserialized_row,
            INCOMPLETE_UDTS_PARTIAL_OUTPUT_EXPECTED_ROW,
        )
        deserialized_row_tuples = tpostprocessor(
            INCOMPLETE_UDTS_PARTIAL_OUTPUT_ROW_TO_POSTPROCESS_TUPLES,
        )
        dict_equal_same_class(
            deserialized_row_tuples,
            INCOMPLETE_UDTS_PARTIAL_OUTPUT_EXPECTED_ROW,
        )

    @pytest.mark.describe("test of row postprocessors, incomplete UDT: full data")
    def test_row_postprocessors_incomplete_full_udts(self) -> None:
        tpostprocessor = create_row_tpostprocessor(
            columns=INCOMPLETE_UDTS_TABLE_COLUMNS,
            options=INCOMPLETE_DESERIALIZER_OPTIONS,
            similarity_pseudocolumn=None,
        )
        deserialized_row = tpostprocessor(
            INCOMPLETE_UDTS_FULL_OUTPUT_ROW_TO_POSTPROCESS,
        )
        dict_equal_same_class(
            deserialized_row,
            INCOMPLETE_UDTS_FULL_OUTPUT_EXPECTED_ROW,
        )
        deserialized_row_tuples = tpostprocessor(
            INCOMPLETE_UDTS_FULL_OUTPUT_ROW_TO_POSTPROCESS_TUPLES,
        )
        dict_equal_same_class(
            deserialized_row_tuples,
            INCOMPLETE_UDTS_FULL_OUTPUT_EXPECTED_ROW,
        )
