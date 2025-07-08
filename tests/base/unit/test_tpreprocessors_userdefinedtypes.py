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

import json

import pytest

from astrapy.data.table import map2tuple_checker_insert_one
from astrapy.data.utils.table_converters import preprocess_table_payload
from astrapy.data_types import (
    DataAPIDictUDT,
    DataAPIMap,
    DataAPISet,
)
from astrapy.utils.api_options import (
    FullSerdesOptions,
    SerdesOptions,
    defaultSerdesOptions,
)

from ..table_udt_assets import (
    THE_BYTES,
    THE_SERIALIZED_BYTES,
    THE_SERIALIZED_TIMESTAMP,
    THE_TIMESTAMP,
    UnitExtendedPlayer,
    _unit_extended_player_serializer,
)

OPTIONS_DAM = defaultSerdesOptions.with_override(
    SerdesOptions(encode_maps_as_lists_in_tables="DATAAPIMAPS"),
)
OPTIONS_NEV = defaultSerdesOptions.with_override(
    SerdesOptions(encode_maps_as_lists_in_tables="NEVER"),
)
OPTIONS_ALW = defaultSerdesOptions.with_override(
    SerdesOptions(encode_maps_as_lists_in_tables="ALWAYS"),
)

FULL_WRAPPABLE_DICT = {
    "name": "Jamie",
    "age": "45",
    "blb": THE_BYTES,
    "ts": THE_TIMESTAMP,
}
FULL_WRAPPED_DICT = DataAPIDictUDT(FULL_WRAPPABLE_DICT)
FULL_EXTENDEDPLAYER = UnitExtendedPlayer(**FULL_WRAPPABLE_DICT)  # type: ignore[arg-type]
FULL_SERIALIZED_DICT = {
    "name": "Jamie",
    "age": "45",
    "blb": THE_SERIALIZED_BYTES,
    "ts": THE_SERIALIZED_TIMESTAMP,
}
FULL_SERIALIZED_TUPLIFIED_DICT = [[k, v] for k, v in FULL_SERIALIZED_DICT.items()]
FULL_DICTWRAPPER_ROW = {
    "d": FULL_WRAPPABLE_DICT,
    "wr": FULL_WRAPPED_DICT,
    "dam": DataAPIMap([("k", "v")]),
    "dami": DataAPIMap([(1, "v1")]),
    "das_wr": DataAPISet([FULL_WRAPPED_DICT]),
    "dam_wr": DataAPIMap([("k1", FULL_WRAPPED_DICT)]),
    "l_wr": [FULL_WRAPPED_DICT],
    "m_wr": {"k2": FULL_WRAPPED_DICT},
}
FULL_CUSTOMCLASS_ROW = {
    "d": FULL_WRAPPABLE_DICT,
    "wr": FULL_EXTENDEDPLAYER,
    "dam": DataAPIMap([("k", "v")]),
    "dami": DataAPIMap([(1, "v1")]),
    "das_wr": DataAPISet([FULL_EXTENDEDPLAYER]),
    "dam_wr": DataAPIMap([("k1", FULL_EXTENDEDPLAYER)]),
    "l_wr": [FULL_EXTENDEDPLAYER],
    "m_wr": {"k2": FULL_EXTENDEDPLAYER},
}
FULL_SERIALIZED_ROW_DAM = {
    "d": FULL_SERIALIZED_DICT,
    "wr": FULL_SERIALIZED_DICT,
    "dam": [["k", "v"]],
    "dami": [[1, "v1"]],
    "das_wr": [FULL_SERIALIZED_DICT],
    "dam_wr": [["k1", FULL_SERIALIZED_DICT]],
    "l_wr": [FULL_SERIALIZED_DICT],
    "m_wr": {"k2": FULL_SERIALIZED_DICT},
}
FULL_SERIALIZED_ROW_NEV = {
    "d": FULL_SERIALIZED_DICT,
    "wr": FULL_SERIALIZED_DICT,
    "dam": {"k": "v"},
    "dami": {1: "v1"},
    "das_wr": [FULL_SERIALIZED_DICT],
    "dam_wr": {"k1": FULL_SERIALIZED_DICT},
    "l_wr": [FULL_SERIALIZED_DICT],
    "m_wr": {"k2": FULL_SERIALIZED_DICT},
}
FULL_SERIALIZED_ROW_ALW = {
    "d": FULL_SERIALIZED_TUPLIFIED_DICT,
    "wr": FULL_SERIALIZED_DICT,
    "dam": [["k", "v"]],
    "dami": [[1, "v1"]],
    "das_wr": [FULL_SERIALIZED_DICT],
    "dam_wr": [["k1", FULL_SERIALIZED_DICT]],
    "l_wr": [FULL_SERIALIZED_DICT],
    "m_wr": [["k2", FULL_SERIALIZED_DICT]],
}


class TestTPreprocessorsUserDefinedTypes:
    @pytest.mark.describe("Sanity check on map-as-lists default serdes options")
    def test_udt_serdesoptions_sanitycheck(self) -> None:
        assert OPTIONS_DAM == defaultSerdesOptions

    @pytest.mark.describe("test of udt conversion in preprocessing for DataAPIDictUDT")
    def test_udt_preprocessing_dataapidictudt(self) -> None:
        preprocessed_dam = preprocess_table_payload(
            {"insertOne": {"document": FULL_DICTWRAPPER_ROW}},
            OPTIONS_DAM,
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]
        preprocessed_nev = preprocess_table_payload(
            {"insertOne": {"document": FULL_DICTWRAPPER_ROW}},
            OPTIONS_NEV,
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]
        preprocessed_alw = preprocess_table_payload(
            {"insertOne": {"document": FULL_DICTWRAPPER_ROW}},
            OPTIONS_ALW,
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]

        assert preprocessed_dam == FULL_SERIALIZED_ROW_DAM
        assert preprocessed_nev == FULL_SERIALIZED_ROW_NEV
        assert preprocessed_alw == FULL_SERIALIZED_ROW_ALW

        _ = json.dumps(preprocessed_dam)
        _ = json.dumps(preprocessed_nev)
        _ = json.dumps(preprocessed_alw)

    @pytest.mark.describe(
        "test of udt conversion in preprocessing for unregistered class",
    )
    def test_udt_preprocessing_unregisteredclass(self) -> None:
        preprocessed_dam = preprocess_table_payload(
            {"insertOne": {"document": FULL_CUSTOMCLASS_ROW}},
            OPTIONS_DAM,
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]
        preprocessed_nev = preprocess_table_payload(
            {"insertOne": {"document": FULL_CUSTOMCLASS_ROW}},
            OPTIONS_NEV,
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]
        preprocessed_alw = preprocess_table_payload(
            {"insertOne": {"document": FULL_CUSTOMCLASS_ROW}},
            OPTIONS_ALW,
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]

        with pytest.raises(TypeError, match="JSON serializable"):
            json.dumps(preprocessed_dam)
        with pytest.raises(TypeError, match="JSON serializable"):
            json.dumps(preprocessed_nev)
        with pytest.raises(TypeError, match="JSON serializable"):
            json.dumps(preprocessed_alw)

    @pytest.mark.describe(
        "test of udt conversion in preprocessing for a registered class",
    )
    def test_udt_preprocessing_registeredclass(self) -> None:
        def _register_options(api_o: FullSerdesOptions) -> FullSerdesOptions:
            return api_o.with_override(
                SerdesOptions(
                    serializer_by_class={
                        UnitExtendedPlayer: _unit_extended_player_serializer,
                    },
                )
            )

        preprocessed_dam = preprocess_table_payload(
            {"insertOne": {"document": FULL_CUSTOMCLASS_ROW}},
            _register_options(OPTIONS_DAM),
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]
        preprocessed_nev = preprocess_table_payload(
            {"insertOne": {"document": FULL_CUSTOMCLASS_ROW}},
            _register_options(OPTIONS_NEV),
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]
        preprocessed_alw = preprocess_table_payload(
            {"insertOne": {"document": FULL_CUSTOMCLASS_ROW}},
            _register_options(OPTIONS_ALW),
            map2tuple_checker_insert_one,
        )["insertOne"]["document"]  # type: ignore[index]

        assert preprocessed_dam == FULL_SERIALIZED_ROW_DAM
        assert preprocessed_nev == FULL_SERIALIZED_ROW_NEV
        assert preprocessed_alw == FULL_SERIALIZED_ROW_ALW

        _ = json.dumps(preprocessed_dam)
        _ = json.dumps(preprocessed_nev)
        _ = json.dumps(preprocessed_alw)
