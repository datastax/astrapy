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

import datetime
from dataclasses import dataclass

import pytest

from astrapy.data.utils.extended_json_converters import convert_to_ejson_bytes
from astrapy.data.utils.table_converters import create_row_tpostprocessor
from astrapy.data_types import (
    DataAPITimestamp,
    DictDataAPIUserDefinedType,
    create_dataclass_userdefinedtype,
)
from astrapy.info import ListTableDescriptor
from astrapy.utils.api_options import SerdesOptions, defaultSerdesOptions

from ..conftest import _repaint_NaNs


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
    ts: DataAPITimestamp | datetime.datetime


ExtendedPlayerWrapper = create_dataclass_userdefinedtype(ExtendedPlayer)

THE_BYTES = b"\xa6"
THE_TIMESTAMP = DataAPITimestamp.from_string("2025-10-29T01:25:37.123Z")
THE_TIMEZONE = datetime.timezone(datetime.timedelta(hours=2, minutes=45))

TABLE_DESCRIPTION = {
    "name": "table_simple",
    "definition": {
        "columns": {
            "p_text": {"type": "text"},
            "udt_column": {
                "type": "userDefined",
                "udtName": "player_udt",
                "definition": {
                    "fields": {
                        "name": {"type": "text"},
                        "age": {"type": "int"},
                        "blb": {"type": "blob"},
                        "ts": {"type": "timestamp"},
                    },
                },
                "apiSupport": {},
            },
        },
        "primaryKey": {"partitionBy": [], "partitionSort": {}},
    },
}

OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "italy",
    "udt_column": {
        "name": "John",
        "age": 40,
        "blb": convert_to_ejson_bytes(THE_BYTES),
        "ts": THE_TIMESTAMP.to_string(),
    },
}

EXPECTED_POSTPROCESSED_ROW_DICT_C = {
    "p_text": "italy",
    "udt_column": DictDataAPIUserDefinedType(
        {
            "name": "John",
            "age": 40,
            "blb": THE_BYTES,
            "ts": THE_TIMESTAMP,
        },
    ),
}

EXPECTED_POSTPROCESSED_ROW_DICT_NC = {
    "p_text": "italy",
    "udt_column": DictDataAPIUserDefinedType(
        {
            "name": "John",
            "age": 40,
            "blb": THE_BYTES,
            "ts": THE_TIMESTAMP.to_datetime(tz=THE_TIMEZONE),
        },
    ),
}

EXPECTED_POSTPROCESSED_ROW_DATACLASS_C = {
    "p_text": "italy",
    "udt_column": ExtendedPlayerWrapper(
        ExtendedPlayer(
            name="John",
            age=40,
            blb=THE_BYTES,
            ts=THE_TIMESTAMP,
        )
    ),
}

EXPECTED_POSTPROCESSED_ROW_DATACLASS_NC = {
    "p_text": "italy",
    "udt_column": ExtendedPlayerWrapper(
        ExtendedPlayer(
            name="John",
            age=40,
            blb=THE_BYTES,
            ts=THE_TIMESTAMP.to_datetime(tz=THE_TIMEZONE),
        )
    ),
}

ExtendedPlayerWrapper


class TestTPostProcessorsUserDefinedTypes:
    @pytest.mark.describe("test of row postprocessors with UDTs to dict from schema")
    def test_row_postprocessors_udts_dict_from_schema(self) -> None:
        col_desc = ListTableDescriptor.coerce(TABLE_DESCRIPTION)

        tpostprocessor_c = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=defaultSerdesOptions.with_override(
                SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
            similarity_pseudocolumn=None,
        )
        converted_column_c = tpostprocessor_c(OUTPUT_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column_c) == _repaint_NaNs(
            EXPECTED_POSTPROCESSED_ROW_DICT_C
        )
        with pytest.raises(ValueError):
            tpostprocessor_c({"bippy": 123})

        tpostprocessor_nc = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=defaultSerdesOptions.with_override(
                SerdesOptions(
                    custom_datatypes_in_reading=False,
                    datetime_tzinfo=THE_TIMEZONE,
                ),
            ),
            similarity_pseudocolumn=None,
        )
        converted_column_nc = tpostprocessor_nc(OUTPUT_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column_nc) == _repaint_NaNs(
            EXPECTED_POSTPROCESSED_ROW_DICT_NC
        )
        with pytest.raises(ValueError):
            tpostprocessor_nc({"bippy": 123})

    @pytest.mark.describe(
        "test of row postprocessors with UDTs to dataclass from schema"
    )
    def test_row_postprocessors_udts_dataclass_from_schema(self) -> None:
        col_desc = ListTableDescriptor.coerce(TABLE_DESCRIPTION)

        tpostprocessor_c = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=defaultSerdesOptions.with_override(
                SerdesOptions(
                    custom_datatypes_in_reading=True,
                    udt_class_map={"player_udt": ExtendedPlayerWrapper},
                ),
            ),
            similarity_pseudocolumn=None,
        )
        converted_column_c = tpostprocessor_c(OUTPUT_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column_c) == _repaint_NaNs(
            EXPECTED_POSTPROCESSED_ROW_DATACLASS_C
        )
        with pytest.raises(ValueError):
            tpostprocessor_c({"bippy": 123})

        tpostprocessor_nc = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=defaultSerdesOptions.with_override(
                SerdesOptions(
                    custom_datatypes_in_reading=False,
                    datetime_tzinfo=THE_TIMEZONE,
                    udt_class_map={"player_udt": ExtendedPlayerWrapper},
                ),
            ),
            similarity_pseudocolumn=None,
        )
        converted_column_nc = tpostprocessor_nc(OUTPUT_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column_nc) == _repaint_NaNs(
            EXPECTED_POSTPROCESSED_ROW_DATACLASS_NC
        )
        with pytest.raises(ValueError):
            tpostprocessor_nc({"bippy": 123})
