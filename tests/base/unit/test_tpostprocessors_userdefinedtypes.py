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
from astrapy.info import ListTableDescriptor
from astrapy.utils.api_options import SerdesOptions, defaultSerdesOptions

from ..conftest import _repaint_NaNs

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
    },
}

EXPECTED_POSTPROCESSED_ROW = {
    "p_text": "italy",
    "udt_column": {
        "name": "John",
        "age": 40,
    },
}


class TestTPostProcessorsUserDefinedTypes:
    @pytest.mark.describe("test of row postprocessors with UDTs from schema")
    def test_row_postprocessors_udts_from_schema(self) -> None:
        col_desc = ListTableDescriptor.coerce(TABLE_DESCRIPTION)
        tpostprocessor = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=defaultSerdesOptions.with_override(
                SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
            similarity_pseudocolumn=None,
        )

        converted_column = tpostprocessor(OUTPUT_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column) == _repaint_NaNs(
            EXPECTED_POSTPROCESSED_ROW
        )

        with pytest.raises(ValueError):
            tpostprocessor({"bippy": 123})
