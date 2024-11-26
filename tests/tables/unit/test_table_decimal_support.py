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

from astrapy.constants import DefaultRowType
from astrapy.data.utils.table_converters import _TableConverterAgent
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import FullSerdesOptions, defaultAPIOptions

from ..conftest import _repaint_NaNs
from ..decimal_support_assets import (
    BASELINE_COLUMNS,
    BASELINE_KEY_STR,
    BASELINE_OBJ,
    COLLTYPES_CUSTOM_OBJ_SCHEMA_TRIPLES,
    COLLTYPES_TRIPLE_IDS,
    WDECS_KEY_STR,
    WDECS_OBJ,
    WDECS_OBJ_COLUMNS,
)


class TestTableDecimalSupportUnit:
    @pytest.mark.describe("test of decimal-related conversions in table codec paths")
    def test_decimalsupport_table_codecpath(self) -> None:
        t_agent: _TableConverterAgent[DefaultRowType] = _TableConverterAgent(
            options=defaultAPIOptions(environment="prod").serdes_options,
        )
        # baseline, encode then decode and check
        baseline_fully_encoded = APICommander._decimal_aware_encode_payload(
            t_agent.preprocess_payload(BASELINE_OBJ)
        )
        baseline_obj_2 = t_agent.postprocess_row(
            APICommander._decimal_aware_parse_json_response(baseline_fully_encoded),  # type: ignore[arg-type]
            columns_dict=BASELINE_COLUMNS,
            similarity_pseudocolumn=None,
        )
        assert _repaint_NaNs(baseline_obj_2) == _repaint_NaNs(BASELINE_OBJ)
        # with-decimals, encode then decode and check
        wdecs_fully_encoded = APICommander._decimal_aware_encode_payload(
            t_agent.preprocess_payload(WDECS_OBJ)
        )
        wdecs_2 = t_agent.postprocess_row(
            APICommander._decimal_aware_parse_json_response(wdecs_fully_encoded),  # type: ignore[arg-type]
            columns_dict=WDECS_OBJ_COLUMNS,
            similarity_pseudocolumn=None,
        )
        assert _repaint_NaNs(wdecs_2) == _repaint_NaNs(WDECS_OBJ)

        # baseline, keys (decode only)
        baseline_kobj_2 = t_agent.postprocess_key(
            APICommander._decimal_aware_parse_json_response(BASELINE_KEY_STR),  # type: ignore[arg-type]
            primary_key_schema_dict=BASELINE_COLUMNS,
        )[1]
        assert _repaint_NaNs(baseline_kobj_2) == _repaint_NaNs(BASELINE_OBJ)
        # with-decimals, keys (decode only)
        wdecs_kobj_2 = t_agent.postprocess_key(
            APICommander._decimal_aware_parse_json_response(WDECS_KEY_STR),  # type: ignore[arg-type]
            primary_key_schema_dict=WDECS_OBJ_COLUMNS,
        )[1]
        assert _repaint_NaNs(wdecs_kobj_2) == _repaint_NaNs(WDECS_OBJ)

    @pytest.mark.parametrize(
        ("colltype_custom", "colltype_obj", "colltype_columns"),
        COLLTYPES_CUSTOM_OBJ_SCHEMA_TRIPLES,
        ids=COLLTYPES_TRIPLE_IDS,
    )
    @pytest.mark.describe(
        "test of decimal-related conversions in table codec paths, collection types"
    )
    def test_decimalsupport_table_collectiontypes_codecpath(
        self,
        colltype_custom: bool,
        colltype_obj: dict[str, Any],
        colltype_columns: dict[str, Any],
    ) -> None:
        t_agent: _TableConverterAgent[DefaultRowType] = _TableConverterAgent(
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=colltype_custom,
                unroll_iterables_to_lists=True,
                use_decimals_in_collections=True,
                accept_naive_datetimes=False,
                datetime_tzinfo=None,
            ),
        )

        fully_encoded = APICommander._decimal_aware_encode_payload(
            t_agent.preprocess_payload(colltype_obj)
        )
        colltype_obj_2 = t_agent.postprocess_row(
            APICommander._decimal_aware_parse_json_response(fully_encoded),  # type: ignore[arg-type]
            columns_dict=colltype_columns,
            similarity_pseudocolumn=None,
        )
        assert _repaint_NaNs(colltype_obj_2) == _repaint_NaNs(colltype_obj)
