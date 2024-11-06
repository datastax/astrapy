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

from decimal import Decimal
from typing import Any

import pytest

from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import FullSerdesOptions

S_OPTS_NO_DECS = FullSerdesOptions(
    binary_encode_vectors=False,
    custom_datatypes_in_reading=True,
    unroll_iterables_to_lists=True,
    use_decimals_in_collections=False,
)
S_OPTS_OK_DECS = FullSerdesOptions(
    binary_encode_vectors=False,
    custom_datatypes_in_reading=True,
    unroll_iterables_to_lists=True,
    use_decimals_in_collections=True,
)
_BASELINE_SCALAR_CASES = {
    "f": 1.23,
    "i": 123,
    "t": "T",
}
_W_DECIMALS_SCALAR_CASES = {
    "f": 1.23,
    "i": 123,
    "t": "T",
    "de": Decimal("1.23"),
    "dfA": Decimal(1.23),
    "dfB": Decimal("1.229999999999999982236431605997495353221893310546875"),
}
BASELINE_OBJ = {
    **_BASELINE_SCALAR_CASES,
    "subdict": _BASELINE_SCALAR_CASES,
    "sublist": list(_BASELINE_SCALAR_CASES.values()),
}
OBJ_W_DECIMALS = {
    **_W_DECIMALS_SCALAR_CASES,
    "subdict": _W_DECIMALS_SCALAR_CASES,
    "sublist": list(_W_DECIMALS_SCALAR_CASES.values()),
}


def is_decimal_super(more_decs: dict[str, Any], less_decs: dict[str, Any]) -> bool:
    """
    Return True if the first item is "the same values, possibly made Decimal
    where the corresponding second item can be another number (int/float)".
    """
    if isinstance(more_decs, list):
        if not isinstance(less_decs, list):
            return False
        if len(more_decs) != len(less_decs):
            return False
        return all(
            [
                is_decimal_super(v_more, v_less)
                for v_more, v_less in zip(more_decs, less_decs)
            ]
        )
    elif isinstance(more_decs, dict):
        if not isinstance(less_decs, dict):
            return False
        if more_decs.keys() != less_decs.keys():
            return False
        return all(
            [is_decimal_super(v_more, less_decs[k]) for k, v_more in more_decs.items()]
        )
    else:
        # other scalars
        if isinstance(more_decs, Decimal):
            if isinstance(less_decs, Decimal):
                return more_decs == less_decs
            else:
                return float(more_decs) == float(less_decs)
        else:
            if isinstance(less_decs, Decimal):
                return False
            return more_decs == less_decs


class TestCollectionDecimalSupport:
    @pytest.mark.describe("test of decimals not supported by default in collections")
    def test_decimalsupport_collections_defaultsettings(self) -> None:
        # write path with baseline
        baseline_fully_encoded = APICommander._decimal_unaware_encode_payload(
            preprocess_collection_payload(
                BASELINE_OBJ,
                options=S_OPTS_NO_DECS,
            )
        )
        # read path back to it
        assert baseline_fully_encoded is not None
        baseline_obj_2 = postprocess_collection_response(
            APICommander._decimal_unaware_parse_json_response(
                baseline_fully_encoded,
            ),
            options=S_OPTS_NO_DECS,
        )
        # this must match exactly (baseline)
        assert BASELINE_OBJ == baseline_obj_2

        # write path with decimals should error instead
        with pytest.raises(TypeError):
            APICommander._decimal_unaware_encode_payload(
                preprocess_collection_payload(
                    OBJ_W_DECIMALS,
                    options=S_OPTS_NO_DECS,
                )
            )

    @pytest.mark.describe("test of decimals supported in collections if set to do so")
    def test_decimalsupport_collections_decimalsettings(self) -> None:
        # write path with baseline
        baseline_fully_encoded = APICommander._decimal_aware_encode_payload(
            preprocess_collection_payload(
                BASELINE_OBJ,
                options=S_OPTS_OK_DECS,
            )
        )
        # read path back to it
        assert baseline_fully_encoded is not None
        baseline_obj_2 = postprocess_collection_response(
            APICommander._decimal_aware_parse_json_response(
                baseline_fully_encoded,
            ),
            options=S_OPTS_OK_DECS,
        )
        # the re-read object must "be more-or-equally Decimal" than the source
        # but otherwise coincide
        assert is_decimal_super(baseline_obj_2, BASELINE_OBJ)

        # write path with decimals
        wdecs_fully_encoded = APICommander._decimal_aware_encode_payload(
            preprocess_collection_payload(
                OBJ_W_DECIMALS,
                options=S_OPTS_OK_DECS,
            )
        )
        # read path back to it
        assert wdecs_fully_encoded is not None
        wdecs_2 = postprocess_collection_response(
            APICommander._decimal_aware_parse_json_response(
                wdecs_fully_encoded,
            ),
            options=S_OPTS_OK_DECS,
        )
        # the re-read object must "be more-or-equally Decimal" than the source
        # but otherwise coincide
        assert is_decimal_super(wdecs_2, OBJ_W_DECIMALS)
