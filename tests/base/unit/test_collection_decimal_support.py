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

from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.utils.api_commander import APICommander

from ..collection_decimal_support_assets import (
    BASELINE_OBJ,
    S_OPTS_NO_DECS,
    S_OPTS_OK_DECS,
    WDECS_OBJ,
    is_decimal_super,
)


class TestCollectionDecimalSupportUnit:
    @pytest.mark.describe(
        "test of decimals not supported by default in collection codec paths"
    )
    def test_decimalsupport_collection_codecpath_defaultsettings(self) -> None:
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
                    WDECS_OBJ,
                    options=S_OPTS_NO_DECS,
                )
            )

    @pytest.mark.describe(
        "test of decimals supported in collection codec paths if set to do so"
    )
    def test_decimalsupport_collection_codecpath_decimalsettings(self) -> None:
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
                WDECS_OBJ,
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
        assert is_decimal_super(wdecs_2, WDECS_OBJ)
