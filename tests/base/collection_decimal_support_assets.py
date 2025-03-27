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

from astrapy.utils.api_options import FullSerdesOptions

S_OPTS_NO_DECS = FullSerdesOptions(
    binary_encode_vectors=False,
    custom_datatypes_in_reading=True,
    unroll_iterables_to_lists=True,
    use_decimals_in_collections=False,
    encode_maps_as_lists_in_tables="NEVER",
    accept_naive_datetimes=False,
    datetime_tzinfo=None,
)
S_OPTS_OK_DECS = FullSerdesOptions(
    binary_encode_vectors=False,
    custom_datatypes_in_reading=True,
    unroll_iterables_to_lists=True,
    use_decimals_in_collections=True,
    encode_maps_as_lists_in_tables="NEVER",
    accept_naive_datetimes=False,
    datetime_tzinfo=None,
)
_BASELINE_SCALAR_CASES = {
    "_id": "baseline",
    "f": 1.23,
    "i": 123,
    "t": "T",
}
_W_DECIMALS_SCALAR_CASES = {
    "_id": "decimals",
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
WDECS_OBJ = {
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
