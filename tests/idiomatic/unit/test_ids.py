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

"""
Unit tests for the ObjectIds and UUIDn conversions, 'idiomatic' imports
"""

from __future__ import annotations

import json

import pytest

from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.ids import UUID, ObjectId
from astrapy.utils.api_options import FullWireFormatOptions


@pytest.mark.describe("test of serdes for ids")
def test_ids_serdes() -> None:
    f_u1 = UUID("8ccd6ff8-e61b-11ee-a2fc-7df4a8c4164b")  # uuid1
    f_u3 = UUID("6fa459ea-ee8a-3ca4-894e-db77e160355e")  # uuid3
    f_u4 = UUID("4f16cba8-1115-43ab-aa39-3a9c29f37db5")  # uuid4
    f_u5 = UUID("886313e1-3b8a-5372-9b90-0c9aee199e5d")  # uuid5
    f_u6 = UUID("1eee61b9-8f2d-69ad-8ebb-5054d2a1a2c0")  # uuid6
    f_u7 = UUID("018e57e5-f586-7ed6-be55-6b0de3041116")  # uuid7
    f_u8 = UUID("018e57e5-fbcd-8bd4-b794-be914f2c4c85")  # uuid8
    f_oi = ObjectId("65f9cfa0d7fabb3f255c25a1")

    full_structure = {
        "f_u1": f_u1,
        "f_u3": f_u3,
        "f_u4": f_u4,
        "f_u5": f_u5,
        "f_u6": f_u6,
        "f_u7": f_u7,
        "f_u8": f_u8,
        "f_oi": f_oi,
    }

    normalized = preprocess_collection_payload(
        full_structure,
        options=FullWireFormatOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            coerce_iterables_to_vectors=True,
        ),
    )
    json.dumps(normalized)
    assert normalized is not None
    restored = postprocess_collection_response(
        normalized,
        options=FullWireFormatOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            coerce_iterables_to_vectors=True,
        ),
    )
    assert restored == full_structure
