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
Unit tests for the ObjectIds and UUIDn conversions
"""

import json
import uuid as lib_uuid

import bson as lib_bson
import pytest
import uuid6 as lib_uuid6

from astrapy.core.ids import (
    UUID,
    ObjectId,
    uuid1,
    uuid3,
    uuid4,
    uuid5,
    uuid6,
    uuid7,
    uuid8,
)
from astrapy.core.utils import normalize_for_api, restore_from_api


@pytest.mark.describe("test wrapper for uuids matches external libraries")
def test_uuid_wrappers() -> None:
    lib_namespace = lib_uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
    lib_u1 = lib_uuid.uuid1()
    lib_u3 = lib_uuid.uuid3(lib_namespace, "abc")
    lib_u4 = lib_uuid.uuid4()
    lib_u5 = lib_uuid.uuid5(lib_namespace, "abc")
    lib_u6 = lib_uuid6.uuid6()
    lib_u7 = lib_uuid6.uuid7()
    lib_u8 = lib_uuid6.uuid8()

    apy_namespace = UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
    apy_u1 = uuid1()
    apy_u3 = uuid3(apy_namespace, "abc")
    apy_u4 = uuid4()
    apy_u5 = uuid5(apy_namespace, "abc")
    apy_u6 = uuid6()
    apy_u7 = uuid7()
    apy_u8 = uuid8()

    assert isinstance(lib_u1, UUID)
    assert isinstance(lib_u3, UUID)
    assert isinstance(lib_u4, UUID)
    assert isinstance(lib_u5, UUID)
    assert isinstance(lib_u6, UUID)
    assert isinstance(lib_u7, UUID)
    assert isinstance(lib_u8, UUID)
    assert isinstance(apy_u1, UUID)
    assert isinstance(apy_u3, UUID)
    assert isinstance(apy_u4, UUID)
    assert isinstance(apy_u5, UUID)
    assert isinstance(apy_u6, UUID)
    assert isinstance(apy_u7, UUID)
    assert isinstance(apy_u8, UUID)

    lib_f_u1 = lib_uuid.UUID("8ccd6ff8-e61b-11ee-a2fc-7df4a8c4164b")  # uuid1
    lib_f_u3 = lib_uuid.UUID("6fa459ea-ee8a-3ca4-894e-db77e160355e")  # uuid3
    lib_f_u4 = lib_uuid.UUID("4f16cba8-1115-43ab-aa39-3a9c29f37db5")  # uuid4
    lib_f_u5 = lib_uuid.UUID("886313e1-3b8a-5372-9b90-0c9aee199e5d")  # uuid5
    lib_f_u6 = lib_uuid.UUID("1eee61b9-8f2d-69ad-8ebb-5054d2a1a2c0")  # uuid6
    lib_f_u7 = lib_uuid.UUID("018e57e5-f586-7ed6-be55-6b0de3041116")  # uuid7
    lib_f_u8 = lib_uuid.UUID("018e57e5-fbcd-8bd4-b794-be914f2c4c85")  # uuid8
    apy_f_u1 = UUID("8ccd6ff8-e61b-11ee-a2fc-7df4a8c4164b")  # uuid1
    apy_f_u3 = UUID("6fa459ea-ee8a-3ca4-894e-db77e160355e")  # uuid3
    apy_f_u4 = UUID("4f16cba8-1115-43ab-aa39-3a9c29f37db5")  # uuid4
    apy_f_u5 = UUID("886313e1-3b8a-5372-9b90-0c9aee199e5d")  # uuid5
    apy_f_u6 = UUID("1eee61b9-8f2d-69ad-8ebb-5054d2a1a2c0")  # uuid6
    apy_f_u7 = UUID("018e57e5-f586-7ed6-be55-6b0de3041116")  # uuid7
    apy_f_u8 = UUID("018e57e5-fbcd-8bd4-b794-be914f2c4c85")  # uuid8

    assert lib_f_u1 == apy_f_u1
    assert lib_f_u3 == apy_f_u3
    assert lib_f_u4 == apy_f_u4
    assert lib_f_u5 == apy_f_u5
    assert lib_f_u6 == apy_f_u6
    assert lib_f_u7 == apy_f_u7
    assert lib_f_u8 == apy_f_u8

    assert apy_f_u1.version == 1
    assert apy_f_u3.version == 3
    assert apy_f_u4.version == 4
    assert apy_f_u5.version == 5
    assert apy_f_u6.version == 6
    assert apy_f_u7.version == 7
    assert apy_f_u8.version == 8


@pytest.mark.describe("test wrapper for ObjectId matches external library")
def test_objectid_wrapper() -> None:
    lib_oi = lib_bson.objectid.ObjectId()

    apy_oi = ObjectId()

    assert isinstance(lib_oi, ObjectId)
    assert isinstance(apy_oi, ObjectId)

    lib_f_oi = lib_bson.objectid.ObjectId("65f9cfa0d7fabb3f255c25a1")
    apy_f_oi = ObjectId("65f9cfa0d7fabb3f255c25a1")

    assert lib_f_oi == apy_f_oi


@pytest.mark.describe("test ser/des for UUIDs and ObjectId")
def test_core_ids_serdes() -> None:
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
        "list": [
            f_u1,
            f_u3,
            f_u4,
            f_u5,
            f_u6,
            f_u7,
            f_u8,
            f_oi,
        ],
        "nested": {
            "list": [
                {"f_u1": f_u1},
                {"f_u3": f_u3},
                {"f_u4": f_u4},
                {"f_u5": f_u5},
                {"f_u6": f_u6},
                {"f_u7": f_u7},
                {"f_u8": f_u8},
                {"f_oi": f_oi},
            ]
        },
    }

    normalized = normalize_for_api(full_structure)
    json.dumps(normalized)
    assert normalized is not None
    restored = restore_from_api(normalized)
    assert restored == full_structure
