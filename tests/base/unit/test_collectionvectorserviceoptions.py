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

from astrapy.info import VectorServiceOptions


class TestVectorServiceOptions:
    @pytest.mark.describe("test of VectorServiceOptions conversions, base")
    def test_VectorServiceOptions_conversions_base(self) -> None:
        dict0 = {
            "provider": "PRO",
            "modelName": "MOD",
        }
        from_dict0 = VectorServiceOptions._from_dict(dict0)
        assert from_dict0 == VectorServiceOptions(
            provider="PRO",
            model_name="MOD",
        )
        assert from_dict0.as_dict() == dict0

    @pytest.mark.describe("test of VectorServiceOptions conversions, with auth")
    def test_VectorServiceOptions_conversions_auth(self) -> None:
        dict_a = {
            "provider": "PRO",
            "modelName": "MOD",
            "authentication": {
                "type": ["A_T"],
                "field": "value",
            },
        }
        from_dict_a = VectorServiceOptions._from_dict(dict_a)
        assert from_dict_a == VectorServiceOptions(
            provider="PRO",
            model_name="MOD",
            authentication={
                "type": ["A_T"],
                "field": "value",
            },
        )
        assert from_dict_a.as_dict() == dict_a

    @pytest.mark.describe("test of VectorServiceOptions conversions, with params")
    def test_VectorServiceOptions_conversions_params(self) -> None:
        dict_p = {
            "provider": "PRO",
            "modelName": "MOD",
            "parameters": {
                "field1": "value1",
                "field2": 12.3,
                "field3": 123,
                "field4": True,
                "field5": None,
                "field6": {"a": 1},
            },
        }
        from_dict_p = VectorServiceOptions._from_dict(dict_p)
        assert from_dict_p == VectorServiceOptions(
            provider="PRO",
            model_name="MOD",
            parameters={
                "field1": "value1",
                "field2": 12.3,
                "field3": 123,
                "field4": True,
                "field5": None,
                "field6": {"a": 1},
            },
        )
        assert from_dict_p.as_dict() == dict_p

    @pytest.mark.describe(
        "test of VectorServiceOptions conversions, with params and auth"
    )
    def test_VectorServiceOptions_conversions_params_auth(self) -> None:
        dict_ap = {
            "provider": "PRO",
            "modelName": "MOD",
            "authentication": {
                "type": ["A_T"],
                "field": "value",
            },
            "parameters": {
                "field1": "value1",
                "field2": 12.3,
                "field3": 123,
                "field4": True,
                "field5": None,
                "field6": {"a": 1},
            },
        }
        from_dict_ap = VectorServiceOptions._from_dict(dict_ap)
        assert from_dict_ap == VectorServiceOptions(
            provider="PRO",
            model_name="MOD",
            authentication={
                "type": ["A_T"],
                "field": "value",
            },
            parameters={
                "field1": "value1",
                "field2": 12.3,
                "field3": 123,
                "field4": True,
                "field5": None,
                "field6": {"a": 1},
            },
        )
        assert from_dict_ap.as_dict() == dict_ap
