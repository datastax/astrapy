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

import pytest

from astrapy.info import CollectionVectorServiceOptions


class TestCollectionVectorServiceOptions:
    @pytest.mark.describe("test of CollectionVectorServiceOptions conversions, base")
    def test_collectionvectorserviceoptions_conversions_base(self) -> None:
        dict0 = {
            "provider": "PRO",
            "modelName": "MOD",
        }
        from_dict0 = CollectionVectorServiceOptions.from_dict(dict0)
        assert from_dict0 == CollectionVectorServiceOptions(
            provider="PRO",
            model_name="MOD",
        )
        assert from_dict0.as_dict() == dict0

    @pytest.mark.describe(
        "test of CollectionVectorServiceOptions conversions, with auth"
    )
    def test_collectionvectorserviceoptions_conversions_auth(self) -> None:
        dict_a = {
            "provider": "PRO",
            "modelName": "MOD",
            "authentication": {
                "type": ["A_T"],
                "field": "value",
            },
        }
        from_dict_a = CollectionVectorServiceOptions.from_dict(dict_a)
        assert from_dict_a == CollectionVectorServiceOptions(
            provider="PRO",
            model_name="MOD",
            authentication={
                "type": ["A_T"],
                "field": "value",
            },
        )
        assert from_dict_a.as_dict() == dict_a

    @pytest.mark.describe(
        "test of CollectionVectorServiceOptions conversions, with params"
    )
    def test_collectionvectorserviceoptions_conversions_params(self) -> None:
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
        from_dict_p = CollectionVectorServiceOptions.from_dict(dict_p)
        assert from_dict_p == CollectionVectorServiceOptions(
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
        "test of CollectionVectorServiceOptions conversions, with params and auth"
    )
    def test_collectionvectorserviceoptions_conversions_params_auth(self) -> None:
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
        from_dict_ap = CollectionVectorServiceOptions.from_dict(dict_ap)
        assert from_dict_ap == CollectionVectorServiceOptions(
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
