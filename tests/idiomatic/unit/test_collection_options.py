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
Unit tests for the validation/parsing of collection options
"""

import pytest

from astrapy.database import _recast_api_collection_dict


@pytest.mark.describe("test of recasting the collection options from the api")
def test_recast_api_collection_dict() -> None:
    plain_raw = {
        "name": "tablename",
        "options": {},
    }
    plain_expected = {
        "name": "tablename",
    }
    assert _recast_api_collection_dict(plain_raw) == plain_expected

    plainplus_raw = {
        "name": "tablename",
        "options": {
            "futuretopfield": "ftvalue",
        },
    }
    plainplus_expected = {
        "name": "tablename",
        "additional_options": {
            "futuretopfield": "ftvalue",
        },
    }
    assert _recast_api_collection_dict(plainplus_raw) == plainplus_expected

    dim_raw = {
        "name": "tablename",
        "options": {
            "vector": {
                "dimension": 10,
            },
        },
    }
    dim_expected = {
        "name": "tablename",
        "dimension": 10,
    }
    assert _recast_api_collection_dict(dim_raw) == dim_expected

    dim_met_raw = {
        "name": "tablename",
        "options": {
            "vector": {
                "dimension": 10,
                "metric": "cosine",
            },
        },
    }
    dim_met_expected = {
        "name": "tablename",
        "dimension": 10,
        "metric": "cosine",
    }
    assert _recast_api_collection_dict(dim_met_raw) == dim_met_expected

    dim_met_did_idx_raw = {
        "name": "tablename",
        "options": {
            "defaultId": {"type": "objectId"},
            "indexing": {
                "allow": ["a"],
            },
            "vector": {
                "dimension": 10,
                "metric": "cosine",
            },
        },
    }
    dim_met_did_idx_expected = {
        "name": "tablename",
        "dimension": 10,
        "metric": "cosine",
        "indexing": {"allow": ["a"]},
        "default_id_type": "objectId",
    }
    assert _recast_api_collection_dict(dim_met_did_idx_raw) == dim_met_did_idx_expected

    dim_met_didplus_idx_raw = {
        "name": "tablename",
        "options": {
            "defaultId": {
                "type": "objectId",
                "futurefield": "fvalue",
            },
            "indexing": {
                "allow": ["a"],
            },
            "vector": {
                "dimension": 10,
                "metric": "cosine",
            },
        },
    }
    dim_met_didplus_idx_expected = {
        "name": "tablename",
        "dimension": 10,
        "metric": "cosine",
        "indexing": {"allow": ["a"]},
        "default_id_type": "objectId",
        "additional_options": {
            "defaultId": {"futurefield": "fvalue"},
        },
    }
    assert (
        _recast_api_collection_dict(dim_met_didplus_idx_raw)
        == dim_met_didplus_idx_expected
    )
