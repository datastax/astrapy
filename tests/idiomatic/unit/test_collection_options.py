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

from typing import Any, Dict, List

import pytest

from astrapy.info import CollectionDescriptor


@pytest.mark.describe("test of recasting the collection options from the api")
def test_recast_api_collection_dict() -> None:
    api_coll_descs: List[Dict[str, Any]] = [
        # minimal:
        {
            "name": "dvv",
        },
        # full:
        {
            "name": "dvv",
            "options": {
                "vector": {
                    "dimension": 1024,
                    "metric": "cosine",
                },
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
        # partial/absent 'vector':
        {
            "name": "dvv",
            "options": {
                "vector": {
                    "metric": "cosine",
                },
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
        {
            "name": "dvv",
            "options": {
                "vector": {
                    "dimension": 1024,
                },
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
        {
            "name": "dvv",
            "options": {
                "vector": {},
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
        {
            "name": "dvv",
            "options": {
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
        # no indexing:
        {
            "name": "dvv",
            "options": {
                "vector": {
                    "dimension": 1024,
                    "metric": "cosine",
                },
                "defaultId": {"type": "objectId"},
            },
        },
        # no defaultId:
        {
            "name": "dvv",
            "options": {
                "vector": {
                    "dimension": 1024,
                    "metric": "cosine",
                },
                "indexing": {"deny": ["a"]},
            },
        },
        # no indexing + no defaultId:
        {
            "name": "dvv",
            "options": {
                "vector": {
                    "dimension": 1024,
                    "metric": "cosine",
                },
            },
        },
    ]
    for api_coll_desc in api_coll_descs:
        assert CollectionDescriptor.from_dict(api_coll_desc).as_dict() == api_coll_desc
