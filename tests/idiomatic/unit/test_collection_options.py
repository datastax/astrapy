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

from __future__ import annotations

from typing import Any

import pytest

from astrapy.info import CollectionDescriptor


@pytest.mark.describe("test of recasting the collection options from the api")
def test_recast_api_collection_dict() -> None:
    api_coll_descs: list[tuple[dict[str, Any], dict[str, Any]]] = [
        # minimal:
        (
            {
                "name": "col_name",
            },
            {"name": "col_name"},
        ),
        # full, w/o service:
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                        "service": {
                            "provider": "nvidia",
                            "modelName": "NV-Embed-QA",
                        },
                    },
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
                "service": {
                    "provider": "nvidia",
                    "modelName": "NV-Embed-QA",
                },
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                    },
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
            },
        ),
        # partial/absent 'vector', w/o service:
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "metric": "cosine",
                    },
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "metric": "cosine",
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "metric": "cosine",
                        "service": {
                            "provider": "nvidia",
                            "modelName": "NV-Embed-QA",
                        },
                    },
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "metric": "cosine",
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
                "service": {
                    "provider": "nvidia",
                    "modelName": "NV-Embed-QA",
                },
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                    },
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "service": {
                            "provider": "nvidia",
                            "modelName": "NV-Embed-QA",
                        },
                    },
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
                "service": {
                    "provider": "nvidia",
                    "modelName": "NV-Embed-QA",
                },
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {},
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "service": {
                            "provider": "nvidia",
                            "modelName": "NV-Embed-QA",
                        },
                    },
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
                "service": {
                    "provider": "nvidia",
                    "modelName": "NV-Embed-QA",
                },
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "indexing": {"deny": ["a"]},
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "indexing": {"deny": ["a"]},
                "default_id_type": "objectId",
            },
        ),
        # no indexing:
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                    },
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
                "default_id_type": "objectId",
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                        "service": {
                            "provider": "nvidia",
                            "modelName": "NV-Embed-QA",
                        },
                    },
                    "defaultId": {"type": "objectId"},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
                "default_id_type": "objectId",
                "service": {
                    "provider": "nvidia",
                    "modelName": "NV-Embed-QA",
                },
            },
        ),
        # no defaultId:
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                    },
                    "indexing": {"deny": ["a"]},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
                "indexing": {"deny": ["a"]},
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                        "service": {
                            "provider": "nvidia",
                            "modelName": "NV-Embed-QA",
                        },
                    },
                    "indexing": {"deny": ["a"]},
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
                "indexing": {"deny": ["a"]},
                "service": {
                    "provider": "nvidia",
                    "modelName": "NV-Embed-QA",
                },
            },
        ),
        # no indexing + no defaultId:
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                    },
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
            },
        ),
        (
            {
                "name": "col_name",
                "options": {
                    "vector": {
                        "dimension": 1024,
                        "metric": "cosine",
                        "service": {
                            "provider": "nvidia",
                            "modelName": "NV-Embed-QA",
                        },
                    },
                },
            },
            {
                "name": "col_name",
                "dimension": 1024,
                "metric": "cosine",
                "service": {
                    "provider": "nvidia",
                    "modelName": "NV-Embed-QA",
                },
            },
        ),
    ]
    for api_coll_desc, flattened_dict in api_coll_descs:
        assert CollectionDescriptor.from_dict(api_coll_desc).as_dict() == api_coll_desc
        assert CollectionDescriptor.from_dict(api_coll_desc).flatten() == flattened_dict
