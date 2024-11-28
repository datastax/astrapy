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

from astrapy.info import CollectionDefinition, CollectionDescriptor


@pytest.mark.describe(
    "test of recasting the collection definition options from the api"
)
def test_recast_api_collection_dict() -> None:
    api_coll_descs: list[dict[str, Any]] = [
        # minimal:
        {
            "name": "col_name",
        },
        # full, w/o service:
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
            "options": {
                "vector": {
                    "dimension": 1024,
                    "metric": "cosine",
                },
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
        # partial/absent "vector", w/o service:
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
            "options": {
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
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
            "options": {
                "indexing": {"deny": ["a"]},
                "defaultId": {"type": "objectId"},
            },
        },
        # no indexing:
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
        # no defaultId:
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
        # no indexing + no defaultId:
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
        # a real one (full)
        {
            "name": "testcoll",
            "options": {
                "vector": {
                    "dimension": 1024,
                    "metric": "cosine",
                    "service": {
                        "provider": "voyageAI",
                        "modelName": "voyage-large-2-instruct",
                        "authentication": {
                            "providerKey": "SHARED_SECRET_EMBEDDING_API_KEY_VOYAGEAI.providerKey",
                        },
                        "parameters": {
                            "autoTruncate": False,
                        },
                    },
                    "sourceModel": "other",
                },
                "indexing": {
                    "allow": [
                        "xing",
                    ],
                },
                "defaultId": {
                    "type": "objectId",
                },
            },
        },
    ]
    for cd_dict in api_coll_descs:
        descriptor = CollectionDescriptor._from_dict(cd_dict)
        # dict->obj->dict test
        assert descriptor.as_dict() == cd_dict
        if "options" in cd_dict:
            cdef_dict = cd_dict["options"]
            assert CollectionDefinition._from_dict(cdef_dict).as_dict() == cdef_dict
        # obj->dict->obj test
        assert CollectionDescriptor._from_dict(descriptor.as_dict()) == descriptor
        definition = descriptor.definition
        assert CollectionDefinition._from_dict(definition.as_dict()) == definition
        # coerce calls
        assert CollectionDescriptor.coerce(cd_dict) == descriptor
        assert CollectionDescriptor.coerce(descriptor) == descriptor
        if "options" in cd_dict:
            assert CollectionDefinition.coerce(cd_dict["options"]) == definition
        assert CollectionDefinition.coerce(definition) == definition


@pytest.mark.describe("test of fluent interface for CollectionDefinition")
def test_fluent_collection_definition() -> None:
    zero = CollectionDefinition.zero()
    assert zero.as_dict() == {}

    rich = (
        zero.set_indexing("allow", ["a", "b"])
        .set_default_id("UUID")
        .set_vector_dimension(123)
        .set_vector_metric("cosine")
        .set_vector_service(
            "prov", "mod", authentication={"a": "u"}, parameters={"p": "a"}
        )
    )
    expected_rich_dict = {
        "vector": {
            "dimension": 123,
            "metric": "cosine",
            "service": {
                "provider": "prov",
                "modelName": "mod",
                "authentication": {"a": "u"},
                "parameters": {"p": "a"},
            },
        },
        "indexing": {"allow": ["a", "b"]},
        "defaultId": {"type": "UUID"},
    }
    assert rich.as_dict() == expected_rich_dict

    zero_2 = (
        rich.set_indexing(None)
        .set_default_id(None)
        .set_vector_dimension(None)
        .set_vector_metric(None)
        .set_vector_source_model(None)
        .set_vector_service(None)
    )
    assert zero_2.as_dict() == {}
