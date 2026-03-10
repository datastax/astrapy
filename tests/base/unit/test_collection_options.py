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

import warnings
from typing import Any

import pytest
from deprecation import DeprecatedWarning

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
                        "modelName": "nvidia/nv-embedqa-e5-v5",
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
                        "modelName": "nvidia/nv-embedqa-e5-v5",
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
                        "modelName": "nvidia/nv-embedqa-e5-v5",
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
                        "modelName": "nvidia/nv-embedqa-e5-v5",
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
                        "modelName": "nvidia/nv-embedqa-e5-v5",
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
                        "modelName": "nvidia/nv-embedqa-e5-v5",
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
                        "modelName": "nvidia/nv-embedqa-e5-v5",
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
    zero = CollectionDefinition.builder()
    assert zero.build().as_dict() == {}

    rich = (
        zero.with_indexing("allow", ["a", "b"])
        .with_default_id("UUID")
        .with_vector_dimension(123)
        .with_vector_metric("cosine")
        .with_vector_service(
            "prov", "mod", authentication={"a": "u"}, parameters={"p": "a"}
        )
        .build()
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
        rich.with_indexing(None)
        .with_default_id(None)
        .with_vector_dimension(None)
        .with_vector_metric(None)
        .with_vector_source_model(None)
        .with_vector_service(None)
        .build()
    )
    assert zero_2.build().as_dict() == {}


@pytest.mark.describe(
    "test that deprecated set_* aliases emit DeprecatedWarning and produce correct results"
)
def test_deprecated_set_aliases_sync() -> None:
    zero = CollectionDefinition.builder()

    deprecated_calls: list[tuple[str, CollectionDefinition]] = []
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        deprecated_calls.append(
            ("set_indexing", zero.set_indexing("allow", ["a"]))
        )
        deprecated_calls.append(
            ("set_default_id", zero.set_default_id("UUID"))
        )
        deprecated_calls.append(
            ("set_vector_dimension", zero.set_vector_dimension(123))
        )
        deprecated_calls.append(
            ("set_vector_metric", zero.set_vector_metric("cosine"))
        )
        deprecated_calls.append(
            ("set_vector_source_model", zero.set_vector_source_model("other"))
        )
        deprecated_calls.append(
            ("set_vector_service", zero.set_vector_service("prov", "mod"))
        )
        deprecated_calls.append(
            ("set_rerank", zero.set_rerank("nvidia", "model"))
        )
        deprecated_calls.append(
            ("set_lexical", zero.set_lexical("STANDARD"))
        )

    # Every call should have emitted exactly one DeprecatedWarning
    dep_warnings = [w for w in caught if issubclass(w.category, DeprecatedWarning)]
    assert len(dep_warnings) == len(deprecated_calls)
    for w, (method_name, _) in zip(dep_warnings, deprecated_calls):
        assert method_name in str(w.message)

    # Results should be identical to the with_* equivalents
    assert deprecated_calls[0][1].build().as_dict() == zero.with_indexing("allow", ["a"]).build().as_dict()
    assert deprecated_calls[1][1].build().as_dict() == zero.with_default_id("UUID").build().as_dict()
    assert deprecated_calls[2][1].build().as_dict() == zero.with_vector_dimension(123).build().as_dict()
    assert deprecated_calls[3][1].build().as_dict() == zero.with_vector_metric("cosine").build().as_dict()
    assert deprecated_calls[4][1].build().as_dict() == zero.with_vector_source_model("other").build().as_dict()
    assert deprecated_calls[5][1].build().as_dict() == zero.with_vector_service("prov", "mod").build().as_dict()
    assert deprecated_calls[6][1].build().as_dict() == zero.with_rerank("nvidia", "model").build().as_dict()
    assert deprecated_calls[7][1].build().as_dict() == zero.with_lexical("STANDARD").build().as_dict()
