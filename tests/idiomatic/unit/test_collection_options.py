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

from functools import partial

import pytest

from astrapy.database import _recast_api_collection_dict, _validate_create_collection_options
from astrapy.collection import EmbeddingService
from astrapy.constants import DefaultIdType, VectorMetric

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

@pytest.mark.describe("test of validation for create_collection options")
def test_validate_create_collection_options() -> None:
    _dimension = 100
    _metric = VectorMetric.COSINE
    _service = EmbeddingService(provider="provider", model_name="modelName")

    variables = [
        (None, {"a": 1}),
        (None, None),
        (DefaultIdType.UUID, {"a": 1}),
        (DefaultIdType.UUID, None),
    ]

    prt000 = partial(
        _validate_create_collection_options,
        dimension=None,
        metric=None,
        service=None,
    )
    for _did, _add in variables:
        prt000(
            default_id_type=_did,
            additional_options=_add,
        )

    prt001 = partial(
        _validate_create_collection_options,
        dimension=None,
        metric=None,
        service=_service,
    )
    for _did, _add in variables:
        prt001(
            default_id_type=_did,
            additional_options=_add,
        )

    prt010 = partial(
        _validate_create_collection_options,
        dimension=None,
        metric=_metric,
        service=None,
    )
    for _did, _add in variables:
        with pytest.raises(ValueError):
            prt010(
                default_id_type=_did,
                additional_options=_add,
            )

    prt011 = partial(
        _validate_create_collection_options,
        dimension=None,
        metric=_metric,
        service=_service,
    )
    for _did, _add in variables:
        prt011(
            default_id_type=_did,
            additional_options=_add,
        )

    prt100 = partial(
        _validate_create_collection_options,
        dimension=_dimension,
        metric=None,
        service=None,
    )
    for _did, _add in variables:
        prt100(
            default_id_type=_did,
            additional_options=_add,
        )

    prt101 = partial(
        _validate_create_collection_options,
        dimension=_dimension,
        metric=None,
        service=_service,
    )
    for _did, _add in variables:
        prt101(
            default_id_type=_did,
            additional_options=_add,
        )

    prt110 = partial(
        _validate_create_collection_options,
        dimension=_dimension,
        metric=_metric,
        service=None,
    )
    for _did, _add in variables:
        prt110(
            default_id_type=_did,
            additional_options=_add,
        )

    prt111 = partial(
        _validate_create_collection_options,
        dimension=_dimension,
        metric=_metric,
        service=_service,
    )
    for _did, _add in variables:
        prt111(
            default_id_type=_did,
            additional_options=_add,
        )
