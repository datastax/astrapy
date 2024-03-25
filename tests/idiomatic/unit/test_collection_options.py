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

from astrapy.database import _validate_create_collection_options
from astrapy.collection import EmbeddingService
from astrapy.constants import DefaultIdType, VectorMetric


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
