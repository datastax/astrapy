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

import os
import sys
from typing import Any

import pytest

from astrapy import Database
from astrapy.authentication import AWSEmbeddingHeadersProvider, EmbeddingHeadersProvider
from astrapy.exceptions import DataAPIResponseException, InsertManyException
from astrapy.info import CollectionVectorServiceOptions

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ..conftest import IS_ASTRA_DB
from ..vectorize_models import live_test_models


def enabled_vectorize_models(auth_type: str) -> list[Any]:
    """
    This actually returns a List of `_pytest.mark.structures.ParameterSet` instances,
    each wrapping a dict with the needed info to test the model

    The tested models can also be further restricted by:
        EMBEDDING_MODEL_TAGS="tag1,tag2..." [...pytest command...]
    where `tag` = "provider/model/auth_type/[0 or f]"
    """
    all_test_models = list(live_test_models())
    all_model_ids: list[str] = [
        str(model_desc["model_tag"]) for model_desc in all_test_models
    ]
    #
    at_test_models = [
        test_model
        for test_model in all_test_models
        if test_model["auth_type_name"] == auth_type
    ]
    at_model_ids: list[str] = [
        str(model_desc["model_tag"]) for model_desc in at_test_models
    ]
    at_chosen_models: list[Any] = []
    if "EMBEDDING_MODEL_TAGS" in os.environ:
        whitelisted_models = [
            _wmd.strip()
            for _wmd in os.environ["EMBEDDING_MODEL_TAGS"].split(",")
            if _wmd.strip()
        ]
    else:
        whitelisted_models = at_model_ids
    unknown_wl_models = [wmd for wmd in whitelisted_models if wmd not in all_model_ids]
    if unknown_wl_models:
        raise ValueError(f"Unknown whitelisted model(s): {','.join(unknown_wl_models)}")

    for model in at_test_models:
        markers = []
        # provider exclusion logic applied here:
        if model["model_tag"] not in whitelisted_models:
            markers.append(pytest.mark.skip(reason="model not whitelisted"))
        at_chosen_models.append(pytest.param(model, marks=markers))
    return at_chosen_models


class TestVectorizeProviders:
    @pytest.mark.parametrize(
        "testable_vectorize_model",
        enabled_vectorize_models(auth_type="HEADER"),
        ids=[
            str(model["model_tag"])
            for model in live_test_models()
            if model["auth_type_name"] == "HEADER"
        ],
    )
    @pytest.mark.describe(
        "test of vectorize collection basic usage with header key, sync"
    )
    def test_vectorize_usage_auth_type_header_sync(
        self,
        sync_database: Database,
        testable_vectorize_model: dict[str, Any],
    ) -> None:
        simple_tag = testable_vectorize_model["simple_tag"].lower()
        # switch betewen header providers according to what is needed
        # For the time being this is necessary on HEADER only
        embedding_api_key: str | EmbeddingHeadersProvider
        at_tokens = testable_vectorize_model["auth_type_tokens"]
        at_token_lnames = {tk.accepted.lower() for tk in at_tokens}
        if at_token_lnames == {"x-embedding-api-key"}:
            embedding_api_key = os.environ[
                f"HEADER_EMBEDDING_API_KEY_{testable_vectorize_model['secret_tag']}"
            ]
        elif at_token_lnames == {"x-embedding-access-id", "x-embedding-secret-id"}:
            embedding_api_key = AWSEmbeddingHeadersProvider(
                embedding_access_id=os.environ[
                    f"HEADER_EMBEDDING_ACCESS_ID_{testable_vectorize_model['secret_tag']}"
                ],
                embedding_secret_id=os.environ[
                    f"HEADER_EMBEDDING_SECRET_ID_{testable_vectorize_model['secret_tag']}"
                ],
            )
        else:
            raise ValueError(
                f"Unsupported auth type tokens for {testable_vectorize_model['model_tag']}"
            )
        dimension = testable_vectorize_model.get("dimension")
        service_options = testable_vectorize_model["service_options"]

        db = sync_database
        collection_name = f"vec_h_{simple_tag}"
        # create with header
        try:
            collection = db.create_collection(
                collection_name,
                dimension=dimension,
                metric="cosine",
                service=service_options,
                embedding_api_key=embedding_api_key,
            )
            # this is to cope with the Data API normalizing options
            expected_service_options = CollectionVectorServiceOptions(
                provider=service_options.provider,
                model_name=testable_vectorize_model.get(
                    "expected_model_name", service_options.model_name
                ),
                parameters=service_options.parameters,
            )
            # test service back from collection info
            c_descriptors = [
                cd for cd in db.list_collections() if cd.name == collection_name
            ]
            assert len(c_descriptors) == 1
            c_descriptor = c_descriptors[0]
            assert c_descriptor.options is not None
            assert c_descriptor.options.vector is not None
            assert c_descriptor.options.vector.service == expected_service_options
            # put entries
            test_assets = testable_vectorize_model["test_assets"]
            if testable_vectorize_model["use_insert_one"]:
                for test_sample_tag, test_sample_text in test_assets["samples"]:
                    collection.insert_one(
                        {"tag": test_sample_tag, "$vectorize": test_sample_text},
                    )
                # also test for an error if inserting many
                with pytest.raises(InsertManyException):
                    collection.insert_many(
                        [
                            {"tag": tag, "$vectorize": text}
                            for tag, text in test_assets["samples"]
                        ],
                    )
            else:
                collection.insert_many(
                    [
                        {"tag": tag, "$vectorize": text}
                        for tag, text in test_assets["samples"]
                    ],
                )
            # instantiate with header
            collection_i = db.get_collection(
                collection_name,
                embedding_api_key=embedding_api_key,
            )
            # run ANN and check results
            hits = [
                document["tag"]
                for document in collection_i.find(
                    sort={"$vectorize": test_assets["probe"]["text"]},
                    limit=len(test_assets["probe"]["expected"]),
                    projection=["tag"],
                )
            ]
            assert hits == test_assets["probe"]["expected"]
        finally:
            db.drop_collection(collection_name)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.parametrize(
        "testable_vectorize_model",
        enabled_vectorize_models(auth_type="NONE"),
        ids=[
            str(model["model_tag"])
            for model in live_test_models()
            if model["auth_type_name"] == "NONE"
        ],
    )
    @pytest.mark.describe("test of vectorize collection basic usage with no auth, sync")
    def test_vectorize_usage_auth_type_none_sync(
        self,
        sync_database: Database,
        testable_vectorize_model: dict[str, Any],
    ) -> None:
        simple_tag = testable_vectorize_model["simple_tag"].lower()
        dimension = testable_vectorize_model.get("dimension")
        service_options = testable_vectorize_model["service_options"]

        db = sync_database
        collection_name = f"vec_n_{simple_tag}"
        # create with header
        try:
            collection = db.create_collection(
                collection_name,
                dimension=dimension,
                metric="cosine",
                service=service_options,
            )
            # this is to cope with the Data API normalizing options
            expected_service_options = CollectionVectorServiceOptions(
                provider=service_options.provider,
                model_name=testable_vectorize_model.get(
                    "expected_model_name", service_options.model_name
                ),
                parameters=service_options.parameters,
            )
            # test service back from collection info
            c_descriptors = [
                cd for cd in db.list_collections() if cd.name == collection_name
            ]
            assert len(c_descriptors) == 1
            c_descriptor = c_descriptors[0]
            assert c_descriptor.options is not None
            assert c_descriptor.options.vector is not None
            assert c_descriptor.options.vector.service == expected_service_options
            # put entries
            test_assets = testable_vectorize_model["test_assets"]
            if testable_vectorize_model["use_insert_one"]:
                for test_sample_tag, test_sample_text in test_assets["samples"]:
                    collection.insert_one(
                        {"tag": test_sample_tag, "$vectorize": test_sample_text},
                    )
            else:
                collection.insert_many(
                    [
                        {"tag": tag, "$vectorize": text}
                        for tag, text in test_assets["samples"]
                    ],
                )
            # instantiate collection
            collection_i = db.get_collection(
                collection_name,
            )
            # run ANN and check results
            hits = [
                document["tag"]
                for document in collection_i.find(
                    sort={"$vectorize": test_assets["probe"]["text"]},
                    limit=len(test_assets["probe"]["expected"]),
                    projection=["tag"],
                )
            ]
            assert hits == test_assets["probe"]["expected"]
        finally:
            db.drop_collection(collection_name)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.parametrize(
        "testable_vectorize_model",
        enabled_vectorize_models(auth_type="SHARED_SECRET"),
        ids=[
            str(model["model_tag"])
            for model in live_test_models()
            if model["auth_type_name"] == "SHARED_SECRET"
        ],
    )
    @pytest.mark.describe(
        "test of vectorize collection basic usage with shared_secret, sync"
    )
    def test_vectorize_usage_auth_type_shared_secret_sync(
        self,
        sync_database: Database,
        testable_vectorize_model: dict[str, Any],
    ) -> None:
        simple_tag = testable_vectorize_model["simple_tag"].lower()
        secret_tag = testable_vectorize_model["secret_tag"]
        dimension = testable_vectorize_model.get("dimension")
        base_service_options = testable_vectorize_model["service_options"]

        db = sync_database
        collection_name = f"vec_s_{simple_tag}"
        # to create the collection, prepare a service_options with the auth info
        service_options = CollectionVectorServiceOptions(
            provider=base_service_options.provider,
            model_name=base_service_options.model_name,
            authentication={
                "providerKey": f"SHARED_SECRET_EMBEDDING_API_KEY_{secret_tag}",
            },
            parameters=base_service_options.parameters,
        )
        # this is to cope with the Data API normalizing options
        expected_service_options = CollectionVectorServiceOptions(
            provider=base_service_options.provider,
            model_name=testable_vectorize_model.get(
                "expected_model_name", service_options.model_name
            ),
            authentication={
                "providerKey": f"SHARED_SECRET_EMBEDDING_API_KEY_{secret_tag}.providerKey",
            },
            parameters=base_service_options.parameters,
        )
        try:
            collection = db.create_collection(
                collection_name,
                dimension=dimension,
                metric="cosine",
                service=service_options,
            )
            # test service back from collection info
            c_descriptors = [
                cd for cd in db.list_collections() if cd.name == collection_name
            ]
            assert len(c_descriptors) == 1
            c_descriptor = c_descriptors[0]
            assert c_descriptor.options is not None
            assert c_descriptor.options.vector is not None
            assert c_descriptor.options.vector.service == expected_service_options
            # put entries
            test_assets = testable_vectorize_model["test_assets"]
            if testable_vectorize_model["use_insert_one"]:
                for test_sample_tag, test_sample_text in test_assets["samples"]:
                    collection.insert_one(
                        {"tag": test_sample_tag, "$vectorize": test_sample_text},
                    )
            else:
                collection.insert_many(
                    [
                        {"tag": tag, "$vectorize": text}
                        for tag, text in test_assets["samples"]
                    ],
                )
            # instantiate collection
            collection_i = db.get_collection(
                collection_name,
            )
            # run ANN and check results
            hits = [
                document["tag"]
                for document in collection_i.find(
                    sort={"$vectorize": test_assets["probe"]["text"]},
                    limit=len(test_assets["probe"]["expected"]),
                    projection=["tag"],
                )
            ]
            assert hits == test_assets["probe"]["expected"]

            # test that header overrides shared secret:
            faulty_collection_i = db.get_collection(
                collection_name,
                embedding_api_key="clearly-not-a-working-secret",
            )
            with pytest.raises(DataAPIResponseException):
                faulty_collection_i.find_one(sort={"$vectorize": "Breaking sentence"})
        finally:
            db.drop_collection(collection_name)
