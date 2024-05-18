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

from typing import Any, Dict, List

import pytest

import os

from astrapy import Database
from astrapy.info import CollectionVectorServiceOptions
from astrapy.exceptions import InsertManyException

from ..vectorize_models import DEFAULT_TEST_ASSETS, TEST_MODELS
from ..conftest import env_filter_match


def enabled_vectorize_models(auth_type: str) -> List[Any]:
    """
    This actually returns a List of `_pytest.mark.structures.ParameterSet` instances,
    each wrapping a dict with fields:
        model_tag: str
        embedding_api_key: Optional[str]
        dimension: int
        service_options: CollectionVectorServiceOptions
    """
    model_ids: List[str] = [
        str(model_desc["model_tag"])
        for model_desc in TEST_MODELS
        if auth_type in model_desc["auth_types"]  # type: ignore[operator]
    ]
    models: List[Any] = []
    if f"{auth_type}_EMBEDDING_MODEL_TAGS" in os.environ:
        whitelisted_models = [
            _wmd.strip()
            for _wmd in os.environ[f"{auth_type}_EMBEDDING_MODEL_TAGS"].split(",")
            if _wmd.strip()
        ]
    else:
        whitelisted_models = model_ids
    unknown_wl_models = [wmd for wmd in whitelisted_models if wmd not in model_ids]
    if unknown_wl_models:
        raise ValueError(
            f"Unknown whitelisted model(s) for this auth type: {','.join(unknown_wl_models)}"
        )
    for model_desc in TEST_MODELS:
        if auth_type in model_desc["auth_types"]:  # type: ignore[operator]
            secret_env_var_name = f"HEADER_EMBEDDING_API_KEY_{model_desc['secret_tag']}"
            model = {
                "model_tag": model_desc["model_tag"],
                "secret_tag": model_desc["secret_tag"],
                "embedding_api_key": os.environ.get(secret_env_var_name),
                "dimension": model_desc["dimension"],
                "service_options": model_desc["service_options"],
                "test_assets": model_desc.get("test_assets", DEFAULT_TEST_ASSETS),
                "use_insert_one": model_desc.get("use_insert_one", False),
            }
            markers = []
            # provider exclusion logic applied here:
            env_filters = model_desc.get("env_filters", [("*", "*", "*")])
            if not env_filter_match(auth_type, env_filters):  # type: ignore[arg-type]
                markers.append(
                    pytest.mark.skip(reason="excluded by env/region/auth_type")
                )
            if model_desc["model_tag"] not in whitelisted_models:
                markers.append(pytest.mark.skip(reason="model not whitelisted"))
            if not model_desc["enabled"]:
                markers.append(pytest.mark.skip(reason="model disabled in code"))
            if auth_type == "HEADER" and model["embedding_api_key"] is None:
                markers.append(pytest.mark.skip(reason="no embedding secret in env"))
            models.append(pytest.param(model, marks=markers))
    return models


"""
The tested models can also be further restricted by:
    {HEADER,NONE...}_EMBEDDING_MODEL_TAGS="mt1,mt2" [...pytest command...]
"""


class TestVectorize:
    @pytest.mark.parametrize(
        "testable_vectorize_model",
        enabled_vectorize_models(auth_type="HEADER"),
        ids=[
            str(model["model_tag"])
            for model in TEST_MODELS
            if "HEADER" in model["auth_types"]  # type: ignore[operator]
        ],
    )
    @pytest.mark.describe(
        "test of vectorize collection basic usage with header key, sync"
    )
    def test_vectorize_usage_auth_type_header_sync(
        self,
        sync_database: Database,
        testable_vectorize_model: Dict[str, Any],
    ) -> None:
        model_tag = testable_vectorize_model["model_tag"]
        embedding_api_key = testable_vectorize_model["embedding_api_key"]
        dimension = testable_vectorize_model["dimension"]
        service_options = testable_vectorize_model["service_options"]

        db = sync_database
        collection_name = f"vectorize_by_header_{model_tag}"
        # create with header
        try:
            collection = db.create_collection(
                collection_name,
                dimension=dimension,
                metric="cosine",
                service=service_options,
                embedding_api_key=embedding_api_key,
            )
            # test service back from collection info
            c_descriptors = [
                cd for cd in db.list_collections() if cd.name == collection_name
            ]
            assert len(c_descriptors) == 1
            c_descriptor = c_descriptors[0]
            assert c_descriptor.options.vector.service == service_options
            # put entries
            test_assets = testable_vectorize_model["test_assets"]
            if testable_vectorize_model["use_insert_one"]:
                for test_sample_tag, test_sample_text in test_assets["samples"]:
                    collection.insert_one(
                        {"tag": test_sample_tag},
                        vectorize=test_sample_text,
                    )
                # also test for an error if inserting many
                with pytest.raises(InsertManyException):
                    collection.insert_many(
                        [{"tag": tag} for tag, _ in test_assets["samples"]],
                        vectorize=[text for _, text in test_assets["samples"]],
                    )
            else:
                collection.insert_many(
                    [{"tag": tag} for tag, _ in test_assets["samples"]],
                    vectorize=[text for _, text in test_assets["samples"]],
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
                    vectorize=test_assets["probe"]["text"],
                    limit=len(test_assets["probe"]["expected"]),
                    projection=["tag"],
                )
            ]
            assert hits == test_assets["probe"]["expected"]
        finally:
            db.drop_collection(collection_name)

    @pytest.mark.parametrize(
        "testable_vectorize_model",
        enabled_vectorize_models(auth_type="NONE"),
        ids=[
            str(model["model_tag"]) for model in TEST_MODELS if "NONE" in model["auth_types"]  # type: ignore[operator]
        ],
    )
    @pytest.mark.describe("test of vectorize collection basic usage with no auth, sync")
    def test_vectorize_usage_auth_type_none_sync(
        self,
        sync_database: Database,
        testable_vectorize_model: Dict[str, Any],
    ) -> None:
        model_tag = testable_vectorize_model["model_tag"]
        dimension = testable_vectorize_model["dimension"]
        service_options = testable_vectorize_model["service_options"]

        db = sync_database
        collection_name = f"vectorize_by_no_auth_{model_tag}"
        # create with header
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
            assert c_descriptor.options.vector.service == service_options
            # put entries
            test_assets = testable_vectorize_model["test_assets"]
            if testable_vectorize_model["use_insert_one"]:
                for test_sample_tag, test_sample_text in test_assets["samples"]:
                    collection.insert_one(
                        {"tag": test_sample_tag},
                        vectorize=test_sample_text,
                    )
            else:
                collection.insert_many(
                    [{"tag": tag} for tag, _ in test_assets["samples"]],
                    vectorize=[text for _, text in test_assets["samples"]],
                )
            # instantiate collection
            collection_i = db.get_collection(
                collection_name,
            )
            # run ANN and check results
            hits = [
                document["tag"]
                for document in collection_i.find(
                    vectorize=test_assets["probe"]["text"],
                    limit=len(test_assets["probe"]["expected"]),
                    projection=["tag"],
                )
            ]
            assert hits == test_assets["probe"]["expected"]
        finally:
            db.drop_collection(collection_name)

    @pytest.mark.parametrize(
        "testable_vectorize_model",
        enabled_vectorize_models(auth_type="SHARED_SECRET"),
        ids=[
            str(model["model_tag"]) for model in TEST_MODELS if "SHARED_SECRET" in model["auth_types"]  # type: ignore[operator]
        ],
    )
    @pytest.mark.describe(
        "test of vectorize collection basic usage with shared_secret, sync"
    )
    def test_vectorize_usage_auth_type_shared_secret_sync(
        self,
        sync_database: Database,
        testable_vectorize_model: Dict[str, Any],
    ) -> None:
        model_tag = testable_vectorize_model["model_tag"]
        secret_tag = testable_vectorize_model["secret_tag"]
        dimension = testable_vectorize_model["dimension"]
        base_service_options = testable_vectorize_model["service_options"]

        db = sync_database
        collection_name = f"vectorize_by_secret_{model_tag}"
        # to create the collection, prepare a service_options with the auth info
        service_options = CollectionVectorServiceOptions(
            provider=base_service_options.provider,
            model_name=base_service_options.model_name,
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
            assert c_descriptor.options.vector.service == service_options
            # put entries
            test_assets = testable_vectorize_model["test_assets"]
            if testable_vectorize_model["use_insert_one"]:
                for test_sample_tag, test_sample_text in test_assets["samples"]:
                    collection.insert_one(
                        {"tag": test_sample_tag},
                        vectorize=test_sample_text,
                    )
            else:
                collection.insert_many(
                    [{"tag": tag} for tag, _ in test_assets["samples"]],
                    vectorize=[text for _, text in test_assets["samples"]],
                )
            # instantiate collection
            collection_i = db.get_collection(
                collection_name,
            )
            # run ANN and check results
            hits = [
                document["tag"]
                for document in collection_i.find(
                    vectorize=test_assets["probe"]["text"],
                    limit=len(test_assets["probe"]["expected"]),
                    projection=["tag"],
                )
            ]
            assert hits == test_assets["probe"]["expected"]
        finally:
            pass
            db.drop_collection(collection_name)
