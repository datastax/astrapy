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


TEST_MODELS = [
    {
        "model_tag": "nvidia",
        "secret_tag": "NVIDIA",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="nvidia",
            model_name="NV-Embed-QA",
        ),
        "enabled": False,
    },
    {
        "model_tag": "openai_ada002",
        "secret_tag": "OPENAI",
        "dimension": 1536,
        "service_options": CollectionVectorServiceOptions(
            provider="openai",
            model_name="text-embedding-ada-002",
        ),
        "enabled": True,
    },
    {
        "model_tag": "openai_3small",
        "secret_tag": "OPENAI",
        "dimension": 1536,
        "service_options": CollectionVectorServiceOptions(
            provider="openai",
            model_name="text-embedding-3-small",
        ),
        "enabled": True,
    },
    {
        "model_tag": "openai_3large",
        "secret_tag": "OPENAI",
        "dimension": 3072,
        "service_options": CollectionVectorServiceOptions(
            provider="openai",
            model_name="text-embedding-3-large",
        ),
        "enabled": True,
    },
    {
        "model_tag": "huggingface",
        "secret_tag": "HUGGINGFACE",
        "dimension": 384,
        "service_options": CollectionVectorServiceOptions(
            provider="huggingface",
            model_name="sentence-transformers/all-MiniLM-L6-v2",
        ),
        "enabled": True,
    },
    {
        "model_tag": "cohere_englishv2",
        "secret_tag": "COHERE",
        "dimension": 4096,
        "service_options": CollectionVectorServiceOptions(
            provider="cohere",
            model_name="embed-english-v2.0",
        ),
        "enabled": False,
        # 500 Internal Server Error
    },
    {
        "model_tag": "cohere_englishv3",
        "secret_tag": "COHERE",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="cohere",
            model_name="embed-english-v3.0",
        ),
        "enabled": False,
        # 500 Internal Server Error
    },
    # vertex?
    {
        "model_tag": "vertexai_gecko",
        "secret_tag": "VERTEXAI",
        "dimension": 768,
        "service_options": CollectionVectorServiceOptions(
            provider="vertexai",
            model_name="textembedding-gecko@003",
            parameters={
                "PROJECT_ID": os.environ.get("HEADER_EMBEDDING_VERTEXAI_PROJECT_ID"),
            },
        ),
        "enabled": False,
        # Server failed (when doing DML: it seems there's an unresolved template variable
    },
    #
    {
        "model_tag": "voyage_ai2",
        "secret_tag": "VOYAGEAI",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="voyageAI",
            model_name="voyage-2",
        ),
        "enabled": False,
        # The provided options are invalid: Service provider 'voyageAI' is not supported
    },
]
MODEL_IDS: List[str] = [str(model_desc["model_tag"]) for model_desc in TEST_MODELS]


def local_vectorize_models() -> List[Any]:
    """
    This actually returns a List of `_pytest.mark.structures.ParameterSet` instances,
    each wrapping a dict with fields:
        model_tag: str
        embedding_api_key: Optional[str]
        dimension: int
        service_options: CollectionVectorServiceOptions
    """
    models: List[Any] = []
    for model_desc in TEST_MODELS:
        secret_env_var_name = f"HEADER_EMBEDDING_API_KEY_{model_desc['secret_tag']}"
        model = {
            "model_tag": model_desc["model_tag"],
            "embedding_api_key": os.environ.get(secret_env_var_name),
            "dimension": model_desc["dimension"],
            "service_options": model_desc["service_options"],
        }
        markers = []
        if not model_desc["enabled"]:
            markers.append(pytest.mark.skip(reason="model disabled in code"))
        if model["embedding_api_key"] is None:
            markers.append(pytest.mark.skip(reason="no embedding secret in env"))
        models.append(pytest.param(model, marks=markers))
    return models


class TestLocalVectorizeByHeader:
    @pytest.mark.parametrize(
        "local_vectorize_model",
        local_vectorize_models(),
        ids=MODEL_IDS,
    )
    @pytest.mark.describe(
        "test of vectorize collection basic usage with header key, sync"
    )
    def test_vectorize_usage_by_header_sync(
        self,
        sync_local_database: Database,
        local_vectorize_model: Dict[str, Any],
    ) -> None:
        model_tag = local_vectorize_model["model_tag"]
        # reducing "" to None for key-less models (which one may want to test all the same)
        _embedding_api_key = local_vectorize_model["embedding_api_key"]
        embedding_api_key = _embedding_api_key if _embedding_api_key else None
        dimension = local_vectorize_model["dimension"]
        service_options = local_vectorize_model["service_options"]

        db = sync_local_database
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
            # put entries
            collection.insert_many(
                [{"tag": "joke"}, {"tag": "sport"}, {"tag": "math"}],
                vectorize=[
                    "Her rebuttal was enough to make her look like a moral winner!",
                    "The player won the game with a well-kicked penalty.",
                    "Now proceed by partial integration in the t variable...",
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
                    vectorize="Someone scored a point.", limit=2, projection=["tag"]
                )
            ]
            assert hits == ["sport", "joke"]
        finally:
            db.drop_collection(collection_name)
