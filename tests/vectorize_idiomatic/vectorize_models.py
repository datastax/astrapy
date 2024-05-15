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

import os

from astrapy.info import CollectionVectorServiceOptions


DEFAULT_TEST_ASSETS = {
    "samples": [
        (
            "car",
            "My car is fast and red, it's awesome.",
        ),
        (
            "landscape",
            "The landscape shows spiky, rocky mountaintops and a river nearby...",
        ),
        (
            "motorbike",
            "Your motorbike is certainly the best I have seen!",
        ),
    ],
    "probe": {
        "text": "Driving a red car is a joy.",
        "expected": ["car", "motorbike"],
    },
}
CODE_TEST_ASSETS = {
    "samples": [
        (
            "py_loop",
            "import os; for i in range(10): print(i + os.getenv('env'))",
        ),
        (
            "java",
            (
                "if (sortClause == null || sortClause.sortExpressions()."
                "isEmpty()){return Uni.createFrom().item(true);}"
            ),
        ),
        (
            "py_while",
            (
                "import json; val=json.load(open('limit.json')); "
                "v=0; while v<val: pprint(v); v = v + 1"
            ),
        ),
    ],
    "probe": {
        "text": "for(q in [10, 11, 12]): print(f'q={q}')",
        "expected": ["py_loop", "py_while"],
    },
}

TEST_MODELS = [
    {
        "model_tag": "azure_openai_textemb3large",
        "secret_tag": "AZURE_OPENAI",
        "dimension": 1234,
        "service_options": CollectionVectorServiceOptions(
            provider="azureOpenAI",
            model_name="text-embedding-3-large",
            parameters={
                "apiVersion": "2024-02-01",
                "deploymentId": "text-embedding-3-large-steo",
                "resourceName": "steo-azure-openai",
            },
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "azure_openai_textemb3small",
        "secret_tag": "AZURE_OPENAI",
        "dimension": 567,
        "service_options": CollectionVectorServiceOptions(
            provider="azureOpenAI",
            model_name="text-embedding-3-small",
            parameters={
                "apiVersion": "2024-02-01",
                "deploymentId": "text-embedding-3-small-steo",
                "resourceName": "steo-azure-openai",
            },
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "azure_openai_textembada2",
        "secret_tag": "AZURE_OPENAI",
        "dimension": 1536,
        "service_options": CollectionVectorServiceOptions(
            provider="azureOpenAI",
            model_name="text-embedding-ada-002",
            parameters={
                "apiVersion": "2024-02-01",
                "deploymentId": "ada2-steo",
                "resourceName": "steo-azure-openai",
            },
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "cohere_englishv2",
        "secret_tag": "COHERE",
        "dimension": 4096,
        "service_options": CollectionVectorServiceOptions(
            provider="cohere",
            model_name="embed-english-v2.0",
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "cohere_englishv3",
        "secret_tag": "COHERE",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="cohere",
            model_name="embed-english-v3.0",
        ),
        "enabled": True,
        "cloud_only": False,
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
        "cloud_only": False,
    },
    {
        "model_tag": "jinaai_base_code",
        "secret_tag": "JINAAI",
        "dimension": 768,
        "service_options": CollectionVectorServiceOptions(
            provider="jinaAI",
            model_name="jina-embeddings-v2-base-code",
        ),
        "test_assets": CODE_TEST_ASSETS,
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "jinaai_base_en",
        "secret_tag": "JINAAI",
        "dimension": 768,
        "service_options": CollectionVectorServiceOptions(
            provider="jinaAI",
            model_name="jina-embeddings-v2-base-en",
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "jinaai_base_de",
        "secret_tag": "JINAAI",
        "dimension": 768,
        "service_options": CollectionVectorServiceOptions(
            provider="jinaAI",
            model_name="jina-embeddings-v2-base-de",
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "jinaai_base_es",
        "secret_tag": "JINAAI",
        "dimension": 768,
        "service_options": CollectionVectorServiceOptions(
            provider="jinaAI",
            model_name="jina-embeddings-v2-base-es",
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "jinaai_base_zh",
        "secret_tag": "JINAAI",
        "dimension": 768,
        "service_options": CollectionVectorServiceOptions(
            provider="jinaAI",
            model_name="jina-embeddings-v2-base-zh",
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "mistral_embed",
        "secret_tag": "MISTRAL",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="mistral",
            model_name="mistral-embed",
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "nvidia",
        "secret_tag": "NVIDIA",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="nvidia",
            model_name="NV-Embed-QA",
        ),
        "enabled": True,
        "cloud_only": True,
    },
    {
        "model_tag": "openai_3large",
        "secret_tag": "OPENAI",
        "dimension": 678,
        "service_options": CollectionVectorServiceOptions(
            provider="openai",
            model_name="text-embedding-3-large",
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "openai_3small",
        "secret_tag": "OPENAI",
        "dimension": 456,
        "service_options": CollectionVectorServiceOptions(
            provider="openai",
            model_name="text-embedding-3-small",
        ),
        "enabled": True,
        "cloud_only": False,
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
        "cloud_only": False,
    },
    {
        "model_tag": "voyage_ai_2",
        "secret_tag": "VOYAGEAI",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="voyageAI",
            model_name="voyage-2",
            parameters={
                "autoTruncate": True,
            },
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "voyage_ai_code_2",
        "secret_tag": "VOYAGEAI",
        "dimension": 1536,
        "service_options": CollectionVectorServiceOptions(
            provider="voyageAI",
            model_name="voyage-code-2",
            parameters={
                "autoTruncate": True,
            },
        ),
        "test_assets": CODE_TEST_ASSETS,
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "voyage_ai_large_2",
        "secret_tag": "VOYAGEAI",
        "dimension": 1536,
        "service_options": CollectionVectorServiceOptions(
            provider="voyageAI",
            model_name="voyage-large-2",
            parameters={
                "autoTruncate": True,
            },
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "voyage_ai_large_2_instruct",
        "secret_tag": "VOYAGEAI",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="voyageAI",
            model_name="voyage-large-2-instruct",
            parameters={
                "autoTruncate": True,
            },
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "voyage_law_2",
        "secret_tag": "VOYAGEAI",
        "dimension": 1024,
        "service_options": CollectionVectorServiceOptions(
            provider="voyageAI",
            model_name="voyage-law-2",
            parameters={
                "autoTruncate": True,
            },
        ),
        "enabled": True,
        "cloud_only": False,
    },
    {
        "model_tag": "vertexai_gecko",
        "secret_tag": "VERTEXAI",
        "dimension": 768,
        "service_options": CollectionVectorServiceOptions(
            provider="vertexai",
            model_name="textembedding-gecko@003",
            parameters={
                "projectId": os.environ.get("HEADER_EMBEDDING_VERTEXAI_PROJECT_ID"),
                "autoTruncate": False,
            },
        ),
        "enabled": False,
        "cloud_only": False,
    },
]
