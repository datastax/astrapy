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
from typing import Any, Dict, Iterable, List, Optional, Tuple

from astrapy.info import CollectionVectorServiceOptions
from astrapy.api_commander import APICommander


alphanum = set("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890")


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

TEST_ASSETS_MAP = {
    ("jinaAI", "jina-embeddings-v2-base-code"): CODE_TEST_ASSETS,
    ("voyageAI", "voyage-code-2"): CODE_TEST_ASSETS,
}

USE_INSERT_ONE_MAP: Dict[Tuple[str, str], bool] = {
    # ("upstageAI", "solar-1-mini-embedding"): True,
}

# environment, region, auth_type. One spec must match.
#   Example: {("nvidia", "NV-Embed-QA"): [("dev", "us-west-2", "*")]}
ENV_FILTERS_MAP: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = {}

SECRET_NAME_ROOT_MAP = {
    "azureOpenAI": "AZURE_OPENAI",
    "cohere": "COHERE",
    "huggingface": "HUGGINGFACE",
    "huggingfaceDedicated": "HUGGINGFACEDED",
    "jinaAI": "JINAAI",
    "mistral": "MISTRAL",
    "nvidia": "NVIDIA",
    "openai": "OPENAI",
    "upstageAI": "UPSTAGE",
    "voyageAI": "VOYAGEAI",
}

# this is a way to suppress/limit certain combinations of
# "full param" testing from the start. If even one of the optional params
# in a test model is PARAM_SKIP_MARKER, the combination is not emitted at all.
PARAM_SKIP_MARKER = "__SKIP_ME__"

PARAMETER_VALUE_MAP = {
    ("azureOpenAI", "text-embedding-3-large", "deploymentId"): os.environ[
        "AZURE_OPENAI_DEPLOY_ID_EMB3LARGE"
    ],
    ("azureOpenAI", "text-embedding-3-large", "resourceName"): os.environ[
        "AZURE_OPENAI_RESNAME_EMB3LARGE"
    ],
    ("azureOpenAI", "text-embedding-3-small", "deploymentId"): os.environ[
        "AZURE_OPENAI_DEPLOY_ID_EMB3SMALL"
    ],
    ("azureOpenAI", "text-embedding-3-small", "resourceName"): os.environ[
        "AZURE_OPENAI_RESNAME_EMB3SMALL"
    ],
    ("azureOpenAI", "text-embedding-ada-002", "deploymentId"): os.environ[
        "AZURE_OPENAI_DEPLOY_ID_ADA2"
    ],
    ("azureOpenAI", "text-embedding-ada-002", "resourceName"): os.environ[
        "AZURE_OPENAI_RESNAME_ADA2"
    ],
    ("voyageAI", "voyage-2", "autoTruncate"): True,
    ("voyageAI", "voyage-code-2", "autoTruncate"): True,
    ("voyageAI", "voyage-large-2", "autoTruncate"): True,
    ("voyageAI", "voyage-large-2-instruct", "autoTruncate"): True,
    ("voyageAI", "voyage-law-2", "autoTruncate"): True,
    #
    ("huggingfaceDedicated", "endpoint-defined-model", "endpointName"): os.environ[
        "HUGGINGFACEDED_ENDPOINTNAME"
    ],
    ("huggingfaceDedicated", "endpoint-defined-model", "regionName"): os.environ[
        "HUGGINGFACEDED_REGIONNAME"
    ],
    ("huggingfaceDedicated", "endpoint-defined-model", "cloudName"): os.environ[
        "HUGGINGFACEDED_CLOUDNAME"
    ],
    #
    ("openai", "text-embedding-3-large", "organizationId"): os.environ["OPENAI_ORGANIZATION_ID"],
    ("openai", "text-embedding-3-large", "projectId"): os.environ["OPENAI_PROJECT_ID"],
    ("openai", "text-embedding-3-small", "organizationId"): os.environ["OPENAI_ORGANIZATION_ID"],
    ("openai", "text-embedding-3-small", "projectId"): os.environ["OPENAI_PROJECT_ID"],
    ("openai", "text-embedding-ada-002", "organizationId"): os.environ["OPENAI_ORGANIZATION_ID"],
    ("openai", "text-embedding-ada-002", "projectId"): os.environ["OPENAI_PROJECT_ID"],
}

# this is ad-hoc for HF dedicated. Models here, though "optional" dimension,
# do not undergo the f/0 optional dimension because of that, rather have
# a forced fixed, provided dimension.
FORCE_DIMENSION_MAP = {
    ("huggingfaceDedicated", "endpoint-defined-model"): int(
        os.environ["HUGGINGFACEDED_DIMENSION"]
    ),
}


def live_provider_info() -> Dict[str, Any]:
    """
    Query the API endpoint `findEmbeddingProviders` endpoint
    for the latest information.
    This is later used to make sure everything is mapped/tested.
    """
    ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
    ASTRA_DB_API_ENDPOINT = os.environ["ASTRA_DB_API_ENDPOINT"]
    api_endpoint = ASTRA_DB_API_ENDPOINT
    path = "api/json/v1"
    headers: Dict[str, Optional[str]] = {"Token": ASTRA_DB_APPLICATION_TOKEN}
    cmd = APICommander(
        api_endpoint=api_endpoint,
        path=path,
        headers=headers,
    )
    response = cmd.request(payload={"findEmbeddingProviders": {}})
    return response


def live_test_models() -> Iterable[Dict[str, Any]]:

    def _from_validation(pspec: Dict[str, Any]) -> int:
        assert pspec["type"] == "number"
        if "numericRange" in pspec["validation"]:
            m0: int = pspec["validation"]["numericRange"][0]
            m1: int = pspec["validation"]["numericRange"][1]
            return (m0 + m1) // 2
        else:
            raise ValueError("unsupported pspec")

    def _collapse(longt: str) -> str:
        if len(longt) <= 40:
            return longt
        else:
            return f"{longt[:30]}_{longt[-5:]}"

    # generate the full list of models based on the live provider endpoint
    live_info = live_provider_info()["status"]["embeddingProviders"]
    for provider_name, provider_desc in sorted(live_info.items()):
        for model in provider_desc["models"]:
            for auth_type_name, auth_type_desc in sorted(
                provider_desc["supportedAuthentication"].items()
            ):
                if auth_type_desc["enabled"]:
                    # test assumptions on auth type
                    if auth_type_name == "NONE":
                        assert auth_type_desc["tokens"] == []
                    elif auth_type_name == "HEADER":
                        assert {t["accepted"] for t in auth_type_desc["tokens"]} == {
                            "x-embedding-api-key"
                        }
                    elif auth_type_name == "SHARED_SECRET":
                        assert {t["accepted"] for t in auth_type_desc["tokens"]} == {
                            "providerKey"
                        }
                    else:
                        raise ValueError("Unknown auth type")

                    # params
                    collated_params = provider_desc.get("parameters", []) + model.get(
                        "parameters", []
                    )
                    all_nond_params = [
                        param
                        for param in collated_params
                        if param["name"] != "vectorDimension"
                    ]
                    required_nond_params = {
                        param["name"] for param in all_nond_params if param["required"]
                    }
                    optional_nond_params = {
                        param["name"]
                        for param in all_nond_params
                        if not param["required"]
                    }
                    #
                    d_params = [
                        param
                        for param in collated_params
                        if param["name"] == "vectorDimension"
                    ]
                    if d_params:
                        d_param = d_params[0]
                        if "defaultValue" in d_param:
                            if (provider_name, model["name"]) in FORCE_DIMENSION_MAP:
                                optional_dimension = False
                                dimension = FORCE_DIMENSION_MAP[
                                    (provider_name, model["name"])
                                ]
                            else:
                                optional_dimension = True
                                assert model["vectorDimension"] is None
                                dimension = _from_validation(d_param)
                        else:
                            optional_dimension = False
                            assert model["vectorDimension"] is None
                            dimension = _from_validation(d_param)
                    else:
                        optional_dimension = False
                        assert model["vectorDimension"] is not None
                        assert model["vectorDimension"] > 0
                        dimension = model["vectorDimension"]

                    model_parameters = {
                        param_name: PARAMETER_VALUE_MAP[
                            (provider_name, model["name"], param_name)
                        ]
                        for param_name in required_nond_params
                    }
                    optional_model_parameters = {
                        param_name: PARAMETER_VALUE_MAP[
                            (provider_name, model["name"], param_name)
                        ]
                        for param_name in optional_nond_params
                    }

                    if optional_dimension or optional_nond_params != set():
                        # we issue a minimal-params version
                        model_tag_0 = (
                            f"{provider_name}/{model['name']}/{auth_type_name}/0"
                        )
                        this_minimal_model = {
                            "model_tag": model_tag_0,
                            "simple_tag": _collapse(
                                "".join(c for c in model_tag_0 if c in alphanum)
                            ),
                            "auth_type_name": auth_type_name,
                            "secret_tag": SECRET_NAME_ROOT_MAP[provider_name],
                            "test_assets": TEST_ASSETS_MAP.get(
                                (provider_name, model["name"]), DEFAULT_TEST_ASSETS
                            ),
                            "use_insert_one": USE_INSERT_ONE_MAP.get(
                                (provider_name, model["name"]), False
                            ),
                            "env_filters": ENV_FILTERS_MAP.get(
                                (provider_name, model["name"]), [("*", "*", "*")]
                            ),
                            "service_options": CollectionVectorServiceOptions(
                                provider=provider_name,
                                model_name=model["name"],
                                parameters=model_parameters,
                            ),
                        }
                        yield this_minimal_model

                    # and in any case we issue a 'full-spec' one ...
                    # ... unless explicitly marked as skipped
                    if all(
                        v != PARAM_SKIP_MARKER
                        for v in optional_model_parameters.values()
                    ):
                        root_model = {
                            "auth_type_name": auth_type_name,
                            "dimension": dimension,
                            "secret_tag": SECRET_NAME_ROOT_MAP[provider_name],
                            "test_assets": TEST_ASSETS_MAP.get(
                                (provider_name, model["name"]), DEFAULT_TEST_ASSETS
                            ),
                            "use_insert_one": USE_INSERT_ONE_MAP.get(
                                (provider_name, model["name"]), False
                            ),
                            "env_filters": ENV_FILTERS_MAP.get(
                                (provider_name, model["name"]), [("*", "*", "*")]
                            ),
                        }

                        model_tag_f = (
                            f"{provider_name}/{model['name']}/{auth_type_name}/f"
                        )

                        this_model = {
                            "model_tag": model_tag_f,
                            "simple_tag": _collapse(
                                "".join(c for c in model_tag_f if c in alphanum)
                            ),
                            "service_options": CollectionVectorServiceOptions(
                                provider=provider_name,
                                model_name=model["name"],
                                parameters={
                                    **model_parameters,
                                    **optional_model_parameters,
                                },
                            ),
                            **root_model,
                        }
                        yield this_model
