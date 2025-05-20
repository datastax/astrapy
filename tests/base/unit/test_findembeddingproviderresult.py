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

import pytest

from astrapy.info import FindEmbeddingProvidersResult

RESPONSE_DICT_0 = {
    "embeddingProviders": {
        "provider": {
            "displayName": "TheProvider",
            "url": "https://api.provider.com/v1/",
            "supportedAuthentication": {
                "SHARED_SECRET": {
                    "tokens": [
                        {
                            "accepted": "providerKey",
                            "forwarded": "Authorization",
                        },
                    ],
                    "enabled": True,
                },
                "HEADER": {
                    "tokens": [
                        {
                            "accepted": "x-embedding-api-key",
                            "forwarded": "Authorization",
                        },
                    ],
                    "enabled": True,
                },
                "NONE": {
                    "tokens": [],
                    "enabled": True,
                },
            },
            "parameters": [
                {
                    "help": "parameter help",
                    "defaultValue": "x",
                    "displayName": "The Parameter",
                    "hint": "Provide a parameter",
                    "name": "the_parameter",
                    "type": "STRING",
                    "validation": {},
                    "required": False,
                },
            ],
            "models": [
                {
                    "name": "the-model",
                    "vectorDimension": None,
                    "parameters": [
                        {
                            "name": "vectorDimension",
                            "type": "NUMBER",
                            "required": True,
                            "defaultValue": "3072",
                            "validation": {
                                "numericRange": [
                                    256,
                                    3072,
                                ],
                            },
                            "help": "Help for vector dimension",
                        },
                    ],
                },
            ],
        },
    },
}

RESPONSE_DICT_1 = {
    "embeddingProviders": {
        "provider": {
            "displayName": "TheProvider",
            "url": "https://api.provider.com/v1/",
            "supportedAuthentication": {
                "SHARED_SECRET": {
                    "tokens": [
                        {
                            "accepted": "providerKey",
                            "forwarded": "Authorization",
                        },
                    ],
                    "enabled": True,
                },
                "HEADER": {
                    "tokens": [
                        {
                            "accepted": "x-embedding-api-key",
                            "forwarded": "Authorization",
                        },
                    ],
                    "enabled": True,
                },
                "NONE": {
                    "tokens": [],
                    "enabled": True,
                },
            },
            "parameters": [
                {
                    "help": "parameter help",
                    "defaultValue": "x",
                    "displayName": "The Parameter",
                    "hint": "Provide a parameter",
                    "name": "the_parameter",
                    "type": "STRING",
                    "validation": {},
                    "required": False,
                },
            ],
            "models": [
                {
                    "name": "provider/<model>",
                    "apiModelSupport": {
                        "status": "SUPPORTED",
                    },
                    "vectorDimension": None,
                    "parameters": [
                        {
                            "name": "vectorDimension",
                            "type": "NUMBER",
                            "required": True,
                            "defaultValue": "3072",
                            "validation": {
                                "numericRange": [
                                    256,
                                    3072,
                                ],
                            },
                            "help": "Help for vector dimension",
                        },
                    ],
                },
            ],
        },
    },
}


class TestFindEmbeddingProvidersResult:
    @pytest.mark.describe("test of FindEmbeddingProvidersResult parsing and back")
    def test_embedding_providers_result_parsing_and_back_base(self) -> None:
        parsed = FindEmbeddingProvidersResult._from_dict(RESPONSE_DICT_0)
        providers = parsed.embedding_providers
        assert len(providers) == 1
        assert providers["provider"].display_name is not None

        models = providers["provider"].models
        assert len(models) == 1

        dumped = parsed.as_dict()
        # clean out apiModelSupport from generated dict before checking
        for pro_v in dumped["embeddingProviders"].values():
            for mod_v in pro_v["models"]:
                del mod_v["apiModelSupport"]
        assert dumped == RESPONSE_DICT_0

    @pytest.mark.describe(
        "test of FindEmbeddingProvidersResult parsing and back (with apiModelSupport)"
    )
    def test_embedding_providers_result_parsing_and_back_rich(self) -> None:
        parsed = FindEmbeddingProvidersResult._from_dict(RESPONSE_DICT_1)
        providers = parsed.embedding_providers
        assert len(providers) == 1
        assert providers["provider"].display_name is not None

        models = providers["provider"].models
        assert len(models) == 1

        dumped = parsed.as_dict()
        assert dumped == RESPONSE_DICT_1
