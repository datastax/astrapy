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

from typing import Any

import pytest

from astrapy.info import (
    CollectionDefinition,
    CollectionDescriptor,
    CollectionLexicalOptions,
    CollectionRerankingOptions,
    RerankingServiceOptions,
)


class TestFindAndRerankCollectionDefinition:
    @pytest.mark.describe("test of FARR collection definitions conversions")
    def test_farr_collectiondefinition_conversions(self) -> None:
        api_coll_descs: list[dict[str, Any]] = [
            {
                "name": "col_name",
                "options": {
                    "lexical": {
                        "enabled": False,
                        "analyzer": "the_analyzer",
                    },
                },
            },
            {
                "name": "col_name",
                "options": {
                    "rerank": {
                        "enabled": True,
                        "service": {
                            "provider": "the_pro",
                            "modelName": "the_mo",
                        },
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

    @pytest.mark.describe("test of FARR collection lexical options conversions")
    def test_farr_collectionlexicaloptions_conversions(self) -> None:
        lx1 = CollectionLexicalOptions(analyzer="a")
        assert lx1.as_dict() == {"analyzer": "a", "enabled": True}
        assert CollectionLexicalOptions._from_dict(lx1.as_dict()) == lx1

        lx2 = CollectionLexicalOptions(analyzer="b", enabled=True)
        assert lx2.as_dict() == {"analyzer": "b", "enabled": True}
        assert CollectionLexicalOptions._from_dict(lx2.as_dict()) == lx2

        lx3 = CollectionLexicalOptions(analyzer={"x": 1}, enabled=False)
        assert lx3.as_dict() == {"analyzer": {"x": 1}, "enabled": False}
        assert CollectionLexicalOptions._from_dict(lx3.as_dict()) == lx3

    @pytest.mark.describe("test of FARR collection rerank options conversions")
    def test_farr_collectionrerankoptions_conversions(self) -> None:
        sv1 = RerankingServiceOptions(
            provider="provider",
            model_name="model_name",
            authentication={"a": "z"},
            parameters={"p": "q"},
        )
        assert sv1.as_dict() == {
            "provider": "provider",
            "modelName": "model_name",
            "authentication": {"a": "z"},
            "parameters": {"p": "q"},
        }
        assert RerankingServiceOptions._from_dict(sv1.as_dict()) == sv1

        sv2 = RerankingServiceOptions(
            provider="pr",
            model_name="mn",
        )
        assert sv2.as_dict() == {"provider": "pr", "modelName": "mn"}
        assert RerankingServiceOptions._from_dict(sv2.as_dict()) == sv2

        rr1 = CollectionRerankingOptions(service=sv1)
        assert rr1.as_dict() == {"service": sv1.as_dict(), "enabled": True}
        assert CollectionRerankingOptions._from_dict(rr1.as_dict()) == rr1

        rr2 = CollectionRerankingOptions(service=None, enabled=True)
        assert rr2.as_dict() == {"enabled": True}
        assert CollectionRerankingOptions._from_dict(rr2.as_dict()) == rr2

        rr3 = CollectionRerankingOptions(service=sv2, enabled=False)
        assert rr3.as_dict() == {"service": sv2.as_dict(), "enabled": False}
        assert CollectionRerankingOptions._from_dict(rr3.as_dict()) == rr3

    @pytest.mark.describe(
        "test of FARR collection definition, rerank builder interface"
    )
    def test_farr_collectiondefinition_rerank_builders(self) -> None:
        zero = CollectionDefinition.builder()

        rer1 = zero.set_rerank("p", "m", parameters={"p": "q"}).build()
        assert rer1.as_dict() == {
            "rerank": {
                "enabled": True,
                "service": {
                    "provider": "p",
                    "modelName": "m",
                    "parameters": {"p": "q"},
                },
            }
        }

        r_svc = RerankingServiceOptions("P", "M")
        rer2 = zero.set_rerank(r_svc).build()
        assert rer2.as_dict() == {
            "rerank": {
                "enabled": True,
                "service": {"provider": "P", "modelName": "M"},
            }
        }

        cr_opt = CollectionRerankingOptions(
            service=RerankingServiceOptions("pp", "mm"),
        )
        rer3 = zero.set_rerank(cr_opt).build()
        assert rer3.as_dict() == {
            "rerank": {
                "enabled": True,
                "service": {"provider": "pp", "modelName": "mm"},
            }
        }

        rer_z = zero.set_rerank("x", "y")
        reset = rer_z.set_rerank(None).build()
        assert reset.as_dict() == {}

    @pytest.mark.describe(
        "test of FARR collection definition, lexical builder interface"
    )
    def test_farr_collectiondefinition_lexical_builders(self) -> None:
        zero = CollectionDefinition.builder()

        lex1 = zero.set_lexical("an").build()
        assert lex1.as_dict() == {
            "lexical": {
                "enabled": True,
                "analyzer": "an",
            }
        }

        lx_opt = CollectionLexicalOptions(enabled=False, analyzer="AN")
        lex2 = zero.set_lexical(lx_opt).build()
        assert lex2.as_dict() == {
            "lexical": {
                "enabled": False,
                "analyzer": "AN",
            }
        }

        lex_z = zero.set_lexical("aaa")
        reset = lex_z.set_lexical(None)
        assert reset.as_dict() == {}
