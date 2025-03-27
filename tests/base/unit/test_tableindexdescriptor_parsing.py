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

import json
import logging
from typing import Any

import pytest

from astrapy.info import (
    TableAPIIndexSupportDescriptor,
    TableIndexDefinition,
    TableIndexDescriptor,
    TableIndexOptions,
    TableIndexType,
    TableUnsupportedIndexDefinition,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
)

LOCAL_R_RESPONSE = json.loads("""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "b",
                    "options": {
                        "ascii": false,
                        "caseSensitive": true,
                        "normalize": false
                    }
                },
                "indexType": "regular"
            }
        ]
    }
}""")
LOCAL_R_RESPONSE_W = json.loads("""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "b",
                    "options": {
                        "ascii": false,
                        "caseSensitive": true,
                        "normalize": false
                    }
                },
                "indexType": "WRONG_INDEX_TYPE"
            }
        ]
    }
}""")
PROD_R_RESPONSE = json.loads("""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "b",
                    "options": {
                        "ascii": false,
                        "caseSensitive": true,
                        "normalize": false
                    }
                }
            }
        ]
    }
}""")

LOCAL_V_RESPONSE = json.loads("""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "b",
                    "options": {
                        "metric": "cosine",
                        "sourceModel": "other"
                    }
                },
                "indexType": "vector"
            }
        ]
    }
}""")
LOCAL_V_RESPONSE_W = json.loads("""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "b",
                    "options": {
                        "metric": "cosine",
                        "sourceModel": "other"
                    }
                },
                "indexType": "WRONG_INDEX_TYPE"
            }
        ]
    }
}""")
PROD_V_RESPONSE = json.loads("""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "b",
                    "options": {
                        "metric": "cosine",
                        "sourceModel": "other"
                    }
                }
            }
        ]
    }
}""")

LOCAL_U_RESPONSE = json.loads(r"""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "UNKNOWN",
                    "apiSupport": {
                        "createIndex": false,
                        "filter": false,
                        "cqlDefinition": "CREATE CUSTOM INDEX t1_b_index ON default_keyspace.t1 (\"bCamelCase\")\nUSING 'StorageAttachedIndex'\nWITH OPTIONS = {\n    'similarity_function' : 'COSINE',\n    'source_model' : 'OTHER'}"
                    }
                },
                "indexType": "UNKNOWN"
            }
        ]
    }
}""")
LOCAL_U_RESPONSE_W = json.loads(r"""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "UNKNOWN",
                    "apiSupport": {
                        "createIndex": false,
                        "filter": false,
                        "cqlDefinition": "CREATE CUSTOM INDEX t1_b_index ON default_keyspace.t1 (\"bCamelCase\")\nUSING 'StorageAttachedIndex'\nWITH OPTIONS = {\n    'similarity_function' : 'COSINE',\n    'source_model' : 'OTHER'}"
                    }
                },
                "indexType": "WRONG_INDEX_TYPE"
            }
        ]
    }
}""")
PROD_U_RESPONSE = json.loads(r"""{
    "status": {
        "indexes": [
            {
                "name": "t1_b_index",
                "definition": {
                    "column": "UNKNOWN",
                    "apiSupport": {
                        "createIndex": false,
                        "filter": false,
                        "cqlDefinition": "CREATE CUSTOM INDEX t1_b_index ON default_keyspace.t1 (\"bCamelCase\")\nUSING 'StorageAttachedIndex'\nWITH OPTIONS = {\n    'similarity_function' : 'COSINE',\n    'source_model' : 'OTHER'}"
                    }
                }
            }
        ]
    }
}""")


class TestTableIndexDescriptionParsing:
    @pytest.mark.describe("test of parsing index descriptions with/out indexType")
    @pytest.mark.parametrize(
        ("no_it_resp", "it_resp", "faulty_it_resp"),
        [
            (PROD_R_RESPONSE, LOCAL_R_RESPONSE, LOCAL_R_RESPONSE_W),
            (PROD_V_RESPONSE, LOCAL_V_RESPONSE, LOCAL_V_RESPONSE_W),
            (PROD_U_RESPONSE, LOCAL_U_RESPONSE, LOCAL_U_RESPONSE_W),
        ],
        ids=("regular", "vector", "unknown"),
    )
    def test_parsing_regular_index_description(
        self,
        caplog: pytest.LogCaptureFixture,
        no_it_resp: dict[str, Any],
        it_resp: dict[str, Any],
        faulty_it_resp: dict[str, Any],
    ) -> None:
        descriptors = [
            TableIndexDescriptor.coerce(response["status"]["indexes"][0], columns={})
            for response in [
                PROD_U_RESPONSE,
                LOCAL_U_RESPONSE,
            ]
        ]
        with caplog.at_level(logging.WARNING):
            descriptors.append(
                TableIndexDescriptor.coerce(
                    LOCAL_U_RESPONSE_W["status"]["indexes"][0], columns={}
                )
            )

        warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
        assert len(warnings) == 1
        assert "WRONG_INDEX_TYPE" in warnings[0].message
        assert descriptors[0] == descriptors[1]
        assert descriptors[0] == descriptors[2]

    @pytest.mark.describe("test of creating regular TableIndexDescriptor instances")
    def test_create_reg_tableindexdescriptor_various_indextype(self) -> None:
        reg_ti_0 = TableIndexDescriptor(
            name="reg_ti",
            definition=TableIndexDefinition(
                column="reg_col",
                options=TableIndexOptions(
                    ascii=False,
                    normalize=True,
                    case_sensitive=False,
                ),
            ),
        )
        reg_ti_e = TableIndexDescriptor(
            name="reg_ti",
            definition=TableIndexDefinition(
                column="reg_col",
                options=TableIndexOptions(
                    ascii=False,
                    normalize=True,
                    case_sensitive=False,
                ),
            ),
            index_type=TableIndexType.REGULAR,
        )
        reg_ti_s = TableIndexDescriptor(
            name="reg_ti",
            definition=TableIndexDefinition(
                column="reg_col",
                options=TableIndexOptions(
                    ascii=False,
                    normalize=True,
                    case_sensitive=False,
                ),
            ),
            index_type="regular",
        )
        assert reg_ti_0 == reg_ti_e
        assert reg_ti_0 == reg_ti_s

    @pytest.mark.describe("test of creating vector TableIndexDescriptor instances")
    def test_create_vec_tableindexdescriptor_various_indextype(self) -> None:
        vec_ti_0 = TableIndexDescriptor(
            name="vec_ti",
            definition=TableVectorIndexDefinition(
                column="vec_col",
                options=TableVectorIndexOptions(metric="cosine"),
            ),
        )
        vec_ti_e = TableIndexDescriptor(
            name="vec_ti",
            definition=TableVectorIndexDefinition(
                column="vec_col",
                options=TableVectorIndexOptions(metric="cosine"),
            ),
            index_type=TableIndexType.VECTOR,
        )
        vec_ti_s = TableIndexDescriptor(
            name="vec_ti",
            definition=TableVectorIndexDefinition(
                column="vec_col",
                options=TableVectorIndexOptions(metric="cosine"),
            ),
            index_type="vector",
        )
        assert vec_ti_0 == vec_ti_e
        assert vec_ti_0 == vec_ti_s

    @pytest.mark.describe("test of creating unsupported TableIndexDescriptor instances")
    def test_create_uns_tableindexdescriptor_various_indextype(self) -> None:
        unk_ti_0 = TableIndexDescriptor(
            name="unk_ti",
            definition=TableUnsupportedIndexDefinition(
                column="UNKNOWN",
                api_support=TableAPIIndexSupportDescriptor(
                    cql_definition="c", create_index=False, filter=True
                ),
            ),
        )
        unk_ti_e = TableIndexDescriptor(
            name="unk_ti",
            definition=TableUnsupportedIndexDefinition(
                column="UNKNOWN",
                api_support=TableAPIIndexSupportDescriptor(
                    cql_definition="c", create_index=False, filter=True
                ),
            ),
            index_type=TableIndexType.UNKNOWN,
        )
        unk_ti_s = TableIndexDescriptor(
            name="unk_ti",
            definition=TableUnsupportedIndexDefinition(
                column="UNKNOWN",
                api_support=TableAPIIndexSupportDescriptor(
                    cql_definition="c", create_index=False, filter=True
                ),
            ),
            index_type="UNKNOWN",
        )
        assert unk_ti_0 == unk_ti_e
        assert unk_ti_0 == unk_ti_s
