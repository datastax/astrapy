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

from astrapy.data.utils.table_converters import _TableConverterAgent
from astrapy.utils.api_options import FullWireFormatOptions


class TestTableConverterAgent:
    @pytest.mark.describe("test of table converter agent, row converters")
    def test_tableconverteragent_row(self) -> None:
        options = FullWireFormatOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            unroll_iterables_to_lists=True,
        )
        agent = _TableConverterAgent(options=options)
        assert len(agent.row_postprocessors) == 0

        # try row of type 1
        schema1 = {
            "p_text": {"type": "text"},
            "p_boolean": {"type": "boolean"},
            "p_float_pinf": {"type": "float"},
        }
        raw_row1 = {
            "p_text": "italy",
            "p_boolean": True,
            "p_float_pinf": "Infinity",
        }
        agent.postprocess_row(raw_row1, columns_dict=schema1)
        assert len(agent.row_postprocessors) == 1

        # try row of type 2
        schema2 = {
            "p_float_pinf": {"type": "float"},
        }
        raw_row2 = {
            "p_float_pinf": "Infinity",
        }
        agent.postprocess_row(raw_row2, columns_dict=schema2)
        assert len(agent.row_postprocessors) == 2

        # try row of type 1 again
        raw_row1b = {
            "p_text": "croatia",
            "p_boolean": False,
            "p_float_pinf": 54.321,
        }
        agent.postprocess_row(raw_row1b, columns_dict=schema1)
        assert len(agent.row_postprocessors) == 2

    @pytest.mark.describe("test of table converter agent, key converters")
    def test_tableconverteragent_key(self) -> None:
        options = FullWireFormatOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            unroll_iterables_to_lists=True,
        )
        agent = _TableConverterAgent(options=options)
        assert len(agent.key_postprocessors) == 0

        # try row of type 1
        k_schema1 = {
            "p_text": {"type": "text"},
            "p_boolean": {"type": "boolean"},
            "p_float_pinf": {"type": "float"},
        }
        pk_list1 = ["italy", True, "Infinity"]
        agent.postprocess_key(pk_list1, primary_key_schema_dict=k_schema1)
        assert len(agent.key_postprocessors) == 1

        # try row of type 2
        k_schema2 = {
            "p_float_pinf": {"type": "float"},
        }
        pk_list2 = ["Infinity"]
        agent.postprocess_key(pk_list2, primary_key_schema_dict=k_schema2)
        assert len(agent.key_postprocessors) == 2

        # try row of type 1 again
        pk_list1b = ["croatia", False, 54.321]
        agent.postprocess_key(pk_list1b, primary_key_schema_dict=k_schema1)
        assert len(agent.key_postprocessors) == 2
