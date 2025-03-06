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

from astrapy.constants import DefaultRowType
from astrapy.data.utils.table_converters import _TableConverterAgent
from astrapy.data_types import DataAPIDate
from astrapy.utils.api_options import FullSerdesOptions


class TestTableConverterAgent:
    @pytest.mark.describe("test of table converter agent, row converters")
    def test_tableconverteragent_row(self) -> None:
        options = FullSerdesOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            unroll_iterables_to_lists=True,
            use_decimals_in_collections=False,
            encode_maps_as_lists_in_tables=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=None,
        )
        agent: _TableConverterAgent[DefaultRowType] = _TableConverterAgent(
            options=options
        )
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
        agent.postprocess_row(
            raw_row1, columns_dict=schema1, similarity_pseudocolumn=None
        )
        assert len(agent.row_postprocessors) == 1

        # try row of type 2
        schema2 = {
            "p_float_pinf": {"type": "float"},
        }
        raw_row2 = {
            "p_float_pinf": "Infinity",
        }
        agent.postprocess_row(
            raw_row2, columns_dict=schema2, similarity_pseudocolumn=None
        )
        assert len(agent.row_postprocessors) == 2

        # try row of type 1 again
        raw_row1b = {
            "p_text": "croatia",
            "p_boolean": False,
            "p_float_pinf": 54.321,
        }
        agent.postprocess_row(
            raw_row1b, columns_dict=schema1, similarity_pseudocolumn=None
        )
        assert len(agent.row_postprocessors) == 2

    @pytest.mark.describe("test of table converter agent, row with similarity")
    def test_tableconverteragent_row_similarity(self) -> None:
        options = FullSerdesOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            unroll_iterables_to_lists=True,
            use_decimals_in_collections=False,
            encode_maps_as_lists_in_tables=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=None,
        )
        agent: _TableConverterAgent[DefaultRowType] = _TableConverterAgent(
            options=options
        )
        schema1 = {
            "$similarity": {"type": "date"},
        }

        raw_row1 = {
            "$similarity": 0.123,
        }
        result1 = agent.postprocess_row(
            raw_row1, columns_dict=schema1, similarity_pseudocolumn="$similarity"
        )
        assert isinstance(result1["$similarity"], float)

        raw_row2 = {
            "$similarity": "2021-11-11",
        }
        result2 = agent.postprocess_row(
            raw_row2, columns_dict=schema1, similarity_pseudocolumn=None
        )
        assert isinstance(result2["$similarity"], DataAPIDate)

    @pytest.mark.describe("test of table converter agent, key converters")
    def test_tableconverteragent_key(self) -> None:
        options = FullSerdesOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            unroll_iterables_to_lists=True,
            use_decimals_in_collections=False,
            encode_maps_as_lists_in_tables=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=None,
        )
        agent: _TableConverterAgent[DefaultRowType] = _TableConverterAgent(
            options=options
        )
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
