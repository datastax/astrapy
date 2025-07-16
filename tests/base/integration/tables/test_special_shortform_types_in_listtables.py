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

from ..conftest import DefaultTable


class TestTableSpecialShortformTypesInListTables:
    @pytest.mark.describe(
        "Test listTables returns the short form for key/valueType primitive fields"
    )
    def test_table_special_shortform_types_listtables(
        self,
        sync_table_all_returns: DefaultTable,
    ) -> None:
        """
        Temporary test added as a guard to ensure short-form is used
        for primitive types when inside collection columns. Once clients 2.1
        are out for some time, this can be lifted and the newer, long-form-preferring
        Data API can be deployed.
        """

        raw_response = sync_table_all_returns.database.command(
            {"listTables": {"options": {"explain": True}}}
        )
        the_table_s = [
            tb_dict
            for tb_dict in raw_response["status"]["tables"]
            if tb_dict["name"] == sync_table_all_returns.name
        ]
        assert the_table_s != []

        for col_name in {"p_list_int", "p_map_text_text", "p_set_int"}:
            col_def = the_table_s[0]["definition"]["columns"][col_name]
            if "valueType" in col_def:
                assert isinstance(col_def["valueType"], str)
            if "keyType" in col_def:
                assert isinstance(col_def["keyType"], str)

    @pytest.mark.describe(
        "Test projectionSchema has the short form for key/valueType primitive fields"
    )
    def test_table_special_shortform_types_projectionschema(
        self,
        sync_table_all_returns: DefaultTable,
    ) -> None:
        """
        Temporary test added as a guard to ensure short-form is used
        for primitive types when inside collection columns. Once clients 2.1
        are out for some time, this can be lifted and the newer, long-form-preferring
        Data API can be deployed.
        """

        raw_response = sync_table_all_returns.command(
            {"findOne": {"filter": {"p_ascii": "a", "p_bigint": 1}}}
        )
        proj_schema = raw_response["status"]["projectionSchema"]

        for col_name in {"p_list_int", "p_map_text_text", "p_set_int"}:
            col_def = proj_schema[col_name]
            if "valueType" in col_def:
                assert isinstance(col_def["valueType"], str)
            if "keyType" in col_def:
                assert isinstance(col_def["keyType"], str)
