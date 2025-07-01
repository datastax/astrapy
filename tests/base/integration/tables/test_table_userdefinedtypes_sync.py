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

import os
from typing import TYPE_CHECKING, Any

import pytest

from astrapy import Database
from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.data_types import (
    DataAPIMap,
    DataAPISet,
    DataAPIUserDefinedType,
    DictDataAPIUserDefinedType,
)
from astrapy.exceptions import DataAPIResponseException

from ..conftest import (
    CQL_AVAILABLE,
    DataAPICredentials,
    DefaultTable,
    # ExtendedPlayer,
    # ExtendedPlayerUDTWrapper,
    # NullablePlayer,
    # NullablePlayerUDTWrapper,
    Player,
    PlayerUDTWrapper,
)
from .table_cql_assets import _extract_udt_definition
from .table_row_assets import (
    UDT_ALTER_OP_1,
    UDT_ALTER_OP_2,
    UDT_DEF0,
    UDT_DEF1,
    UDT_NAME,
)

if TYPE_CHECKING:
    from cassandra.cluster import Session


@pytest.mark.skipif(
    "ASTRAPY_TEST_UDT" not in os.environ,
    reason="UDT testing not enabled",
)
@pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
class TestTableUserDefinedTypes:
    @pytest.mark.describe("Test of UDT lifecycle, sync")
    def test_table_udt_lifecycle_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        cql_session: Session,
        sync_database: Database,
    ) -> None:
        try:
            sync_database.create_type(UDT_NAME, definition=UDT_DEF0)
            with pytest.raises(DataAPIResponseException):
                sync_database.create_type(UDT_NAME, definition=UDT_DEF0)
            sync_database.create_type(UDT_NAME, definition=UDT_DEF0, if_not_exists=True)

            assert UDT_DEF0 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            sync_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_1)
            sync_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_2)
            with pytest.raises(DataAPIResponseException):
                sync_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_1)

            assert UDT_DEF1 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            sync_database.drop_type(UDT_NAME)
            assert (
                _extract_udt_definition(
                    cql_session,
                    data_api_credentials_kwargs["keyspace"],
                    UDT_NAME,
                )
                is None
            )
            with pytest.raises(DataAPIResponseException):
                sync_database.drop_type(UDT_NAME)
            sync_database.drop_type(UDT_NAME, if_exists=True)

            sync_database.create_type(UDT_NAME, definition=UDT_DEF0)
            sync_database.drop_type(UDT_NAME)
        finally:
            sync_database.drop_type(UDT_NAME, if_exists=True)

    @pytest.mark.parametrize(
        ("use_dataclass",),
        [(False,), (True,)],
        ids=["dict-based", "dataclass-based"],
    )
    @pytest.mark.parametrize(
        ("use_maptuples",),
        [(False,), (True,)],
        ids=["no-map-tuples", "do-map-tuples"],
    )
    @pytest.mark.describe("Test of full-UDT DML, sync")
    def test_table_full_udt_dml_sync(
        self,
        player_udt: str,
        sync_empty_table_udt_player: DefaultTable,
        use_dataclass: bool,
        use_maptuples: bool,
    ) -> None:
        if use_maptuples:
            if "ASTRAPY_TEST_LATEST_MAIN" not in os.environ:
                pytest.skip("maps-as-tuples require 'latest main' testing for now.")

        wrapped_object: Any
        wrapper_class: type[DataAPIUserDefinedType[Any]]
        udt_class_map: dict[str, type[DataAPIUserDefinedType[Any]]]
        if use_dataclass:
            wrapped_object = Player(name="Jim", age=75)
            wrapper_class = PlayerUDTWrapper
            udt_class_map = {player_udt: PlayerUDTWrapper}
        else:
            wrapped_object = {"name": "Charles", "age": 36}
            wrapper_class = DictDataAPIUserDefinedType
            udt_class_map = {}

        # the serdes options have a nontrivial udt-class-map only if not dict-based:
        table = sync_empty_table_udt_player.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    udt_class_map=udt_class_map,
                    encode_maps_as_lists_in_tables="ALWAYS"
                    if use_maptuples
                    else "NEVER",
                ),
            ),
        )

        wrapped = wrapper_class(wrapped_object)
        write_row = {
            "id": "a",
            "scalar_udt": wrapped,
            "list_udt": [wrapped],
            "set_udt": DataAPISet([wrapped]),
            "map_udt": DataAPIMap([("schlussel", wrapped)]),
        }
        ins_result = table.insert_one(write_row)
        assert ins_result.inserted_id == {"id": "a"}

        read_row = table.find_one(ins_result.inserted_id)
        assert read_row == write_row

        # projections
        for fld in write_row.keys():
            prj_read_row = table.find_one(
                ins_result.inserted_id,
                projection={fld: True},
            )
            assert prj_read_row == {fld: write_row[fld]}
