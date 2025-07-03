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

import datetime
import os
from typing import TYPE_CHECKING, Any

import pytest

from astrapy import Database
from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.data_types import (
    DataAPIMap,
    DataAPISet,
    DataAPITimestamp,
    DataAPIUserDefinedType,
    DictDataAPIUserDefinedType,
)
from astrapy.exceptions import DataAPIResponseException

from ..conftest import (
    CQL_AVAILABLE,
    DataAPICredentials,
    DefaultTable,
    ExtendedPlayer,
    ExtendedPlayerUDTWrapper,
    NullablePlayer,
    NullablePlayerUDTWrapper,
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
    WEIRD_BASE_DOCUMENT,
    WEIRD_BASE_DOCUMENT_PK,
    WEIRD_NESTED_EXPECTED_DOCUMENT,
    WEIRD_UDT_BASE_CLOSE_STATEMENTS,
    WEIRD_UDT_BASE_INITIALIZE_STATEMENTS,
    WEIRD_UDT_BASE_TABLE_NAME,
    WEIRD_UDT_NESTED_CLOSE_STATEMENTS,
    WEIRD_UDT_NESTED_DOCUMENT_PK,
    WEIRD_UDT_NESTED_INITIALIZE_STATEMENTS,
    WEIRD_UDT_NESTED_TABLE_NAME,
)

if TYPE_CHECKING:
    from cassandra.cluster import Session


@pytest.mark.skipif(
    "ASTRAPY_TEST_UDT" not in os.environ,
    reason="UDT testing not enabled",
)
class TestTableUserDefinedTypes:
    @pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
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
        ids=["dictBased", "dataclassBased"],
    )
    @pytest.mark.parametrize(
        ("use_maptuples",),
        [(False,), (True,)],
        ids=["noMapTuples", "doMapTuples"],
    )
    @pytest.mark.parametrize(
        ("use_customdatatypes",),
        [(False,), (True,)],
        ids=["stdlibDatatypes", "customDatatypes"],
    )
    @pytest.mark.parametrize(
        ("udt_mode",),
        [("simple",), ("partial",), ("empty",), ("extended",)],
        ids=["nonNullable", "partialNullable", "emptyNullable", "extendedClass"],
    )
    @pytest.mark.describe("Test of full-UDT DML, sync")
    def test_table_full_udt_dml_sync(
        self,
        player_udt: str,
        extended_player_udt: str,
        # TODO RESTRICT_UDT_TEST re-enable both once udt-cache issues solved in data api
        sync_empty_table_udt_player: DefaultTable,
        # sync_empty_table_udt_extended_player: DefaultTable,
        use_dataclass: bool,
        use_maptuples: bool,
        use_customdatatypes: bool,
        udt_mode: str,
    ) -> None:
        # Manual restriction of tests - TODO RESTRICT_UDT_TEST remove restriction
        _tdesc_tuple = (use_dataclass, use_maptuples, use_customdatatypes, udt_mode)
        if _tdesc_tuple[3] == "extended":
            pytest.skip("Manually scoped out for now.")

        if use_maptuples:
            if "ASTRAPY_TEST_LATEST_MAIN" not in os.environ:
                pytest.skip("maps-as-tuples require 'latest main' testing for now.")

        wrapped_object: Any
        wrapper_class: type[DataAPIUserDefinedType[Any]]
        udt_class_map: dict[str, type[DataAPIUserDefinedType[Any]]]

        date_value: DataAPITimestamp | datetime.datetime
        _da_ts = DataAPITimestamp.from_string("2033-05-18T03:33:20.321Z")
        if use_customdatatypes:
            date_value = _da_ts
        else:
            date_value = _da_ts.from_string("2033-05-18T03:33:20.321Z").to_datetime(
                tz=datetime.timezone.utc
            )

        if use_dataclass:
            if udt_mode == "simple":
                wrapped_object = Player(name="Jim", age=75)
                wrapper_class = PlayerUDTWrapper
                udt_class_map = {player_udt: PlayerUDTWrapper}
            elif udt_mode == "partial":
                wrapped_object = NullablePlayer(name="Jim")
                wrapper_class = NullablePlayerUDTWrapper
                udt_class_map = {player_udt: NullablePlayerUDTWrapper}
            elif udt_mode == "empty":
                wrapped_object = NullablePlayer()
                wrapper_class = NullablePlayerUDTWrapper
                udt_class_map = {player_udt: NullablePlayerUDTWrapper}
            elif udt_mode == "extended":
                wrapped_object = ExtendedPlayer(
                    name="Betta",
                    age=65,
                    # TODO NOBLOBINUDT blb=b"\x12\x0a",
                    ts=date_value,
                )
                wrapper_class = ExtendedPlayerUDTWrapper
                udt_class_map = {extended_player_udt: ExtendedPlayerUDTWrapper}
            else:
                raise ValueError("Unknown udt_mode in test.")
        else:
            wrapper_class = DictDataAPIUserDefinedType
            udt_class_map = {}
            if udt_mode == "simple":
                wrapped_object = {"name": "Charles", "age": 36}
            elif udt_mode == "partial":
                wrapped_object = {"name": "Charles"}
            elif udt_mode == "empty":
                wrapped_object = {}
            elif udt_mode == "extended":
                wrapped_object = {
                    "name": "Betta",
                    "age": 65,
                    # TODO NOBLOBINUDT "blb": b"\x12\x0a",
                    "ts": date_value,
                }
            else:
                raise ValueError("Unknown udt_mode in test.")

        # the serdes options have a nontrivial udt-class-map only if not dict-based:
        src_table: DefaultTable
        # TODO RESTRICT_UDT_TEST reinstate this in full once extended coexists w/ rest
        if udt_mode == "extended":
            raise ValueError("No.")
            # src_table = sync_empty_table_udt_extended_player
        else:
            src_table = sync_empty_table_udt_player
        table = src_table.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    udt_class_map=udt_class_map,
                    encode_maps_as_lists_in_tables="ALWAYS"
                    if use_maptuples
                    else "NEVER",
                    custom_datatypes_in_reading=use_customdatatypes,
                ),
            ),
        )

        wrapped = wrapper_class(wrapped_object)
        write_row: dict[str, Any]
        if use_customdatatypes:
            write_row = {
                "id": "a",
                "scalar_udt": wrapped,
                "list_udt": [wrapped],
                "set_udt": DataAPISet([wrapped]),
                "map_udt": DataAPIMap([("schlussel", wrapped)]),
            }
        else:
            write_row = {
                "id": "a",
                "scalar_udt": wrapped,
                "list_udt": [wrapped],
                "map_udt": {"schlussel": wrapped},
            }

        expected_row: dict[str, Any]
        if udt_mode == "empty":
            expected_row = {
                **write_row,
                **{"scalar_udt": None},
            }
        else:
            expected_row = write_row

        ins_result = table.insert_one(write_row)
        assert ins_result.inserted_id == {"id": "a"}

        read_row = table.find_one(
            ins_result.inserted_id,
            projection={fld: True for fld in write_row.keys()},
        )
        assert read_row == expected_row

        # projections
        for fld in expected_row.keys():
            prj_read_row = table.find_one(
                ins_result.inserted_id,
                projection={fld: True},
            )
            assert prj_read_row == {fld: expected_row[fld]}

    @pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
    @pytest.mark.describe("Test of weird UDT columns, sync")
    def test_table_udt_weirdcolumns_sync(
        self,
        cql_session: Session,
        sync_database: Database,
    ) -> None:
        try:
            for cql_statement in WEIRD_UDT_BASE_INITIALIZE_STATEMENTS:
                cql_session.execute(cql_statement)

            # test a read and a write for 'base weird'
            table_weird_base = sync_database.get_table(WEIRD_UDT_BASE_TABLE_NAME)
            ins_result = table_weird_base.insert_one(WEIRD_BASE_DOCUMENT)
            assert ins_result.inserted_id == WEIRD_BASE_DOCUMENT_PK
            doc_weird_base = table_weird_base.find_one(WEIRD_BASE_DOCUMENT_PK)
            assert doc_weird_base == WEIRD_BASE_DOCUMENT
        finally:
            for cql_statement in WEIRD_UDT_BASE_CLOSE_STATEMENTS:
                cql_session.execute(cql_statement)

    @pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
    @pytest.mark.describe("Test of weird UDT columns, sync")
    def test_table_udt_weirdnested_sync(
        self,
        cql_session: Session,
        sync_database: Database,
    ) -> None:
        try:
            for cql_statement in WEIRD_UDT_NESTED_INITIALIZE_STATEMENTS:
                cql_session.execute(cql_statement)

            # test a read for 'nested weird'
            table_weird_nst = sync_database.get_table(WEIRD_UDT_NESTED_TABLE_NAME)
            doc_weird_nst = table_weird_nst.find_one(WEIRD_UDT_NESTED_DOCUMENT_PK)
            assert doc_weird_nst == WEIRD_NESTED_EXPECTED_DOCUMENT
        finally:
            for cql_statement in WEIRD_UDT_NESTED_CLOSE_STATEMENTS:
                cql_session.execute(cql_statement)
