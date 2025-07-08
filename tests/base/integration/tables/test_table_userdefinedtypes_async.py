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

from astrapy import AsyncDatabase
from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import SerializerFunctionType, UDTDeserializerFunctionType
from astrapy.data_types import (
    DataAPIDictUDT,
    DataAPIMap,
    DataAPISet,
)
from astrapy.exceptions import DataAPIResponseException

from ..conftest import (
    CQL_AVAILABLE,
    EXTENDED_PLAYER_TYPE_NAME,
    PLAYER_TYPE_NAME,
    # THE_BYTES,
    THE_DATETIME,
    THE_TIMESTAMP,
    THE_TIMEZONE,
    DataAPICredentials,
    DefaultAsyncTable,
    ExtendedPlayer,
    NullablePlayer,
    Player,
    _extended_player_from_dict,
    _extended_player_serializer,
    _nullable_player_from_dict,
    _nullable_player_serializer,
    _player_from_dict,
    _player_serializer,
    dict_equal_same_class,
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
    WEIRD_UDT_BASE_CLOSE_STATEMENTS,
    WEIRD_UDT_BASE_INITIALIZE_STATEMENTS,
    WEIRD_UDT_BASE_TABLE_NAME,
)

if TYPE_CHECKING:
    from cassandra.cluster import Session

SERIALIZER_BY_CLASS: dict[type, SerializerFunctionType] = {
    ExtendedPlayer: _extended_player_serializer,
    NullablePlayer: _nullable_player_serializer,
    Player: _player_serializer,
}


@pytest.mark.skipif(
    "ASTRAPY_TEST_UDT" not in os.environ,
    reason="UDT testing not enabled",
)
@pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
class TestTableUserDefinedTypes:
    @pytest.mark.describe("Test of UDT lifecycle, async")
    async def test_table_udt_lifecycle_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        cql_session: Session,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            await async_database.create_type(UDT_NAME, definition=UDT_DEF0)
            with pytest.raises(DataAPIResponseException):
                await async_database.create_type(UDT_NAME, definition=UDT_DEF0)
            await async_database.create_type(
                UDT_NAME,
                definition=UDT_DEF0,
                if_not_exists=True,
            )

            assert UDT_DEF0 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            await async_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_1)
            await async_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_2)
            with pytest.raises(DataAPIResponseException):
                await async_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_1)

            assert UDT_DEF1 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            await async_database.drop_type(UDT_NAME)
            assert (
                _extract_udt_definition(
                    cql_session,
                    data_api_credentials_kwargs["keyspace"],
                    UDT_NAME,
                )
                is None
            )
            with pytest.raises(DataAPIResponseException):
                await async_database.drop_type(UDT_NAME)
            await async_database.drop_type(UDT_NAME, if_exists=True)

            await async_database.create_type(UDT_NAME, definition=UDT_DEF0)
            await async_database.drop_type(UDT_NAME)
        finally:
            await async_database.drop_type(UDT_NAME, if_exists=True)

    @pytest.mark.parametrize(
        ("encode_maps_as_lists_in_tables",),
        [
            ("NEVER",),
            ("DATAAPIMAPS",),
            ("ALWAYS",),
        ],
        ids=[
            "tuplesNEVER",
            "tuplesDATAAPIMAPS",
            "tuplesALWAYS",
        ],
    )
    @pytest.mark.parametrize(
        ("udt_format",),
        [
            ("dict",),
            ("dictwrapper",),
            ("dataclass",),
        ],
        ids=[
            "dict",
            "dictWrapper",
            "dataclass",
        ],
    )
    @pytest.mark.parametrize(
        ("udt_mode",),
        [
            ("simple",),
            ("partial",),
            ("empty",),
            ("extended",),
        ],
        ids=[
            "nonNullable",
            "partialNullable",
            "emptyNullable",
            "extendedClass",
        ],
    )
    @pytest.mark.describe("Test of UDT DML, async")
    async def test_table_udt_dml_async(
        self,
        player_udt: str,
        extended_player_udt: str,
        async_empty_table_udt_player: DefaultAsyncTable,
        async_empty_table_udt_extended_player: DefaultAsyncTable,
        encode_maps_as_lists_in_tables: str,
        udt_format: str,
        udt_mode: str,
    ) -> None:
        # combinations of write settings that are not supposed to work are skipped:
        if udt_format == "dict" and encode_maps_as_lists_in_tables == "ALWAYS":
            pytest.skip("The Data API is not supposed to accept such a write format.")

        if udt_mode == "extended":
            pytest.skip("Manually scoped out for now.")

        # choice of serdes options for writes
        src_atable: DefaultAsyncTable
        if udt_mode == "extended":
            src_atable = async_empty_table_udt_extended_player
        else:
            src_atable = async_empty_table_udt_player
        atable = src_atable.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    serializer_by_class=dict(SERIALIZER_BY_CLASS),
                    encode_maps_as_lists_in_tables=encode_maps_as_lists_in_tables,
                ),
            ),
        )

        # asset preparation
        udt_unit: Any
        if udt_mode == "simple":
            if udt_format == "dict":
                udt_unit = {
                    "name": "John",
                    "age": 90,
                }
            elif udt_format == "dictwrapper":
                udt_unit = DataAPIDictUDT(
                    {
                        "name": "John",
                        "age": 90,
                    }
                )
            elif udt_format == "dataclass":
                udt_unit = Player(
                    name="John",
                    age=90,
                )
            else:
                raise ValueError(f"Unknown udt format '{udt_format}'.")
        elif udt_mode == "partial":
            if udt_format == "dict":
                udt_unit = {
                    "age": 90,
                }
            elif udt_format == "dictwrapper":
                udt_unit = DataAPIDictUDT(
                    {
                        "age": 90,
                    }
                )
            elif udt_format == "dataclass":
                udt_unit = NullablePlayer(
                    age=90,
                )
            else:
                raise ValueError(f"Unknown udt format '{udt_format}'.")
        elif udt_mode == "empty":
            if udt_format == "dict":
                udt_unit = {}
            elif udt_format == "dictwrapper":
                udt_unit = DataAPIDictUDT({})
            elif udt_format == "dataclass":
                udt_unit = NullablePlayer()
            else:
                raise ValueError(f"Unknown udt format '{udt_format}'.")
        elif udt_mode == "extended":
            if udt_format == "dict":
                udt_unit = {
                    "name": "John",
                    "age": 90,
                    # TODO NOBLOBINUDT
                    # "blb": THE_BYTES,
                    "ts": THE_TIMESTAMP,
                }
            elif udt_format == "dictwrapper":
                udt_unit = DataAPIDictUDT(
                    {
                        "name": "John",
                        "age": 90,
                        # TODO NOBLOBINUDT
                        # "blb": THE_BYTES,
                        "ts": THE_TIMESTAMP,
                    }
                )
            elif udt_format == "dataclass":
                udt_unit = ExtendedPlayer(
                    name="John",
                    age=90,
                    # TODO NOBLOBINUDT
                    # blb=THE_BYTES,
                    ts=THE_TIMESTAMP,
                )
            else:
                raise ValueError(f"Unknown udt format '{udt_format}'.")
        else:
            raise ValueError(f"Unknown udt mode '{udt_mode}'.")

        row_custom_colls = {
            "id": "row_custom_colls",
            "scalar_udt": udt_unit,
            "list_udt": [udt_unit],
            "set_udt": DataAPISet([udt_unit]),
            "map_udt": DataAPIMap([("k", udt_unit)]),
        }
        row_stdlib_colls = {
            "id": "row_stdlib_colls",
            "scalar_udt": udt_unit,
            "list_udt": [udt_unit],
            "set_udt": [udt_unit],
            "map_udt": {"k": udt_unit},
        }
        ins1c_result = await atable.insert_one(row_custom_colls)
        ins1s_result = await atable.insert_one(row_stdlib_colls)
        insm_result = await atable.insert_many([row_custom_colls, row_stdlib_colls])

        ins1_tuples = {ins1c_result.inserted_id_tuple, ins1s_result.inserted_id_tuple}
        assert ins1_tuples == set(insm_result.inserted_id_tuples)

        # read and verify, {custom, stdlib} x {deserializer in map, default behaviour}:

        # Custom datatypes, no deserializer specified:
        for custom_datatypes_in_reading in [True, False]:
            for deserializer_in_map in [False, True]:
                # prepare assets for read verification
                exp_row: dict[str, Any]
                expected_udt_unit: Any
                projection: dict[str, bool]

                deserializer_by_udt: dict[str, UDTDeserializerFunctionType]
                if deserializer_in_map:
                    if udt_mode == "simple":
                        deserializer_by_udt = {PLAYER_TYPE_NAME: _player_from_dict}
                    elif udt_mode in {"partial", "empty"}:
                        deserializer_by_udt = {
                            PLAYER_TYPE_NAME: _nullable_player_from_dict
                        }
                    elif udt_mode == "extended":
                        deserializer_by_udt = {
                            EXTENDED_PLAYER_TYPE_NAME: _extended_player_from_dict,
                        }
                    else:
                        raise ValueError(f"Unknown udt mode '{udt_mode}'.")
                else:
                    deserializer_by_udt = {}

                if custom_datatypes_in_reading:
                    projection = {"id": False}
                    if deserializer_in_map:
                        if udt_mode == "simple":
                            expected_udt_unit = Player(name="John", age=90)
                        elif udt_mode == "partial":
                            expected_udt_unit = NullablePlayer(age=90)
                        elif udt_mode == "empty":
                            expected_udt_unit = NullablePlayer()
                        elif udt_mode == "extended":
                            expected_udt_unit = ExtendedPlayer(
                                name="John",
                                age=90,
                                # TODO NOBLOBINUDT
                                # blb=THE_BYTES,
                                ts=THE_TIMESTAMP,
                            )
                        else:
                            raise ValueError(f"Unknown udt mode '{udt_mode}'.")
                    else:
                        if udt_mode == "simple":
                            expected_udt_unit = DataAPIDictUDT(
                                {
                                    "name": "John",
                                    "age": 90,
                                }
                            )
                        elif udt_mode == "partial":
                            expected_udt_unit = DataAPIDictUDT({"age": 90})
                        elif udt_mode == "empty":
                            expected_udt_unit = DataAPIDictUDT({})
                        elif udt_mode == "extended":
                            expected_udt_unit = DataAPIDictUDT(
                                {
                                    "name": "John",
                                    "age": 90,
                                    # TODO NOBLOBINUDT
                                    # "blb": THE_BYTES,
                                    "ts": THE_TIMESTAMP,
                                }
                            )
                        else:
                            raise ValueError(f"Unknown udt mode '{udt_mode}'.")
                    exp_row = {
                        # TODO: reflects the behaviour for 'whole-null' udt:
                        "scalar_udt": expected_udt_unit
                        if udt_mode != "empty"
                        else None,
                        "list_udt": [expected_udt_unit],
                        "set_udt": DataAPISet([expected_udt_unit]),
                        "map_udt": DataAPIMap([("k", expected_udt_unit)]),
                    }
                else:
                    projection = {"id": False, "set_udt": False}
                    if deserializer_in_map:
                        if udt_mode == "simple":
                            expected_udt_unit = Player(name="John", age=90)
                        elif udt_mode == "partial":
                            expected_udt_unit = NullablePlayer(age=90)
                        elif udt_mode == "empty":
                            expected_udt_unit = NullablePlayer()
                        elif udt_mode == "extended":
                            expected_udt_unit = ExtendedPlayer(
                                name="John",
                                age=90,
                                # TODO NOBLOBINUDT
                                # blb=THE_BYTES,
                                ts=THE_DATETIME,
                            )
                        else:
                            raise ValueError(f"Unknown udt mode '{udt_mode}'.")
                    else:
                        if udt_mode == "simple":
                            expected_udt_unit = {
                                "name": "John",
                                "age": 90,
                            }
                        elif udt_mode == "partial":
                            expected_udt_unit = {"age": 90}
                        elif udt_mode == "empty":
                            expected_udt_unit = {}
                        elif udt_mode == "extended":
                            expected_udt_unit = {
                                "name": "John",
                                "age": 90,
                                # TODO NOBLOBINUDT
                                # "blb": THE_BYTES,
                                "ts": THE_DATETIME,
                            }
                        else:
                            raise ValueError(f"Unknown udt mode '{udt_mode}'.")
                    exp_row = {
                        # TODO: reflects the behaviour for 'whole-null' udt:
                        "scalar_udt": expected_udt_unit
                        if udt_mode != "empty"
                        else None,
                        "list_udt": [expected_udt_unit],
                        "map_udt": {"k": expected_udt_unit},
                    }

                # table for reading
                reader_atable = atable.with_options(
                    api_options=APIOptions(
                        serdes_options=SerdesOptions(
                            custom_datatypes_in_reading=custom_datatypes_in_reading,
                            deserializer_by_udt=dict(deserializer_by_udt),
                            datetime_tzinfo=THE_TIMEZONE,
                        ),
                    ),
                )

                # read and check results
                row1 = await reader_atable.find_one(
                    {"id": "row_custom_colls"},
                    projection=projection,
                )
                row2 = await reader_atable.find_one(
                    {"id": "row_stdlib_colls"},
                    projection=projection,
                )
                dict_equal_same_class(row1, row2)
                dict_equal_same_class(row1, exp_row)

                # projection-based reads
                for fld in exp_row.keys() - projection.keys():
                    prj_read_row = await reader_atable.find_one(
                        {"id": "row_custom_colls"},
                        projection={fld: True},
                    )
                    dict_equal_same_class(prj_read_row, {fld: exp_row[fld]})

    @pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
    @pytest.mark.describe("Test of weird UDT columns, async")
    async def test_table_udt_weirdcolumns_async(
        self,
        cql_session: Session,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            for cql_statement in WEIRD_UDT_BASE_INITIALIZE_STATEMENTS:
                cql_session.execute(cql_statement)

            # test a read and a write for 'base weird'
            atable_weird_base = async_database.get_table(WEIRD_UDT_BASE_TABLE_NAME)
            ins_result = await atable_weird_base.insert_one(WEIRD_BASE_DOCUMENT)
            assert ins_result.inserted_id == WEIRD_BASE_DOCUMENT_PK
            doc_weird_base = await atable_weird_base.find_one(WEIRD_BASE_DOCUMENT_PK)
            assert doc_weird_base == WEIRD_BASE_DOCUMENT
        finally:
            for cql_statement in WEIRD_UDT_BASE_CLOSE_STATEMENTS:
                cql_session.execute(cql_statement)
