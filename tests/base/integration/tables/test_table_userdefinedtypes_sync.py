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
import time
from typing import TYPE_CHECKING, Any

import pytest

from astrapy import Database
from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import SerializerFunctionType, UDTDeserializerFunctionType
from astrapy.data.utils.table_types import TableUnsupportedColumnType
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
    DefaultTable,
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
    UNSUPPORTED_UDT_CREATE,
    UNSUPPORTED_UDT_DROP,
    UNSUPPORTED_UDT_NAME,
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

            assert UDT_NAME in sync_database.list_type_names()
            db_types_match = [
                t_def
                for t_def in sync_database.list_types()
                if t_def.udt_name == UDT_NAME
            ]
            assert db_types_match != []
            assert db_types_match[0].definition == UDT_DEF0

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
    @pytest.mark.describe("Test of UDT DML, sync")
    def test_table_udt_dml_sync(
        self,
        player_udt: str,
        extended_player_udt: str,
        sync_empty_table_udt_player: DefaultTable,
        sync_empty_table_udt_extended_player: DefaultTable,
        encode_maps_as_lists_in_tables: str,
        udt_format: str,
        udt_mode: str,
    ) -> None:
        """
        This is a test of reading/writing UDTs on tables, within various column types.
        The test is heavily parametrized. Each parameter is a different 'write mode',
        but each individual test runs the read with all relevant read modes.
        'Write modes' = serdes options, the type used to express the UDT, the content.

        Note all "udt_mode" use one table except the "extended" (different UDT schema).
        The scenario of UDT as plain dicts + always maps as lists of pairs is forbidden,
        hence skipped.

        So the count of tested param combinations is as follows:
            simple UDT (2 fields)               extended UDT (4 fields) | serdesMaps:
            ------------------------------------------------------------+------------
             dict: 6  | nondict: 12              dict: 2  | nondict:  4 | nonALWAYS
            [dict: 3] | nondict: 6              [dict: 1] | nondict:  2 |    ALWAYS
            ------------------------------------------------------------+------------
        """

        # combinations of write settings that are not supposed to work are skipped:
        if udt_format == "dict" and encode_maps_as_lists_in_tables == "ALWAYS":
            pytest.skip("The Data API is not supposed to accept such a write format.")

        # choice of serdes options for writes
        src_table: DefaultTable
        if udt_mode == "extended":
            src_table = sync_empty_table_udt_extended_player
        else:
            src_table = sync_empty_table_udt_player
        table = src_table.with_options(
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
        ins1c_result = table.insert_one(row_custom_colls)
        ins1s_result = table.insert_one(row_stdlib_colls)
        insm_result = table.insert_many([row_custom_colls, row_stdlib_colls])

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
                reader_table = table.with_options(
                    api_options=APIOptions(
                        serdes_options=SerdesOptions(
                            custom_datatypes_in_reading=custom_datatypes_in_reading,
                            deserializer_by_udt=dict(deserializer_by_udt),
                            datetime_tzinfo=THE_TIMEZONE,
                        ),
                    ),
                )

                # read and check results
                row1 = reader_table.find_one(
                    {"id": "row_custom_colls"},
                    projection=projection,
                )
                row2 = reader_table.find_one(
                    {"id": "row_stdlib_colls"},
                    projection=projection,
                )
                dict_equal_same_class(row1, row2)
                dict_equal_same_class(row1, exp_row)

                # projection-based reads
                for fld in exp_row.keys() - projection.keys():
                    prj_read_row = reader_table.find_one(
                        {"id": "row_custom_colls"},
                        projection={fld: True},
                    )
                    dict_equal_same_class(prj_read_row, {fld: exp_row[fld]})

    @pytest.mark.describe("Test of null/partial UDT in DML, sync")
    def test_table_incomplete_udt_dml_sync(
        self,
        player_udt: str,
        sync_empty_table_udt_player: DefaultTable,
    ) -> None:
        table = sync_empty_table_udt_player.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                )
            ),
        )
        insert_results = table.insert_many(
            [
                {
                    "id": "nulls",
                    "scalar_udt": None,
                    "list_udt": [{}],
                    "set_udt": DataAPISet([{}]),
                    "map_udt": DataAPIMap({"k": {}}),
                },
                {
                    "id": "partials",
                    "scalar_udt": {"age": 75},
                    "list_udt": [{"age": 75}],
                    "set_udt": DataAPISet([{"age": 75}]),
                    "map_udt": DataAPIMap({"k": {"age": 75}}),
                },
            ]
        )
        assert insert_results.inserted_id_tuples == [("nulls",), ("partials",)]

        row_nulls = table.find_one({"id": "nulls"})
        null_udt_dict = DataAPIDictUDT({"name": None, "age": None})
        expected_row_nulls = {
            "id": "nulls",
            "scalar_udt": null_udt_dict,
            "list_udt": [null_udt_dict],
            "set_udt": DataAPISet([null_udt_dict]),
            "map_udt": DataAPIMap({"k": null_udt_dict}),
        }
        dict_equal_same_class(row_nulls, expected_row_nulls)

        row_partials = table.find_one({"id": "partials"})
        partial_udt_dict = DataAPIDictUDT({"name": None, "age": 75})
        expected_row_partials = {
            "id": "partials",
            "scalar_udt": partial_udt_dict,
            "list_udt": [partial_udt_dict],
            "set_udt": DataAPISet([partial_udt_dict]),
            "map_udt": DataAPIMap({"k": partial_udt_dict}),
        }
        dict_equal_same_class(row_partials, expected_row_partials)

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
    @pytest.mark.describe("Test of unsupported UDT listing, sync")
    def test_table_udt_listunsupported_sync(
        self,
        cql_session: Session,
        sync_database: Database,
    ) -> None:
        try:
            cql_session.execute(UNSUPPORTED_UDT_CREATE)
            time.sleep(1.5)  # udt propagation requires some time, it seems

            assert UNSUPPORTED_UDT_NAME in sync_database.list_type_names()
            lt_result = sync_database.list_types()
            listed_udt_matches = [
                l_u
                for l_u in lt_result
                # sloppy matching
                if UNSUPPORTED_UDT_NAME in str(l_u.as_dict())
            ]
            assert len(listed_udt_matches) == 1
            listed_udt_match = listed_udt_matches[0]
            assert listed_udt_match.udt_type == TableUnsupportedColumnType.UNSUPPORTED
            assert listed_udt_match.api_support is not None
            assert UNSUPPORTED_UDT_NAME in listed_udt_match.api_support.cql_definition
        finally:
            cql_session.execute(UNSUPPORTED_UDT_DROP)

    @pytest.mark.skipif(
        "ASTRAPY_TEST_LATEST_MAIN" not in os.environ,
        reason="Currently available only on cutting-edge Data API `main`",
    )
    @pytest.mark.describe("Test of UDT filtering, sync")
    def test_table_udt_filtering_sync(
        self,
        sync_database: Database,
        sync_empty_table_udtcollindexed: DefaultTable,
    ) -> None:
        table = sync_empty_table_udtcollindexed
        u_dict = DataAPIDictUDT({"name": "Otto", "age": 8})
        table.insert_one(
            {
                "id": "fullrow",
                "scalar_udt": u_dict,
                "set_udt": DataAPISet([u_dict]),
                "list_udt": [u_dict],
                "map_text_udt_e": {"ke": u_dict},
                "map_text_udt_v": {"kv": u_dict},
            }
        )

        # running different filtering patterns with UDTs
        x_dict = DataAPIDictUDT({"name": "Ada", "age": 8})
        prj = {"id": True}

        with pytest.raises(DataAPIResponseException):
            table.find_one(filter={"scalar_udt": u_dict})

        su_row0 = table.find_one(
            filter={"set_udt": {"$in": [u_dict, x_dict]}}, projection=prj
        )
        su_row1 = table.find_one(filter={"set_udt": {"$nin": [x_dict]}}, projection=prj)
        su_rown0 = table.find_one(filter={"set_udt": {"$in": [x_dict]}}, projection=prj)
        su_rown1 = table.find_one(
            filter={"set_udt": {"$nin": [x_dict, u_dict]}}, projection=prj
        )
        assert su_row0 is not None and su_row0["id"] == "fullrow"
        assert su_row1 is not None and su_row1["id"] == "fullrow"
        assert su_rown0 is None
        assert su_rown1 is None

        lu_row0 = table.find_one(
            filter={"list_udt": {"$in": [u_dict, x_dict]}}, projection=prj
        )
        lu_row1 = table.find_one(
            filter={"list_udt": {"$nin": [x_dict]}}, projection=prj
        )
        lu_rown0 = table.find_one(
            filter={"list_udt": {"$in": [x_dict]}}, projection=prj
        )
        lu_rown1 = table.find_one(
            filter={"list_udt": {"$nin": [x_dict, u_dict]}}, projection=prj
        )
        assert lu_row0 is not None and lu_row0["id"] == "fullrow"
        assert lu_row1 is not None and lu_row1["id"] == "fullrow"
        assert lu_rown0 is None
        assert lu_rown1 is None

        meu_row0 = table.find_one(
            filter={"map_text_udt_e": {"$in": [["ke", u_dict], ["ke", x_dict]]}},
            projection=prj,
        )
        meu_row1 = table.find_one(
            filter={"map_text_udt_e": {"$nin": [["ke", x_dict]]}}, projection=prj
        )
        meu_rown0 = table.find_one(
            filter={"map_text_udt_e": {"$in": [["KE", u_dict], ["ke", x_dict]]}},
            projection=prj,
        )
        meu_rown1 = table.find_one(
            filter={"map_text_udt_e": {"$nin": [["ke", u_dict], ["ke", x_dict]]}},
            projection=prj,
        )
        assert meu_row0 is not None and meu_row0["id"] == "fullrow"
        assert meu_row1 is not None and meu_row1["id"] == "fullrow"
        assert meu_rown0 is None
        assert meu_rown1 is None

        mvu_row0 = table.find_one(
            filter={"map_text_udt_v": {"$values": {"$in": [u_dict, x_dict]}}},
            projection=prj,
        )
        mvu_row1 = table.find_one(
            filter={"map_text_udt_v": {"$values": {"$nin": [x_dict]}}},
            projection=prj,
        )
        mvu_row2 = table.find_one(
            filter={"map_text_udt_v": {"$values": {"$all": [u_dict]}}},
            projection=prj,
        )
        mvu_rown0 = table.find_one(
            filter={"map_text_udt_v": {"$values": {"$in": [x_dict]}}},
            projection=prj,
        )
        mvu_rown1 = table.find_one(
            filter={"map_text_udt_v": {"$values": {"$nin": [u_dict, x_dict]}}},
            projection=prj,
        )
        mvu_rown2 = table.find_one(
            filter={"map_text_udt_v": {"$values": {"$all": [u_dict, x_dict]}}},
            projection=prj,
        )
        assert mvu_row0 is not None and mvu_row0["id"] == "fullrow"
        assert mvu_row1 is not None and mvu_row1["id"] == "fullrow"
        assert mvu_row2 is not None and mvu_row2["id"] == "fullrow"
        assert mvu_rown0 is None
        assert mvu_rown1 is None
        assert mvu_rown2 is None

        # using partial UDTs for filtering in various ways
        p_dict = DataAPIDictUDT({"age": 8})

        with pytest.raises(DataAPIResponseException):
            table.find_one(filter={"scalar_udt": p_dict})

        psu_rown0 = table.find_one(filter={"set_udt": {"$in": [p_dict]}})
        assert psu_rown0 is None
        plu_rown0 = table.find_one(filter={"list_udt": {"$in": [p_dict]}})
        assert plu_rown0 is None
        pmeu_rown0 = table.find_one(filter={"map_text_udt_e": {"$in": [["h", p_dict]]}})
        assert pmeu_rown0 is None
        pmvu_rown0 = table.find_one(
            filter={"map_text_udt_e": {"$values": {"$in": [p_dict]}}}
        )
        assert pmvu_rown0 is None
        pmvu_rown1 = table.find_one(
            filter={"map_text_udt_e": {"$values": {"$all": [p_dict]}}}
        )
        assert pmvu_rown1 is None

        table.insert_one(
            {
                "id": "withnulls",
                "scalar_udt": p_dict,
                "set_udt": DataAPISet([p_dict]),
                "list_udt": [p_dict],
                "map_text_udt_e": {"he": p_dict},
                "map_text_udt_v": {"hv": p_dict},
            }
        )

        psu_row0 = table.find_one(filter={"set_udt": {"$in": [p_dict]}}, projection=prj)
        assert psu_row0 is not None and psu_row0["id"] == "withnulls"
        plu_row0 = table.find_one(
            filter={"list_udt": {"$in": [p_dict]}}, projection=prj
        )
        assert plu_row0 is not None and plu_row0["id"] == "withnulls"
        pmeu_row0 = table.find_one(
            filter={"map_text_udt_e": {"$in": [["he", p_dict]]}}, projection=prj
        )
        assert pmeu_row0 is not None and pmeu_row0["id"] == "withnulls"
        pmvu_row0 = table.find_one(
            filter={"map_text_udt_e": {"$values": {"$in": [p_dict]}}}, projection=prj
        )
        assert pmvu_row0 is not None and pmvu_row0["id"] == "withnulls"
        pmvu_row1 = table.find_one(
            filter={"map_text_udt_e": {"$values": {"$all": [p_dict]}}}, projection=prj
        )
        assert pmvu_row1 is not None and pmvu_row1["id"] == "withnulls"
