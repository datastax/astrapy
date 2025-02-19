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

from astrapy.api_options import APIOptions, SerdesOptions

from ..conftest import DefaultAsyncTable
from .table_row_assets import (
    ALLMAPS_CUSTOMTYPES_ROW,
    ALLMAPS_STDLIB_ROW,
)

STDLIB_OPTIONS = APIOptions(
    serdes_options=SerdesOptions(custom_datatypes_in_reading=False)
)


class TestTableMapsAsTuplesAsync:
    @pytest.mark.describe(
        "test of table maps-as-tuples, insert one and find one, async"
    )
    async def test_table_mapsastuples_insert_one_find_one_async(
        self,
        async_empty_table_allmaps: DefaultAsyncTable,
    ) -> None:
        customdt_atable = async_empty_table_allmaps
        stdlib_atable = async_empty_table_allmaps.with_options(
            api_options=STDLIB_OPTIONS
        )

        # custom types and corresponding serdes options
        cd_io_res = await customdt_atable.insert_one(ALLMAPS_CUSTOMTYPES_ROW)
        assert cd_io_res.inserted_id == {"id": ALLMAPS_CUSTOMTYPES_ROW["id"]}
        assert (
            await customdt_atable.find_one({"id": ALLMAPS_CUSTOMTYPES_ROW["id"]})
            == ALLMAPS_CUSTOMTYPES_ROW
        )

        # stdlib types and corresponding serdes options
        sl_io_res = await stdlib_atable.insert_one(ALLMAPS_STDLIB_ROW)
        assert sl_io_res.inserted_id == {"id": ALLMAPS_STDLIB_ROW["id"]}
        assert (
            await stdlib_atable.find_one({"id": ALLMAPS_STDLIB_ROW["id"]})
            == ALLMAPS_STDLIB_ROW
        )

    @pytest.mark.describe("test of table maps-as-tuples, insert many and find, async")
    async def test_table_mapsastuples_insert_many_find_async(
        self,
        async_empty_table_allmaps: DefaultAsyncTable,
    ) -> None:
        customdt_atable = async_empty_table_allmaps
        stdlib_atable = async_empty_table_allmaps.with_options(
            api_options=STDLIB_OPTIONS
        )

        # custom types and corresponding serdes options I: ordered insert
        cd_im_res = await customdt_atable.insert_many(
            [ALLMAPS_CUSTOMTYPES_ROW], ordered=True
        )
        assert cd_im_res.inserted_ids == [{"id": ALLMAPS_CUSTOMTYPES_ROW["id"]}]
        customdt_founds = await customdt_atable.find(
            {"id": ALLMAPS_CUSTOMTYPES_ROW["id"]}
        ).to_list()
        assert customdt_founds == [ALLMAPS_CUSTOMTYPES_ROW]

        # custom types and corresponding serdes options II: unordered, concurrency=1 insert
        await customdt_atable.delete_many({"id": ALLMAPS_CUSTOMTYPES_ROW["id"]})
        cd_im_res = await customdt_atable.insert_many(
            [ALLMAPS_CUSTOMTYPES_ROW], ordered=False, concurrency=1
        )
        assert cd_im_res.inserted_ids == [{"id": ALLMAPS_CUSTOMTYPES_ROW["id"]}]
        customdt_founds = await customdt_atable.find(
            {"id": ALLMAPS_CUSTOMTYPES_ROW["id"]}
        ).to_list()
        assert customdt_founds == [ALLMAPS_CUSTOMTYPES_ROW]

        # custom types and corresponding serdes options III: unordered, concurrency>1 insert
        await customdt_atable.delete_many({"id": ALLMAPS_CUSTOMTYPES_ROW["id"]})
        cd_im_res = await customdt_atable.insert_many(
            [ALLMAPS_CUSTOMTYPES_ROW], ordered=False, concurrency=2
        )
        assert cd_im_res.inserted_ids == [{"id": ALLMAPS_CUSTOMTYPES_ROW["id"]}]
        customdt_founds = await customdt_atable.find(
            {"id": ALLMAPS_CUSTOMTYPES_ROW["id"]}
        ).to_list()
        assert customdt_founds == [ALLMAPS_CUSTOMTYPES_ROW]

        # stdlib types and corresponding serdes options
        sl_im_res = await stdlib_atable.insert_many([ALLMAPS_STDLIB_ROW])
        assert sl_im_res.inserted_ids == [{"id": ALLMAPS_STDLIB_ROW["id"]}]
        stdlib_founds = await stdlib_atable.find(
            {"id": ALLMAPS_STDLIB_ROW["id"]}
        ).to_list()
        assert stdlib_founds == [ALLMAPS_STDLIB_ROW]

    @pytest.mark.describe(
        "test of table maps-as-tuples, update one and delete one, async"
    )
    async def test_table_mapsastuples_update_one_delete_one_async(
        self,
        async_empty_table_allmaps: DefaultAsyncTable,
    ) -> None:
        customdt_atable = async_empty_table_allmaps
        stdlib_atable = async_empty_table_allmaps.with_options(
            api_options=STDLIB_OPTIONS
        )

        # custom types and corresponding serdes options
        await customdt_atable.insert_one({"id": ALLMAPS_CUSTOMTYPES_ROW["id"]})
        await customdt_atable.update_one(
            {"id": ALLMAPS_CUSTOMTYPES_ROW["id"]},
            update={
                "$set": {k: v for k, v in ALLMAPS_CUSTOMTYPES_ROW.items() if k != "id"}
            },
        )
        assert (
            await customdt_atable.find_one({"id": ALLMAPS_CUSTOMTYPES_ROW["id"]})
            == ALLMAPS_CUSTOMTYPES_ROW
        )
        await customdt_atable.delete_one({"id": ALLMAPS_CUSTOMTYPES_ROW["id"]})
        assert (
            await customdt_atable.find_one({"id": ALLMAPS_CUSTOMTYPES_ROW["id"]})
            is None
        )

        # stdlib types and corresponding serdes options
        await stdlib_atable.insert_one({"id": ALLMAPS_STDLIB_ROW["id"]})
        await stdlib_atable.update_one(
            {"id": ALLMAPS_STDLIB_ROW["id"]},
            update={"$set": {k: v for k, v in ALLMAPS_STDLIB_ROW.items() if k != "id"}},
        )
        assert (
            await stdlib_atable.find_one({"id": ALLMAPS_STDLIB_ROW["id"]})
            == ALLMAPS_STDLIB_ROW
        )
        await stdlib_atable.delete_one({"id": ALLMAPS_STDLIB_ROW["id"]})
        assert await stdlib_atable.find_one({"id": ALLMAPS_STDLIB_ROW["id"]}) is None
