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

from astrapy.api_options import APIOptions

from ..conftest import DefaultAsyncCollection, DefaultCollection
from ..decimal_support_assets import (
    BASELINE_OBJ,
    OBJ_W_DECIMALS,
    S_OPTS_NO_DECS,
    S_OPTS_OK_DECS,
    is_decimal_super,
)


class TestCollectionDecimalSupportIntegration:
    @pytest.mark.describe(
        "test of decimals not supported by default in collections, sync"
    )
    def test_decimalsupport_collections_defaultsettings_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        # write-and-read, baseline
        no_decimal_coll = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=S_OPTS_NO_DECS,
            ),
        )
        no_decimal_coll.insert_one(BASELINE_OBJ)
        baseline_obj_2 = no_decimal_coll.find_one({"_id": BASELINE_OBJ["_id"]})
        assert baseline_obj_2 is not None
        assert BASELINE_OBJ == baseline_obj_2

        # the write should error for object with decimals
        with pytest.raises(TypeError):
            no_decimal_coll.insert_one(OBJ_W_DECIMALS)

    @pytest.mark.describe(
        "test of decimals not supported by default in collections, async"
    )
    async def test_decimalsupport_collections_defaultsettings_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        # write-and-read, baseline
        no_decimal_acoll = async_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=S_OPTS_NO_DECS,
            ),
        )
        await no_decimal_acoll.insert_one(BASELINE_OBJ)
        baseline_obj_2 = await no_decimal_acoll.find_one({"_id": BASELINE_OBJ["_id"]})
        assert baseline_obj_2 is not None
        assert BASELINE_OBJ == baseline_obj_2

        # the write should error for object with decimals
        with pytest.raises(TypeError):
            await no_decimal_acoll.insert_one(OBJ_W_DECIMALS)

    @pytest.mark.describe(
        "test of decimals supported in collections if set to do so, sync"
    )
    def test_decimalsupport_collections_decimalsettings_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        # write-and-read, baseline
        ok_decimal_coll = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=S_OPTS_OK_DECS,
            ),
        )
        ok_decimal_coll.insert_one(BASELINE_OBJ)
        baseline_obj_2 = ok_decimal_coll.find_one({"_id": BASELINE_OBJ["_id"]})
        assert baseline_obj_2 is not None
        assert is_decimal_super(baseline_obj_2, BASELINE_OBJ)

        # write-and-read, with decimals
        ok_decimal_coll.insert_one(OBJ_W_DECIMALS)
        wdecs_2 = ok_decimal_coll.find_one({"_id": OBJ_W_DECIMALS["_id"]})
        assert wdecs_2 is not None
        assert is_decimal_super(wdecs_2, OBJ_W_DECIMALS)

    @pytest.mark.describe(
        "test of decimals supported in collections if set to do so, async"
    )
    async def test_decimalsupport_collections_decimalsettings_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        # write-and-read, baseline
        ok_decimal_acoll = async_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=S_OPTS_OK_DECS,
            ),
        )
        await ok_decimal_acoll.insert_one(BASELINE_OBJ)
        baseline_obj_2 = await ok_decimal_acoll.find_one({"_id": BASELINE_OBJ["_id"]})
        assert baseline_obj_2 is not None
        assert is_decimal_super(baseline_obj_2, BASELINE_OBJ)

        # write-and-read, with decimals
        await ok_decimal_acoll.insert_one(OBJ_W_DECIMALS)
        wdecs_2 = await ok_decimal_acoll.find_one({"_id": OBJ_W_DECIMALS["_id"]})
        assert wdecs_2 is not None
        assert is_decimal_super(wdecs_2, OBJ_W_DECIMALS)
