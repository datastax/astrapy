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

from astrapy import DataAPIClient

from ..conftest import IS_ASTRA_DB, DataAPICredentials


class TestFindRegionsAsync:
    @pytest.mark.skipif(not IS_ASTRA_DB, reason="This test requires Astra DB")
    @pytest.mark.describe("test of find_available_regions, async")
    async def test_findavailableregions_async(
        self,
        client: DataAPIClient,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        admin = client.get_admin(token=data_api_credentials_kwargs["token"])

        ar0 = await admin.async_find_available_regions()
        art = await admin.async_find_available_regions(only_org_enabled_regions=True)
        arf = await admin.async_find_available_regions(only_org_enabled_regions=False)

        assert ar0 == art
        assert len(arf) >= len(art)
        assert all(reg in arf for reg in art)
