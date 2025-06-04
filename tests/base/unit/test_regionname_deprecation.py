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
from deprecation import DeprecatedWarning

from astrapy.data.info.database_info import (
    AstraDBAdminDatabaseRegionInfo,
    AstraDBAvailableRegionInfo,
)

from ..conftest import is_future_version


class TestRegionNameDeprecation:
    @pytest.mark.describe("test of region_name in AstraDBAdminDatabaseRegionInfo")
    def test_regionname_dbregioninfo(self) -> None:
        dc0 = {
            "capacityUnits": 1,
            "cloudProvider": "AWS",
            "dateCreated": "2025-04-10T19:14:44Z",
            "id": "...",
            "isPrimary": True,
            "name": "dc-1",
            "region": "us-east-2",
            "regionClassification": "standard",
            "regionZone": "na",
            "requestedNodeCount": 3,
            "secureBundleInternalUrl": "...",
            "secureBundleMigrationProxyInternalUrl": "...",
            "secureBundleMigrationProxyUrl": "...",
            "secureBundleUrl": "...",
            "status": "",
            "streamingTenant": {},
            "targetAccount": "abcd0123",
            "tier": "serverless",
        }
        region_info = AstraDBAdminDatabaseRegionInfo(
            raw_datacenter_dict=dc0,
            environment="dev",
            database_id="D",
        )

        r0 = region_info.region
        with pytest.warns(DeprecationWarning) as w_checker:
            r1 = region_info.region_name
        assert len(w_checker.list) == 1
        warning0 = w_checker.list[0].message
        assert isinstance(warning0, DeprecatedWarning)
        assert is_future_version(warning0.removed_in)

        assert r0 == r1

    @pytest.mark.describe("test of region_name in AstraDBAvailableRegionInfo")
    def test_regionname_availableregion(self) -> None:
        ar0 = {
            "classification": "premium",
            "cloudProvider": "GCP",
            "displayName": "West Europe3 (Frankfurt, Germany)",
            "enabled": True,
            "name": "europe-west3",
            "region_type": "serverless",
            "reservedForQualifiedUsers": True,
            "zone": "emea",
        }
        available_region_info = AstraDBAvailableRegionInfo._from_dict(ar0)

        r0 = available_region_info.region
        with pytest.warns(DeprecationWarning) as w_checker:
            r1 = available_region_info.region_name

        assert len(w_checker.list) == 1
        warning0 = w_checker.list[0].message
        assert isinstance(warning0, DeprecatedWarning)
        assert is_future_version(warning0.removed_in)

        assert r0 == r1
