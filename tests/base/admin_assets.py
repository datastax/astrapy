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
from typing import Any

from astrapy.info import (
    PCUGroupTypeDescriptor,
    PCUGroupTypeDetailsDescriptor,
)

SOME_PCU_GROUP_DESC_JSON = {
    "orgId": "the_org_id",
    "title": "the_title",
    "cloudProvider": "the_cloud_provider",
    "region": "the_region",
    "instanceType": "the_instance_type",
    "pcuType": {
        "type": "the_type2",
        "region": "the_region2",
        "provider": "the_cloud_provider2",
        "details": {
            "vCPU": 40,
            "memory": "the_memory3",
            "disk_cache": "the_disk_cache3",
        },
    },
    "provisionType": "the_provision_type",
    "min": 10,
    "max": 20,
    "reserved": 15,
    "description": "the_description",
    "createdAt": "2021-06-01T12:00:00.000Z",
    "updatedAt": "2025-06-01T12:00:00.000Z",
    "createdBy": "the_created_by",
    "updatedBy": "the_updated_by",
    "status": "the_status",
}

SOME_PCU_GROUP_DESCRIPTOR_KWARGS = {
    "org_id": "the_org_id",
    "title": "the_title",
    "cloud_provider": "the_cloud_provider",
    "region": "the_region",
    "instance_type": "the_instance_type",
    "pcu_type": PCUGroupTypeDescriptor(
        type="the_type2",
        region="the_region2",
        cloud_provider="the_cloud_provider2",
        details=PCUGroupTypeDetailsDescriptor(
            v_cpu=40,
            memory="the_memory3",
            disk_cache="the_disk_cache3",
        ),
    ),
    "provision_type": "the_provision_type",
    "min": 10,
    "max": 20,
    "reserved": 15,
    "description": "the_description",
    "created_at": datetime.datetime(2021, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
    "updated_at": datetime.datetime(2025, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
    "created_by": "the_created_by",
    "updated_by": "the_updated_by",
    "status": "the_status",
}

SOME_PCU_GROUP_DESC_JSON_NORESERVED = {
    "orgId": "76c5ba4c-b4e3-40a6-86ca-18eeaab510e7",
    "title": "test_pcu_group",
    "cloudProvider": "AWS",
    "region": "us-west-2",
    "instanceType": "small",
    "pcuType": {
        "type": "small",
        "region": "us-west-2",
        "provider": "aws",
        "details": {"vCPU": 4, "memory": "16GB", "disk_cache": "125GB"},
    },
    "provisionType": "shared",
    "min": 1,
    "max": 1,
    "description": "A transient PCU group for testing the client",
    "createdAt": "2026-06-23T21:12:06.000Z",
    "updatedAt": "2026-06-23T21:12:06.000Z",
    "createdBy": "umWGYUwuxFvQKBKPAHwHjKef",
    "updatedBy": "umWGYUwuxFvQKBKPAHwHjKef",
    "status": "CREATED",
}

SOME_PCU_GROUP_DESCRIPTOR_KWARGS_NORESERVED = {
    "org_id": "76c5ba4c-b4e3-40a6-86ca-18eeaab510e7",
    "title": "test_pcu_group",
    "cloud_provider": "AWS",
    "region": "us-west-2",
    "instance_type": "small",
    "pcu_type": PCUGroupTypeDescriptor(
        type="small",
        region="us-west-2",
        cloud_provider="aws",
        details=PCUGroupTypeDetailsDescriptor(
            v_cpu=4,
            memory="16GB",
            disk_cache="125GB",
        ),
    ),
    "provision_type": "shared",
    "min": 1,
    "max": 1,
    "description": "A transient PCU group for testing the client",
    "created_at": datetime.datetime(
        2026, 6, 23, 21, 12, 6, tzinfo=datetime.timezone.utc
    ),
    "updated_at": datetime.datetime(
        2026, 6, 23, 21, 12, 6, tzinfo=datetime.timezone.utc
    ),
    "created_by": "umWGYUwuxFvQKBKPAHwHjKef",
    "updated_by": "umWGYUwuxFvQKBKPAHwHjKef",
    "status": "CREATED",
}

MINIMAL_PCU_GROUP_DESCRIPTOR = {
    "uuid": "minimal_pgd_id",
    "cloudProvider": "AWS",
    "region": "us-west-2",
}

MINIMAL_PCU_GROUP_TYPE_DESCRIPTOR = {
    "type": "small",
}

MINIMAL_PCU_GROUP_TYPE_DETAILS_DESCRIPTOR: dict[str, Any] = {}
