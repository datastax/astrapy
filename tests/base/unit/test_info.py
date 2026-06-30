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

"""
Unit tests for the parsing of API endpoints and related
"""

from __future__ import annotations

import pytest

from astrapy.admin import ParsedAPIEndpoint, parse_api_endpoint
from astrapy.info import (
    AstraDBAvailableRegionInfo,
    DatabaseDefinition,
    PCUGroupDescriptor,
    PCUGroupTypeDescriptor,
    PCUGroupTypeDetailsDescriptor,
)
from astrapy.settings.defaults import (
    DEFAULT_CREATE_DB_CAPACITY_UNITS,
    DEFAULT_CREATE_DB_DB_TYPE,
    DEFAULT_CREATE_DB_TIER,
)

from ..admin_assets import (
    MINIMAL_PCU_GROUP_DESCRIPTOR,
    MINIMAL_PCU_GROUP_TYPE_DESCRIPTOR,
    MINIMAL_PCU_GROUP_TYPE_DETAILS_DESCRIPTOR,
    SOME_PCU_GROUP_DESC_JSON,
    SOME_PCU_GROUP_DESC_JSON_NORESERVED,
    SOME_PCU_GROUP_DESCRIPTOR_KWARGS,
    SOME_PCU_GROUP_DESCRIPTOR_KWARGS_NORESERVED,
)


@pytest.mark.describe("test of parsing API endpoints")
def test_parse_api_endpoint() -> None:
    parsed_prod = parse_api_endpoint(
        "https://01234567-89ab-cdef-0123-456789abcdef-eu-west-1.apps.astra.datastax.com"
    )
    assert isinstance(parsed_prod, ParsedAPIEndpoint)
    assert parsed_prod == ParsedAPIEndpoint(
        database_id="01234567-89ab-cdef-0123-456789abcdef",
        region="eu-west-1",
        environment="prod",
    )

    parsed_prod = parse_api_endpoint(
        "https://a1234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-dev.datastax.com"
    )
    assert isinstance(parsed_prod, ParsedAPIEndpoint)
    assert parsed_prod == ParsedAPIEndpoint(
        database_id="a1234567-89ab-cdef-0123-456789abcdef",
        region="us-central1",
        environment="dev",
    )

    parsed_prod = parse_api_endpoint(
        "https://b1234567-89ab-cdef-0123-456789abcdef-eu-southwest-4.apps.astra-test.datastax.com/subpath?a=1"
    )
    assert isinstance(parsed_prod, ParsedAPIEndpoint)
    assert parsed_prod == ParsedAPIEndpoint(
        database_id="b1234567-89ab-cdef-0123-456789abcdef",
        region="eu-southwest-4",
        environment="test",
    )

    malformed_endpoints = [
        "http://01234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-dev.datastax.com",
        "https://a909bdbf-q9ba-4e5e-893c-a859ed701407-us-central1.apps.astra-dev.datastax.com",
        "https://01234567-89ab-cdef-0123-456789abcdef-us-c_entral1.apps.astra-dev.datastax.com",
        "https://01234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-fake.datastax.com",
        "https://01234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-dev.datastax-staging.com",
        "ahttps://01234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-dev.datastax.com",
    ]

    for m_ep in malformed_endpoints:
        assert parse_api_endpoint(m_ep) is None


@pytest.mark.describe("test of marshaling of available region info")
def test_parse_availableregioninfo() -> None:
    region_dict = {
        "classification": "standard",
        "cloudProvider": "AWS",
        "displayName": "US East (Ohio)",
        "enabled": True,
        "name": "us-east-2",
        "region_type": "vector",
        "reservedForQualifiedUsers": False,
        "zone": "na",
    }
    assert AstraDBAvailableRegionInfo._from_dict(region_dict).as_dict() == region_dict

    rich_region_dict = {
        "classification": "standard",
        "cloudProvider": "AWS",
        "displayName": "US East (Ohio)",
        "enabled": True,
        "name": "us-east-2",
        "region_type": "vector",
        "reservedForQualifiedUsers": False,
        "zone": "na",
        "pcu_types": [
            {
                "type": "vector",
                "details": {
                    "vCPU": 123,
                    "memory": "77KB",
                    "disk_cache": "17PB",
                },
            },
        ],
    }
    assert (
        AstraDBAvailableRegionInfo._from_dict(rich_region_dict).as_dict()
        == rich_region_dict
    )


@pytest.mark.describe("test of marshaling and unmarshaling of database definition")
def test_parse_databasedefinition() -> None:
    def_payload0 = {
        "name": "the_name0",
        "cloudProvider": "the_cloudProvider0",
        "region": "the_region0",
    }
    def_payload1 = {
        "name": "the_name1",
        "cloudProvider": "the_cloudProvider1",
        "region": "the_region1",
        "tier": "the_tier1",
        "capacityUnits": 9999,
        "dbType": "the_dbType1",
        "keyspace": "the_keyspace1",
        "pcuGroupUUID": "the_pcuGroupUUID1",
    }

    # _from_dict + default, dict match test

    db_def0 = DatabaseDefinition._from_dict(def_payload0)
    db_def1 = DatabaseDefinition._from_dict(def_payload1)
    expected_pload0 = {
        "name": "the_name0",
        "cloudProvider": "the_cloudProvider0",
        "region": "the_region0",
        "tier": DEFAULT_CREATE_DB_TIER,
        "capacityUnits": DEFAULT_CREATE_DB_CAPACITY_UNITS,
        "dbType": DEFAULT_CREATE_DB_DB_TYPE,
    }
    expected_pload1 = {
        "name": "the_name1",
        "cloudProvider": "the_cloudProvider1",
        "region": "the_region1",
        "tier": "the_tier1",
        "capacityUnits": 9999,
        "dbType": "the_dbType1",
        "keyspace": "the_keyspace1",
        "pcuGroupUUID": "the_pcuGroupUUID1",
    }
    assert db_def0.with_defaults().as_dict(name="the_name0") == expected_pload0
    assert db_def1.with_defaults().as_dict(name="the_name1") == expected_pload1

    # instance match test

    built_def0 = DatabaseDefinition(
        cloud_provider="the_cloudProvider0",
        region="the_region0",
    )
    built_def1 = DatabaseDefinition(
        cloud_provider="the_cloudProvider1",
        region="the_region1",
        tier="the_tier1",
        capacity_units=9999,
        db_type="the_dbType1",
        keyspace="the_keyspace1",
        pcu_group_id="the_pcuGroupUUID1",
    )
    assert built_def0 == db_def0
    assert built_def1 == db_def1


@pytest.mark.describe(
    "test of marshaling and unmarshaling of full PCU Group descriptors"
)
def test_parse_full_pcugroupdescriptor() -> None:
    pcugt_json = {
        "uuid": "the_id",
        **SOME_PCU_GROUP_DESC_JSON,
    }
    gtype_desc = PCUGroupDescriptor(
        id="the_id",
        **SOME_PCU_GROUP_DESCRIPTOR_KWARGS,  # type: ignore[arg-type]
    )

    assert gtype_desc.as_dict() == pcugt_json
    assert PCUGroupDescriptor._from_dict(pcugt_json) == gtype_desc


@pytest.mark.describe(
    "test of marshaling and unmarshaling of no-reserved PCU Group descriptors"
)
def test_parse_noreserved_pcugroupdescriptor() -> None:
    the_id = "84c06b4a-cb01-4a56-aa81-a158dc946833"

    pcugt_json = {
        "uuid": the_id,
        **SOME_PCU_GROUP_DESC_JSON_NORESERVED,
    }
    gtype_desc = PCUGroupDescriptor(
        id=the_id,
        **SOME_PCU_GROUP_DESCRIPTOR_KWARGS_NORESERVED,  # type: ignore[arg-type]
    )

    assert gtype_desc.as_dict() == pcugt_json
    assert PCUGroupDescriptor._from_dict(pcugt_json) == gtype_desc


@pytest.mark.describe(
    "test of marshaling and unmarshaling of minimal PCU Group descriptor objects"
)
def test_parse_minimal_pcugroupobjects() -> None:
    assert (
        PCUGroupDescriptor._from_dict(MINIMAL_PCU_GROUP_DESCRIPTOR).as_dict()
        == MINIMAL_PCU_GROUP_DESCRIPTOR
    )
    assert (
        PCUGroupTypeDescriptor._from_dict(MINIMAL_PCU_GROUP_TYPE_DESCRIPTOR).as_dict()
        == MINIMAL_PCU_GROUP_TYPE_DESCRIPTOR
    )
    assert (
        PCUGroupTypeDetailsDescriptor._from_dict(
            MINIMAL_PCU_GROUP_TYPE_DETAILS_DESCRIPTOR
        ).as_dict()
        == MINIMAL_PCU_GROUP_TYPE_DETAILS_DESCRIPTOR
    )
