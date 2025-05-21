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
from astrapy.info import AstraDBAvailableRegionInfo


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
