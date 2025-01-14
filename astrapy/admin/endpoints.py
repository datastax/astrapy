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

import re
from dataclasses import dataclass

from astrapy.settings.defaults import (
    API_ENDPOINT_TEMPLATE_ENV_MAP,
)

database_id_matcher = re.compile(
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

api_endpoint_parser = re.compile(
    r"https://"
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    r"-"
    r"([a-z0-9\-]+)"
    r".apps.astra[\-]{0,1}"
    r"(dev|test)?"
    r".datastax.com"
)
api_endpoint_description = (
    "https://<db uuid, 8-4-4-4-12 hex format>-<db region>.apps.astra.datastax.com"
)

generic_api_url_matcher = re.compile(r"^https?:\/\/[a-zA-Z0-9\-.]+(\:[0-9]{1,6}){0,1}$")
generic_api_url_descriptor = "http[s]://<domain name or IP>[:port]"


@dataclass
class ParsedAPIEndpoint:
    """
    The results of successfully parsing an Astra DB API endpoint, for internal
    by database metadata-related functions.

    Attributes:
        database_id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        region: a region ID, such as "us-west1".
        environment: a label, whose value is one of Environment.PROD,
            Environment.DEV or Environment.TEST.
    """

    database_id: str
    region: str
    environment: str


def parse_api_endpoint(api_endpoint: str) -> ParsedAPIEndpoint | None:
    """
    Parse an API Endpoint into a ParsedAPIEndpoint structure.

    Args:
        api_endpoint: a full API endpoint for the Data API.

    Returns:
        The parsed ParsedAPIEndpoint. If parsing fails, return None.
    """

    match = api_endpoint_parser.match(api_endpoint)
    if match and match.groups():
        d_id, d_re, d_en_x = match.groups()
        return ParsedAPIEndpoint(
            database_id=d_id,
            region=d_re,
            environment=d_en_x if d_en_x else "prod",
        )
    else:
        return None


def api_endpoint_parsing_error_message(failing_url: str) -> str:
    """
    Format an error message with a suggestion for the expected url format.
    """
    return (
        f"Cannot parse the supplied API endpoint ({failing_url}). The endpoint "
        f'must be in the following form: "{api_endpoint_description}".'
    )


def api_endpoint_parsing_cdinfo_message(failing_url: str) -> str:
    """
    Format a warning message about a possibly-custom-domain API endpoint.
    """
    return (
        f"An API endpoint was supplied ({failing_url}) that does not conform to the "
        f'standard form ("{api_endpoint_description}"). It will be assumed that this '
        "is intentional, i.e. that the desired API endpoint is a 'Custom Domain'."
    )


def parse_generic_api_url(api_endpoint: str) -> str | None:
    """
    Validate a generic API Endpoint string,
    such as `http://10.1.1.1:123` or `https://my.domain`.

    Args:
        api_endpoint: a string supposedly expressing a valid API Endpoint
        (not necessarily an Astra DB one).

    Returns:
        a normalized (stripped) version of the endpoint if valid. If invalid,
        return None.
    """
    if api_endpoint and api_endpoint[-1] == "/":
        _api_endpoint = api_endpoint[:-1]
    else:
        _api_endpoint = api_endpoint
    match = generic_api_url_matcher.match(_api_endpoint)
    if match:
        return match[0].rstrip("/")
    else:
        return None


def generic_api_url_parsing_error_message(failing_url: str) -> str:
    """
    Format an error message with a suggestion for the expected url format.
    """
    return (
        f"Cannot parse the supplied API endpoint ({failing_url}). The endpoint "
        f'must be in the following form: "{generic_api_url_descriptor}".'
    )


def build_api_endpoint(environment: str, database_id: str, region: str) -> str:
    """
    Build the API Endpoint full strings from database parameters.

    Args:
        environment: a label, whose value can be Environment.PROD
            or another of Environment.* for which this operation makes sense.
        database_id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        region: a region ID, such as "us-west1".

    Returns:
        the endpoint string, such as "https://01234567-...-eu-west1.apps.datastax.com"
    """

    return API_ENDPOINT_TEMPLATE_ENV_MAP[environment].format(
        database_id=database_id,
        region=region,
    )
