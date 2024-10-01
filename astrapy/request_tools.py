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

import logging
from typing import Any, TypedDict, Union

import httpx

from astrapy.defaults import DEFAULT_REQUEST_TIMEOUT_MS

logger = logging.getLogger(__name__)


def log_httpx_request(
    http_method: str,
    full_url: str,
    request_params: dict[str, Any] | None,
    redacted_request_headers: dict[str, str],
    payload: dict[str, Any] | None,
) -> None:
    """
    Log the details of an HTTP request for debugging purposes.

    Args:
        http_method: the HTTP verb of the request (e.g. "POST").
        full_url: the URL of the request (e.g. "https://domain.com/full/path").
        request_params: parameters of the request.
        redacted_request_headers: caution, as these will be logged as they are.
        payload: The payload sent with the request, if any.
    """
    logger.debug(f"Request URL: {http_method} {full_url}")
    if request_params:
        logger.debug(f"Request params: {request_params}")
    if redacted_request_headers:
        logger.debug(f"Request headers: {redacted_request_headers}")
    if payload:
        logger.debug(f"Request payload: {payload}")


def log_httpx_response(response: httpx.Response) -> None:
    """
    Log the details of an httpx.Response.

    Args:
        response: the httpx.Response object to log.
    """
    logger.debug(f"Response status code: {response.status_code}")
    logger.debug(f"Response headers: {response.headers}")
    logger.debug(f"Response text: {response.text}")


class HttpMethod:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class TimeoutInfo(TypedDict, total=False):
    read: float
    write: float
    base: float


TimeoutInfoWideType = Union[TimeoutInfo, float, None]


def to_httpx_timeout(timeout_info: TimeoutInfoWideType) -> httpx.Timeout | None:
    if timeout_info is None:
        return None
    if isinstance(timeout_info, float) or isinstance(timeout_info, int):
        return httpx.Timeout(timeout_info)
    elif isinstance(timeout_info, dict):
        _base = timeout_info.get("base") or DEFAULT_REQUEST_TIMEOUT_MS
        _read = timeout_info.get("read") or _base
        _write = timeout_info.get("write") or _base
        return httpx.Timeout(_base, read=_read, write=_write)
    else:
        raise ValueError("Invalid timeout info provided.")
