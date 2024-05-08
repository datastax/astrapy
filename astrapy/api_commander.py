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

from typing import Any, Dict, List, Optional, Tuple, cast

import json
import httpx

from astrapy.core.defaults import (
    DEFAULT_AUTH_HEADER,
    DEFAULT_DEV_OPS_AUTH_HEADER,
    DEFAULT_TIMEOUT,
    DEFAULT_VECTORIZE_SECRET_HEADER,
)
from astrapy.core.utils import (
    http_methods,
    logger,
    log_request,
    log_response,
    normalize_for_api,
    restore_from_api,
    TimeoutInfoWideType,
    to_httpx_timeout,
    user_agent_astrapy,
    user_agent_rs,
    user_agent_string,
)
from astrapy.exceptions import (
    DataAPIResponseException,
    DataAPIFaultyResponseException,
    to_dataapi_timeout_exception,
)


DEFAULT_REDACTED_HEADER_NAMES = [
    DEFAULT_AUTH_HEADER,
    DEFAULT_DEV_OPS_AUTH_HEADER,
    DEFAULT_VECTORIZE_SECRET_HEADER,
]


def full_user_agent(
    callers: List[Tuple[Optional[str], Optional[str]]]
) -> Optional[str]:
    regular_user_agents = [
        user_agent_string(caller[0], caller[1]) for caller in callers
    ]
    all_user_agents = [
        ua_block
        for ua_block in [user_agent_rs] + regular_user_agents + [user_agent_astrapy]
        if ua_block
    ]
    if all_user_agents:
        return " ".join(all_user_agents)
    else:
        return None


class APICommander:
    client = httpx.Client()
    async_client = httpx.AsyncClient()

    def __init__(
        self,
        api_endpoint: str,
        path: str,
        headers: Dict[str, str] = {},
        callers: List[Tuple[Optional[str], Optional[str]]] = [],
        redacted_header_names: List[str] = DEFAULT_REDACTED_HEADER_NAMES,
    ) -> None:
        self.api_endpoint = api_endpoint.rstrip("/")
        self.path = path.lstrip("/")
        self.headers = headers
        self.callers = callers
        self.redacted_header_names = set(redacted_header_names)

        user_agent = full_user_agent(self.callers)
        self.caller_header: Dict[str, str] = (
            {"User-Agent": user_agent} if user_agent else {}
        )
        self.full_headers: Dict[str, str] = {
            **self.headers,
            **self.caller_header,
        }
        self._loggable_headers = {
            k: v if k not in self.redacted_header_names else "***"
            for k, v in self.full_headers.items()
        }
        self.full_path = ("/".join([self.api_endpoint, self.path])).rstrip("/")

    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError

    def _copy(self) -> None:
        raise NotImplementedError

    def raw_request(
        self,
        *,
        http_method: str = http_methods.POST,
        payload: Optional[Dict[str, Any]] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> httpx.Response:
        timeout = to_httpx_timeout(timeout_info)
        normalized_payload = normalize_for_api(payload)
        # Log the request
        log_request(
            http_method,
            self.full_path,
            {},
            self._loggable_headers,
            normalized_payload,
        )
        encoded_payload = json.dumps(
            normalized_payload,
            allow_nan=False,
            separators=(",", ":"),
        ).encode()
        try:
            raw_response = self.client.request(
                method=http_method,
                url=self.full_path,
                content=encoded_payload,
                timeout=timeout or DEFAULT_TIMEOUT,
                headers=self.full_headers,
            )
        except httpx.TimeoutException as timeout_exc:
            raise to_dataapi_timeout_exception(timeout_exc)
        raw_response.raise_for_status()
        # Log the response before returning it
        log_response(raw_response)
        return raw_response

    async def async_raw_request(
        self,
        *,
        http_method: str = http_methods.POST,
        payload: Optional[Dict[str, Any]] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> httpx.Response:
        timeout = to_httpx_timeout(timeout_info)
        normalized_payload = normalize_for_api(payload)
        # Log the request
        log_request(
            http_method,
            self.full_path,
            {},
            self._loggable_headers,
            normalized_payload,
        )
        encoded_payload = json.dumps(
            normalized_payload,
            allow_nan=False,
            separators=(",", ":"),
        ).encode()
        raw_response = await self.async_client.request(
            method=http_method,
            url=self.full_path,
            content=encoded_payload,
            timeout=timeout or DEFAULT_TIMEOUT,
            headers=self.full_headers,
        )
        raw_response.raise_for_status()
        # Log the response before returning it
        log_response(raw_response)
        return raw_response

    def request(
        self,
        *,
        http_method: str = http_methods.POST,
        payload: Optional[Dict[str, Any]] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> Dict[str, Any]:
        raw_response = self.raw_request(
            http_method=http_method,
            payload=payload,
            raise_api_errors=raise_api_errors,
            timeout_info=timeout_info,
        )
        # try to process it into a JSON or throw a failure
        raw_response_json: Dict[str, Any]
        try:
            raw_response_json = cast(
                Dict[str, Any],
                raw_response.json(),
            )
        except ValueError:
            # json() parsing has failed (e.g., empty body)
            if payload is not None:
                command_desc = "/".join(sorted(payload.keys()))
            else:
                command_desc = "(none)"
            raise DataAPIFaultyResponseException(
                text=f"Unparseable response from API '{command_desc}' command.",
                raw_response={
                    "raw_response": raw_response.text,
                },
            )

        if raise_api_errors and "errors" in raw_response_json:
            logger.debug(raw_response_json["errors"])
            raise DataAPIResponseException.from_response(
                command=payload,
                raw_response=raw_response_json,
            )
        # further processing
        response_json = restore_from_api(raw_response_json)
        return response_json

    async def async_request(
        self,
        *,
        http_method: str = http_methods.POST,
        payload: Optional[Dict[str, Any]] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> Dict[str, Any]:
        raw_response = await self.async_raw_request(
            http_method=http_method,
            payload=payload,
            raise_api_errors=raise_api_errors,
            timeout_info=timeout_info,
        )
        # try to process it into a JSON or throw a failure
        raw_response_json: Dict[str, Any]
        try:
            raw_response_json = cast(
                Dict[str, Any],
                raw_response.json(),
            )
        except ValueError:
            # json() parsing has failed (e.g., empty body)
            if payload is not None:
                command_desc = "/".join(sorted(payload.keys()))
            else:
                command_desc = "(none)"
            raise DataAPIFaultyResponseException(
                text=f"Unparseable response from API '{command_desc}' command.",
                raw_response={
                    "raw_response": raw_response.text,
                },
            )

        if raise_api_errors and "errors" in raw_response_json:
            logger.debug(raw_response_json["errors"])
            raise DataAPIResponseException.from_response(
                command=payload,
                raw_response=raw_response_json,
            )
        # further processing
        response_json = restore_from_api(raw_response_json)
        return response_json
