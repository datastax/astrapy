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

import json
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, cast

import httpx

from astrapy.authentication import (
    EMBEDDING_HEADER_API_KEY,
    EMBEDDING_HEADER_AWS_ACCESS_ID,
    EMBEDDING_HEADER_AWS_SECRET_ID,
)
from astrapy.core.defaults import (
    DEFAULT_AUTH_HEADER,
    DEFAULT_DEV_OPS_AUTH_HEADER,
    DEFAULT_TIMEOUT,
)
from astrapy.core.utils import (
    TimeoutInfoWideType,
    http_methods,
    log_request,
    log_response,
    logger,
    normalize_for_api,
    restore_from_api,
    to_httpx_timeout,
    user_agent_astrapy,
    user_agent_rs,
    user_agent_string,
)
from astrapy.exceptions import (
    DataAPIFaultyResponseException,
    DataAPIResponseException,
    to_dataapi_timeout_exception,
)

DEFAULT_REDACTED_HEADER_NAMES = {
    DEFAULT_AUTH_HEADER,
    DEFAULT_DEV_OPS_AUTH_HEADER,
    EMBEDDING_HEADER_AWS_ACCESS_ID,
    EMBEDDING_HEADER_AWS_SECRET_ID,
    EMBEDDING_HEADER_API_KEY,
}


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
        headers: Dict[str, Union[str, None]] = {},
        callers: List[Tuple[Optional[str], Optional[str]]] = [],
        redacted_header_names: Iterable[str] = DEFAULT_REDACTED_HEADER_NAMES,
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
            **{k: v for k, v in self.headers.items() if v is not None},
            **self.caller_header,
            **{"Content-Type": "application/json"},
        }
        self._loggable_headers = {
            k: v if k not in self.redacted_header_names else "***"
            for k, v in self.full_headers.items()
        }
        self.full_path = ("/".join([self.api_endpoint, self.path])).rstrip("/")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, APICommander):
            return all(
                [
                    self.api_endpoint == other.api_endpoint,
                    self.path == other.path,
                    self.headers == other.headers,
                    self.callers == other.callers,
                    self.redacted_header_names == other.redacted_header_names,
                ]
            )
        else:
            return False
        raise NotImplementedError

    def _copy(
        self,
        api_endpoint: Optional[str] = None,
        path: Optional[str] = None,
        headers: Optional[Dict[str, Union[str, None]]] = None,
        callers: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
        redacted_header_names: Optional[List[str]] = None,
    ) -> APICommander:
        # some care in allowing e.g. {} to override (but not None):
        return APICommander(
            api_endpoint=(
                api_endpoint if api_endpoint is not None else self.api_endpoint
            ),
            path=path if path is not None else self.path,
            headers=headers if headers is not None else self.headers,
            callers=callers if callers is not None else self.callers,
            redacted_header_names=(
                redacted_header_names
                if redacted_header_names is not None
                else self.redacted_header_names
            ),
        )

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
