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
import logging
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import httpx

from astrapy.defaults import (
    DEFAULT_REDACTED_HEADER_NAMES,
    DEFAULT_REQUEST_TIMEOUT_MS,
    HEADER_REDACT_PLACEHOLDER,
)
from astrapy.exceptions import (
    DataAPIFaultyResponseException,
    DataAPIHttpException,
    DataAPIResponseException,
    DevOpsAPIFaultyResponseException,
    DevOpsAPIHttpException,
    DevOpsAPIResponseException,
    to_dataapi_timeout_exception,
    to_devopsapi_timeout_exception,
)
from astrapy.request_tools import (
    HttpMethod,
    log_httpx_request,
    log_httpx_response,
    to_httpx_timeout,
)
from astrapy.transform_payload import normalize_for_api, restore_from_api
from astrapy.user_agents import (
    compose_full_user_agent,
    detect_astrapy_user_agent,
    detect_ragstack_user_agent,
)

if TYPE_CHECKING:
    from astrapy.request_tools import TimeoutInfoWideType


user_agent_astrapy = detect_astrapy_user_agent()
user_agent_ragstack = detect_ragstack_user_agent()

logger = logging.getLogger(__name__)


class APICommander:
    client = httpx.Client()

    def __init__(
        self,
        api_endpoint: str,
        path: str,
        headers: Dict[str, Union[str, None]] = {},
        callers: List[Tuple[Optional[str], Optional[str]]] = [],
        redacted_header_names: Iterable[str] = DEFAULT_REDACTED_HEADER_NAMES,
        dev_ops_api: bool = False,
    ) -> None:
        self.async_client = httpx.AsyncClient()
        self.api_endpoint = api_endpoint.rstrip("/")
        self.path = path.lstrip("/")
        self.headers = headers
        self.callers = callers
        self.redacted_header_names = set(redacted_header_names)
        self.dev_ops_api = dev_ops_api

        self._faulty_response_exc_class: Union[
            Type[DevOpsAPIFaultyResponseException], Type[DataAPIFaultyResponseException]
        ]
        self._response_exc_class: Union[
            Type[DevOpsAPIResponseException], Type[DataAPIResponseException]
        ]
        self._http_exc_class: Union[
            Type[DataAPIHttpException], Type[DevOpsAPIHttpException]
        ]
        if self.dev_ops_api:
            self._faulty_response_exc_class = DevOpsAPIFaultyResponseException
            self._response_exc_class = DevOpsAPIResponseException
            self._http_exc_class = DevOpsAPIHttpException
        else:
            self._faulty_response_exc_class = DataAPIFaultyResponseException
            self._response_exc_class = DataAPIResponseException
            self._http_exc_class = DataAPIHttpException
        self._api_description = "DevOps API" if self.dev_ops_api else "Data API"

        full_user_agent_string = compose_full_user_agent(
            [user_agent_ragstack] + self.callers + [user_agent_astrapy]
        )
        self.caller_header: Dict[str, str] = (
            {"User-Agent": full_user_agent_string} if full_user_agent_string else {}
        )
        self.full_headers: Dict[str, str] = {
            **{k: v for k, v in self.headers.items() if v is not None},
            **self.caller_header,
            **{"Content-Type": "application/json"},
        }
        self._loggable_headers = {
            k: v if k not in self.redacted_header_names else HEADER_REDACT_PLACEHOLDER
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
                    self.dev_ops_api == other.dev_ops_api,
                ]
            )
        else:
            return False

    async def __aenter__(self) -> APICommander:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        await self.async_client.aclose()

    def _copy(
        self,
        api_endpoint: Optional[str] = None,
        path: Optional[str] = None,
        headers: Optional[Dict[str, Union[str, None]]] = None,
        callers: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
        redacted_header_names: Optional[List[str]] = None,
        dev_ops_api: Optional[bool] = None,
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
            dev_ops_api=dev_ops_api if dev_ops_api is not None else self.dev_ops_api,
        )

    def _raw_response_to_json(
        self,
        raw_response: httpx.Response,
        raise_api_errors: bool,
        payload: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        # try to process the httpx raw response into a JSON or throw a failure
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
            raise self._faulty_response_exc_class(
                text=f"Unparseable response from API '{command_desc}' command.",
                raw_response={
                    "raw_response": raw_response.text,
                },
            )

        if raise_api_errors and "errors" in raw_response_json:
            logger.warning(
                f"APICommander about to raise from: {raw_response_json['errors']}"
            )
            raise self._response_exc_class.from_response(
                command=payload,
                raw_response=raw_response_json,
            )

        # no warnings check for DevOps API (there, 'status' may contain a string)
        if not self.dev_ops_api:
            warning_messages: List[str] = (raw_response_json.get("status") or {}).get(
                "warnings"
            ) or []
            if warning_messages:
                for warning_message in warning_messages:
                    full_warning = f"The {self._api_description} returned a warning: {warning_message}"
                    logger.warning(full_warning)

        # further processing
        response_json = restore_from_api(raw_response_json)
        return response_json

    def _compose_request_url(self, additional_path: Optional[str]) -> str:
        if additional_path:
            return "/".join([self.full_path.rstrip("/"), additional_path.lstrip("/")])
        else:
            return self.full_path

    def _encode_payload(
        self, normalized_payload: Optional[Dict[str, Any]]
    ) -> Optional[bytes]:
        if normalized_payload is not None:
            return json.dumps(
                normalized_payload,
                allow_nan=False,
                separators=(",", ":"),
            ).encode()
        else:
            return None

    def raw_request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: Optional[Dict[str, Any]] = None,
        additional_path: Optional[str] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> httpx.Response:
        timeout = to_httpx_timeout(timeout_info)
        normalized_payload = normalize_for_api(payload)
        request_url = self._compose_request_url(additional_path)
        log_httpx_request(
            http_method=http_method,
            full_url=request_url,
            request_params={},
            redacted_request_headers=self._loggable_headers,
            payload=normalized_payload,
        )
        encoded_payload = self._encode_payload(normalized_payload)

        try:
            raw_response = self.client.request(
                method=http_method,
                url=request_url,
                content=encoded_payload,
                timeout=timeout or DEFAULT_REQUEST_TIMEOUT_MS,
                headers=self.full_headers,
            )
        except httpx.TimeoutException as timeout_exc:
            if self.dev_ops_api:
                raise to_devopsapi_timeout_exception(timeout_exc)
            else:
                raise to_dataapi_timeout_exception(timeout_exc)

        try:
            raw_response.raise_for_status()
        except httpx.HTTPStatusError as http_exc:
            raise self._http_exc_class.from_httpx_error(http_exc)
        log_httpx_response(response=raw_response)
        return raw_response

    async def async_raw_request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: Optional[Dict[str, Any]] = None,
        additional_path: Optional[str] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> httpx.Response:
        timeout = to_httpx_timeout(timeout_info)
        normalized_payload = normalize_for_api(payload)
        request_url = self._compose_request_url(additional_path)
        log_httpx_request(
            http_method=http_method,
            full_url=request_url,
            request_params={},
            redacted_request_headers=self._loggable_headers,
            payload=normalized_payload,
        )
        encoded_payload = self._encode_payload(normalized_payload)

        try:
            raw_response = await self.async_client.request(
                method=http_method,
                url=request_url,
                content=encoded_payload,
                timeout=timeout or DEFAULT_REQUEST_TIMEOUT_MS,
                headers=self.full_headers,
            )
        except httpx.TimeoutException as timeout_exc:
            if self.dev_ops_api:
                raise to_devopsapi_timeout_exception(timeout_exc)
            else:
                raise to_dataapi_timeout_exception(timeout_exc)

        try:
            raw_response.raise_for_status()
        except httpx.HTTPStatusError as http_exc:
            raise self._http_exc_class.from_httpx_error(http_exc)
        log_httpx_response(response=raw_response)
        return raw_response

    def request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: Optional[Dict[str, Any]] = None,
        additional_path: Optional[str] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> Dict[str, Any]:
        raw_response = self.raw_request(
            http_method=http_method,
            payload=payload,
            additional_path=additional_path,
            raise_api_errors=raise_api_errors,
            timeout_info=timeout_info,
        )
        return self._raw_response_to_json(
            raw_response, raise_api_errors=raise_api_errors, payload=payload
        )

    async def async_request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: Optional[Dict[str, Any]] = None,
        additional_path: Optional[str] = None,
        raise_api_errors: bool = True,
        timeout_info: TimeoutInfoWideType = None,
    ) -> Dict[str, Any]:
        raw_response = await self.async_raw_request(
            http_method=http_method,
            payload=payload,
            additional_path=additional_path,
            raise_api_errors=raise_api_errors,
            timeout_info=timeout_info,
        )
        return self._raw_response_to_json(
            raw_response, raise_api_errors=raise_api_errors, payload=payload
        )
