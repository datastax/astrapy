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
import re
from decimal import Decimal
from types import TracebackType
from typing import Any, Dict, Iterable, Sequence, cast

import httpx

from astrapy.constants import CallerType
from astrapy.exceptions import (
    DataAPIHttpException,
    DataAPIResponseException,
    DevOpsAPIHttpException,
    DevOpsAPIResponseException,
    UnexpectedDataAPIResponseException,
    UnexpectedDevOpsAPIResponseException,
    _TimeoutContext,
    to_dataapi_timeout_exception,
    to_devopsapi_timeout_exception,
)
from astrapy.settings.defaults import (
    CHECK_DECIMAL_ESCAPING_CONSISTENCY,
    DEFAULT_REDACTED_HEADER_NAMES,
    FIXED_SECRET_PLACEHOLDER,
)
from astrapy.utils.request_tools import (
    HttpMethod,
    log_httpx_request,
    log_httpx_response,
    to_httpx_timeout,
)
from astrapy.utils.user_agents import (
    compose_full_user_agent,
    detect_astrapy_user_agent,
)

user_agent_astrapy = detect_astrapy_user_agent()

logger = logging.getLogger(__name__)


# these are a mixture from disparate alphabet, to minimize the chance
# of a collision with user-provided actual content:
DECIMAL_MARKER_PREFIX_STR = "𐐏丂"
DECIMAL_MARKER_SUFFIX_STR = "∀🇦🇫"
DECIMAL_CLEANER_PATTERN = re.compile(
    f'"{DECIMAL_MARKER_PREFIX_STR}([0-9.]+){DECIMAL_MARKER_SUFFIX_STR}"'
)


class _MarkedDecimalDefuser(json.JSONEncoder):
    def default(self, obj: object) -> Any:
        if isinstance(obj, Decimal):
            return "(defused decimal)"
        return super().default(obj)


class _MarkedDecimalEncoder(json.JSONEncoder):
    def default(self, obj: object) -> Any:
        if isinstance(obj, Decimal):
            return f"{DECIMAL_MARKER_PREFIX_STR}{obj}{DECIMAL_MARKER_SUFFIX_STR}"
        return super().default(obj)

    @staticmethod
    def _check_mark_match(json_string: str) -> bool:
        return bool(DECIMAL_CLEANER_PATTERN.search(json_string))

    @staticmethod
    def _clean_encoded_string(json_string: str) -> str:
        return re.sub(DECIMAL_CLEANER_PATTERN, r"\1", json_string)


class APICommander:
    client = httpx.Client()

    def __init__(
        self,
        *,
        api_endpoint: str,
        path: str,
        headers: dict[str, str | None] = {},
        callers: Sequence[CallerType] = [],
        redacted_header_names: Iterable[str] | None = None,
        dev_ops_api: bool = False,
        handle_decimals_writes: bool = False,
        handle_decimals_reads: bool = False,
    ) -> None:
        self.async_client = httpx.AsyncClient()
        self.api_endpoint = api_endpoint.rstrip("/")
        self.path = path.lstrip("/")
        self.headers = headers
        self.callers = callers
        self.redacted_header_names = set(redacted_header_names or [])
        self.upper_full_redacted_header_names = {
            header_name.upper()
            for header_name in (
                self.redacted_header_names | DEFAULT_REDACTED_HEADER_NAMES
            )
        }
        self.dev_ops_api = dev_ops_api
        self.handle_decimals_writes = handle_decimals_writes
        self.handle_decimals_reads = handle_decimals_reads

        self._faulty_response_exc_class: (
            type[UnexpectedDevOpsAPIResponseException]
            | type[UnexpectedDataAPIResponseException]
        )
        self._response_exc_class: (
            type[DevOpsAPIResponseException] | type[DataAPIResponseException]
        )
        self._http_exc_class: type[DataAPIHttpException] | type[DevOpsAPIHttpException]
        if self.dev_ops_api:
            self._faulty_response_exc_class = UnexpectedDevOpsAPIResponseException
            self._response_exc_class = DevOpsAPIResponseException
            self._http_exc_class = DevOpsAPIHttpException
        else:
            self._faulty_response_exc_class = UnexpectedDataAPIResponseException
            self._response_exc_class = DataAPIResponseException
            self._http_exc_class = DataAPIHttpException
        self._api_description = "DevOps API" if self.dev_ops_api else "Data API"

        full_user_agent_string = compose_full_user_agent(
            list(self.callers) + [user_agent_astrapy]
        )
        self.caller_header: dict[str, str] = (
            {"User-Agent": full_user_agent_string} if full_user_agent_string else {}
        )
        self.full_headers: dict[str, str] = {
            k: v
            for k, v in {
                **{
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                **self.caller_header,
                **self.headers,
            }.items()
            if v is not None
        }
        self._loggable_headers = {
            k: v
            if k.upper() not in self.upper_full_redacted_header_names
            else FIXED_SECRET_PLACEHOLDER
            for k, v in self.full_headers.items()
        }
        self.full_path = ("/".join([self.api_endpoint, self.path])).rstrip("/")

    def __repr__(self) -> str:
        pieces = [
            pc
            for pc in (
                f"api_endpoint={self.api_endpoint}",
                f"path={self.path}",
                f"callers={self.callers}",
                f"dev_ops_api={self.dev_ops_api}",
            )
            if pc is not None
        ]
        inner_desc = ", ".join(pieces)
        return f"{self.__class__.__name__}({inner_desc})"

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
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.async_client.aclose()

    def _copy(
        self,
        api_endpoint: str | None = None,
        path: str | None = None,
        headers: dict[str, str | None] | None = None,
        callers: Sequence[CallerType] | None = None,
        redacted_header_names: list[str] | None = None,
        dev_ops_api: bool | None = None,
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

    def _compose_request_url(self, additional_path: str | None) -> str:
        if additional_path:
            return "/".join([self.full_path.rstrip("/"), additional_path.lstrip("/")])
        else:
            return self.full_path

    def _raw_response_to_json(
        self,
        raw_response: httpx.Response,
        raise_api_errors: bool,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        # try to process the httpx raw response into a JSON or throw a failure
        raw_response_json: dict[str, Any]
        try:
            if self.handle_decimals_reads:
                # for decimal-aware contents (aka 'tables'), all number-looking things
                # are made into Decimal.
                # (for collections, this will be it. for Tables, schema-aware
                # proper post-processing will refine types, e.g. back to int, ...)
                raw_response_json = self._decimal_aware_parse_json_response(
                    raw_response.text,
                )
            else:
                raw_response_json = self._decimal_unaware_parse_json_response(
                    raw_response.text,
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
            warning_messages: list[str] = (raw_response_json.get("status") or {}).get(
                "warnings"
            ) or []
            if warning_messages:
                for warning_message in warning_messages:
                    full_warning = f"The {self._api_description} returned a warning: {warning_message}"
                    logger.warning(full_warning)

        return raw_response_json

    @staticmethod
    def _decimal_unaware_parse_json_response(response_text: str) -> dict[str, Any]:
        return cast(
            Dict[str, Any],
            json.loads(response_text),
        )

    @staticmethod
    def _decimal_aware_parse_json_response(response_text: str) -> dict[str, Any]:
        return cast(
            Dict[str, Any],
            json.loads(
                response_text,
                parse_float=Decimal,
                parse_int=Decimal,
            ),
        )

    @staticmethod
    def _decimal_unaware_encode_payload(payload: dict[str, Any] | None) -> str | None:
        # This is the JSON encoder in absence of the workaround to treat Decimals
        if payload is not None:
            return json.dumps(
                payload,
                allow_nan=False,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        else:
            return None

    @staticmethod
    def _decimal_aware_encode_payload(payload: dict[str, Any] | None) -> str | None:
        if payload is not None:
            if CHECK_DECIMAL_ESCAPING_CONSISTENCY:
                # check if escaping collision. This is expensive and 99.9999999% useless
                _naive_dump = json.dumps(
                    payload,
                    allow_nan=False,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    cls=_MarkedDecimalDefuser,
                )
                if _MarkedDecimalEncoder._check_mark_match(_naive_dump):
                    raise ValueError(
                        "The pattern to work around Decimals was detected in a "
                        "user-provided item. This payload cannot be JSON-encoded."
                    )
            dec_marked_dump = json.dumps(
                payload,
                allow_nan=False,
                separators=(",", ":"),
                ensure_ascii=False,
                cls=_MarkedDecimalEncoder,
            )
            return _MarkedDecimalEncoder._clean_encoded_string(dec_marked_dump)
        else:
            return None

    def raw_request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: dict[str, Any] | None = None,
        additional_path: str | None = None,
        request_params: dict[str, Any] = {},
        raise_api_errors: bool = True,
        timeout_context: _TimeoutContext | None = None,
    ) -> httpx.Response:
        request_url = self._compose_request_url(additional_path)
        _timeout_context = timeout_context or _TimeoutContext(request_ms=None)
        encoded_payload = (
            self._decimal_aware_encode_payload(payload)
            if self.handle_decimals_writes
            else self._decimal_unaware_encode_payload(payload)
        )
        log_httpx_request(
            http_method=http_method,
            full_url=request_url,
            request_params=request_params,
            redacted_request_headers=self._loggable_headers,
            encoded_payload=encoded_payload,
            timeout_context=_timeout_context,
        )
        httpx_timeout_s = to_httpx_timeout(_timeout_context)

        try:
            raw_response = self.client.request(
                method=http_method,
                url=request_url,
                content=encoded_payload.encode()
                if encoded_payload is not None
                else None,
                params=request_params,
                timeout=httpx_timeout_s,
                headers=self.full_headers,
            )
        except httpx.TimeoutException as timeout_exc:
            if self.dev_ops_api:
                raise to_devopsapi_timeout_exception(
                    timeout_exc, timeout_context=_timeout_context
                )
            else:
                raise to_dataapi_timeout_exception(
                    timeout_exc, timeout_context=_timeout_context
                )

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
        payload: dict[str, Any] | None = None,
        additional_path: str | None = None,
        request_params: dict[str, Any] = {},
        raise_api_errors: bool = True,
        timeout_context: _TimeoutContext | None = None,
    ) -> httpx.Response:
        request_url = self._compose_request_url(additional_path)
        _timeout_context = timeout_context or _TimeoutContext(request_ms=None)
        encoded_payload = (
            self._decimal_aware_encode_payload(payload)
            if self.handle_decimals_writes
            else self._decimal_unaware_encode_payload(payload)
        )
        log_httpx_request(
            http_method=http_method,
            full_url=request_url,
            request_params=request_params,
            redacted_request_headers=self._loggable_headers,
            encoded_payload=encoded_payload,
            timeout_context=_timeout_context,
        )
        httpx_timeout_s = to_httpx_timeout(_timeout_context)

        try:
            raw_response = await self.async_client.request(
                method=http_method,
                url=request_url,
                content=encoded_payload.encode()
                if encoded_payload is not None
                else None,
                params=request_params,
                timeout=httpx_timeout_s,
                headers=self.full_headers,
            )
        except httpx.TimeoutException as timeout_exc:
            if self.dev_ops_api:
                raise to_devopsapi_timeout_exception(
                    timeout_exc, timeout_context=_timeout_context
                )
            else:
                raise to_dataapi_timeout_exception(
                    timeout_exc, timeout_context=_timeout_context
                )

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
        payload: dict[str, Any] | None = None,
        additional_path: str | None = None,
        request_params: dict[str, Any] = {},
        raise_api_errors: bool = True,
        timeout_context: _TimeoutContext | None = None,
    ) -> dict[str, Any]:
        raw_response = self.raw_request(
            http_method=http_method,
            payload=payload,
            additional_path=additional_path,
            request_params=request_params,
            raise_api_errors=raise_api_errors,
            timeout_context=timeout_context,
        )
        return self._raw_response_to_json(
            raw_response, raise_api_errors=raise_api_errors, payload=payload
        )

    async def async_request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: dict[str, Any] | None = None,
        additional_path: str | None = None,
        request_params: dict[str, Any] = {},
        raise_api_errors: bool = True,
        timeout_context: _TimeoutContext | None = None,
    ) -> dict[str, Any]:
        raw_response = await self.async_raw_request(
            http_method=http_method,
            payload=payload,
            additional_path=additional_path,
            request_params=request_params,
            raise_api_errors=raise_api_errors,
            timeout_context=timeout_context,
        )
        return self._raw_response_to_json(
            raw_response, raise_api_errors=raise_api_errors, payload=payload
        )
