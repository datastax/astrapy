import logging
import httpx
from typing import Any, Dict, Optional, TypeVar, cast

from astrapy.types import API_RESPONSE
from astrapy.utils import amake_request, make_request

T = TypeVar("T", bound="APIRequestHandler")
AT = TypeVar("AT", bound="AsyncAPIRequestHandler")


logger = logging.getLogger(__name__)


class APIRequestError(ValueError):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(response.text)

        self.response = response

    def __repr__(self) -> str:
        return f"{self.response}"


class APIRequestHandler:
    def __init__(
        self: T,
        client: httpx.Client,
        base_url: str,
        auth_header: str,
        token: str,
        method: str,
        json_data: Optional[Dict[str, Any]],
        url_params: Optional[Dict[str, Any]],
        path: Optional[str] = None,
        skip_error_check: bool = False,
    ) -> None:
        self.client = client
        self.base_url = base_url
        self.auth_header = auth_header
        self.token = token
        self.method = method
        self.path = path
        self.json_data = json_data
        self.url_params = url_params
        self.skip_error_check = skip_error_check

    def raw_request(self: T) -> httpx.Response:
        return make_request(
            client=self.client,
            base_url=self.base_url,
            auth_header=self.auth_header,
            token=self.token,
            method=self.method,
            path=self.path,
            json_data=self.json_data,
            url_params=self.url_params,
        )

    def request(self: T) -> API_RESPONSE:
        # Make the raw request to the API
        self.response = self.raw_request()

        # If the response was not successful (non-success error code) raise an error directly
        self.response.raise_for_status()

        # Otherwise, process the successful response
        return self._process_response()

    def _process_response(self: T) -> API_RESPONSE:
        # In case of other successful responses, parse the JSON body.
        try:
            # Cast the response to the expected type.
            response_body: API_RESPONSE = cast(API_RESPONSE, self.response.json())

            # If the API produced an error, warn and return the API request error class
            if "errors" in response_body and not self.skip_error_check:
                logger.debug(response_body["errors"])

                raise APIRequestError(self.response)

            # Otherwise, set the response body
            return response_body
        except ValueError:
            # Handle cases where json() parsing fails (e.g., empty body)
            raise APIRequestError(self.response)


class AsyncAPIRequestHandler:
    def __init__(
        self: AT,
        client: httpx.AsyncClient,
        base_url: str,
        auth_header: str,
        token: str,
        method: str,
        json_data: Optional[Dict[str, Any]],
        url_params: Optional[Dict[str, Any]],
        path: Optional[str] = None,
        skip_error_check: bool = False,
    ) -> None:
        self.client = client
        self.base_url = base_url
        self.auth_header = auth_header
        self.token = token
        self.method = method
        self.path = path
        self.json_data = json_data
        self.url_params = url_params
        self.skip_error_check = skip_error_check

    async def raw_request(self: AT) -> httpx.Response:
        return await amake_request(
            client=self.client,
            base_url=self.base_url,
            auth_header=self.auth_header,
            token=self.token,
            method=self.method,
            path=self.path,
            json_data=self.json_data,
            url_params=self.url_params,
        )

    async def request(self: AT) -> API_RESPONSE:
        # Make the raw request to the API
        self.response = await self.raw_request()

        # If the response was not successful (non-success error code) raise an error directly
        self.response.raise_for_status()

        # Otherwise, process the successful response
        return await self._process_response()

    async def _process_response(self: AT) -> API_RESPONSE:
        # In case of other successful responses, parse the JSON body.
        try:
            # Cast the response to the expected type.
            response_body: API_RESPONSE = cast(API_RESPONSE, self.response.json())

            # If the API produced an error, warn and return the API request error class
            if "errors" in response_body and not self.skip_error_check:
                logger.debug(response_body["errors"])

                raise APIRequestError(self.response)

            # Otherwise, set the response body
            return response_body
        except ValueError:
            # Handle cases where json() parsing fails (e.g., empty body)
            raise APIRequestError(self.response)
