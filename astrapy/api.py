import httpx

from typing import cast

from astrapy.types import API_RESPONSE
from astrapy.utils import make_request


class APIRequestError(Exception):
    def __init__(self, message):
        super().__init__(message)


class APIRequestHandler:
    def __init__(
        self,
        client,
        base_url,
        auth_header,
        token,
        method,
        path,
        json_data,
        url_params,
        skip_error_check=False,
    ):
        self.client = client
        self.base_url = base_url
        self.auth_header = auth_header
        self.token = token
        self.method = method
        self.path = path
        self.json_data = json_data
        self.url_params = url_params
        self.skip_error_check = skip_error_check

    def request(self):
        try:
            response = make_request(
                client=self.client,
                base_url=self.base_url,
                auth_header=self.auth_header,
                token=self.token,
                method=self.method,
                path=self.path,
                json_data=self.json_data,
                url_params=self.url_params,
            )
            return self._process_response(response)
        except httpx.RequestError as e:
            raise APIRequestError(f"An error occurred while making the request: {e}")

    def _process_response(self, response: httpx.Response):
        if (
            not 200 <= response.status_code < 300
        ):  # TODO: Is this the right range? Any abstractions?
            raise APIRequestError(
                f"Non-success status code received: {response.status_code}"
            )

        responsebody = cast(API_RESPONSE, response.json())

        if "errors" in responsebody and not self.skip_error_check:
            raise APIRequestError(f"API returned an error: {responsebody['errors']}")

        return responsebody
