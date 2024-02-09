from __future__ import annotations
from typing import Any, cast, Dict, Iterable, List, Optional, Union
import time
import datetime
import logging
import copy

import httpx

from astrapy import __version__
from astrapy.defaults import DEFAULT_AUTH_HEADER, DEFAULT_TIMEOUT
from astrapy.types import API_RESPONSE


class CustomLogger(logging.Logger):
    def trace(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self.isEnabledFor(5):
            self._log(5, msg, args, **kwargs)


# Add a new TRACE logging level
logging.addLevelName(5, "TRACE")

# Tell the logging system to use your custom logger
logging.setLoggerClass(CustomLogger)


logger = logging.getLogger(__name__)


class http_methods:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


package_name = __name__.split(".")[0]


def log_request(
    method: str,
    url: str,
    params: Optional[Dict[str, Any]],
    headers: Dict[str, str],
    json_data: Optional[Dict[str, Any]],
) -> None:
    """
    Log the details of an HTTP request for debugging purposes.

    Args:
        json_data (dict or None): The JSON payload sent with the request, if any.
    """
    logger.debug(f"Request url: {url}")
    logger.debug(f"Request method: {method}")
    logger.debug(f"Request params: {params}")

    # Redact the token from the request headers
    headers_log = copy.deepcopy(headers)
    if DEFAULT_AUTH_HEADER in headers_log:
        headers_log[DEFAULT_AUTH_HEADER] = "AstraCS:<...>"

    logger.debug(f"Request headers: {headers_log}")

    if json_data:
        logger.trace(f"Request payload: {json_data}")  # type: ignore


def log_response(r: httpx.Response) -> None:
    """
    Log the details of an HTTP response for debugging purposes.

    Args:
        r (requests.Response): The response object from the HTTP request.
    """
    logger.debug(f"Response status code: {r.status_code}")
    logger.debug(f"Response headers: {r.headers}")
    logger.debug(f"Response content: {r.text}")


def compose_user_agent(
    caller_name: Optional[str], caller_version: Optional[str]
) -> str:
    if caller_name:
        if caller_version:
            return f"{caller_name}/{caller_version} {package_name}/{__version__}"
        else:
            return f"{caller_name} {package_name}/{__version__}"
    else:
        return f"{package_name}/{__version__}"


def make_request(
    client: httpx.Client,
    base_url: str,
    auth_header: str,
    token: str,
    method: str,
    json_data: Optional[Dict[str, Any]],
    url_params: Optional[Dict[str, Any]],
    path: Optional[str],
    caller_name: Optional[str],
    caller_version: Optional[str],
) -> httpx.Response:
    """
    Make an HTTP request to a specified URL.

    Args:
        client (httpx): The httpx client for the request.
        base_url (str): The base URL for the request.
        auth_header (str): The authentication header key.
        token (str): The token used for authentication.
        method (str, optional): The HTTP method to use for the request. Default is POST.
        path (str, optional): The specific path to append to the base URL.
        json_data (dict, optional): JSON payload to be sent with the request.
        url_params (dict, optional): URL parameters to be sent with the request.

    Returns:
        requests.Response: The response from the HTTP request.
    """
    # Build the request headers from the token and user agent
    request_headers = {
        auth_header: token,
        "User-Agent": compose_user_agent(caller_name, caller_version),
    }

    # Log the parameters of the request accordingly
    log_request(method, f"{base_url}{path}", url_params, request_headers, json_data)

    # Make the request
    r = client.request(
        method=method,
        url=f"{base_url}{path}",
        params=url_params,
        json=json_data,
        timeout=DEFAULT_TIMEOUT,
        headers=request_headers,
    )

    # Log the response before returning it
    log_response(r)

    return r


async def amake_request(
    client: httpx.AsyncClient,
    base_url: str,
    auth_header: str,
    token: str,
    method: str,
    path: Optional[str],
    json_data: Optional[Dict[str, Any]],
    url_params: Optional[Dict[str, Any]],
    caller_name: Optional[str],
    caller_version: Optional[str],
) -> httpx.Response:
    """
    Make an HTTP request to a specified URL.

    Args:
        client (httpx): The httpx client for the request.
        base_url (str): The base URL for the request.
        auth_header (str): The authentication header key.
        token (str): The token used for authentication.
        method (str, optional): The HTTP method to use for the request. Default is POST.
        path (str, optional): The specific path to append to the base URL.
        json_data (dict, optional): JSON payload to be sent with the request.
        url_params (dict, optional): URL parameters to be sent with the request.

    Returns:
        requests.Response: The response from the HTTP request.
    """
    # Build the request headers from the token and user agent
    request_headers = {
        auth_header: token,
        "User-Agent": compose_user_agent(caller_name, caller_version),
    }

    # Log the parameters of the request accordingly
    log_request(method, f"{base_url}{path}", url_params, request_headers, json_data)

    # Make the request
    r = await client.request(
        method=method,
        url=f"{base_url}{path}",
        params=url_params,
        json=json_data,
        timeout=DEFAULT_TIMEOUT,
        headers=request_headers,
    )

    # Log the response before returning it
    log_response(r)

    return r


def make_payload(top_level: str, **kwargs: Any) -> Dict[str, Any]:
    """
    Construct a JSON payload for an HTTP request with a specified top-level key.

    Args:
        top_level (str): The top-level key for the JSON payload.
        **kwargs: Arbitrary keyword arguments representing other keys and their values to be included in the payload.

    Returns:
        dict: The constructed JSON payload.
    """
    params = {}
    for key, value in kwargs.items():
        params[key] = value

    json_query: Dict[str, Any] = {top_level: {}}

    # Adding keys only if they're provided
    for key, value in params.items():
        if value is not None:
            json_query[top_level][key] = value

    return json_query


def convert_vector_to_floats(vector: Iterable[Any]) -> List[float]:
    """
    Convert a vector of strings to a vector of floats.

    Args:
        vector (list): A vector of objects.

    Returns:
        list: A vector of floats.
    """
    return [float(value) for value in vector]


def is_list_of_floats(vector: Iterable[Any]) -> bool:
    """
    Safely determine if it's a list of floats.
    Assumption: if list, and first item is float, then all items are.
    """
    return isinstance(vector, list) and (
        len(vector) == 0 or isinstance(vector[0], float) or isinstance(vector[0], int)
    )


def convert_to_ejson_date_object(
    date_value: Union[datetime.date, datetime.datetime]
) -> Dict[str, int]:
    return {"$date": int(time.mktime(date_value.timetuple()) * 1000)}


def convert_ejson_date_object_to_datetime(
    date_object: Dict[str, int]
) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(date_object["$date"] / 1000.0)


def _normalize_payload_value(path: List[str], value: Any) -> Any:
    """
    The path helps determining special treatments
    """
    _l2 = ".".join(path[-2:])
    _l1 = ".".join(path[-1:])
    if _l1 == "$vector" and _l2 != "projection.$vector":
        if not is_list_of_floats(value):
            return convert_vector_to_floats(value)
        else:
            return value
    else:
        if isinstance(value, dict):
            return {
                k: _normalize_payload_value(path + [k], v) for k, v in value.items()
            }
        elif isinstance(value, list):
            return [
                _normalize_payload_value(path + [""], list_item) for list_item in value
            ]
        else:
            if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                return convert_to_ejson_date_object(value)
            else:
                return value


def normalize_for_api(
    payload: Union[Dict[str, Any], None]
) -> Union[Dict[str, Any], None]:
    """
    Normalize a payload for API calls.
    This includes e.g. ensuring values for "$vector" key
    are made into plain lists of floats.

    Args:
        payload (Dict[str, Any]): A dict expressing a payload for an API call

    Returns:
        Dict[str, Any]: a "normalized" payload dict
    """

    if payload:
        return cast(Dict[str, Any], _normalize_payload_value([], payload))
    else:
        return payload


def _restore_response_value(path: List[str], value: Any) -> Any:
    """
    The path helps determining special treatments
    """
    if isinstance(value, dict):
        if len(value) == 1 and "$date" in value:
            # this is `{"$date": 123456}`, restore to datetime.datetime
            return convert_ejson_date_object_to_datetime(value)
        else:
            return {k: _restore_response_value(path + [k], v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_restore_response_value(path + [""], list_item) for list_item in value]
    else:
        return value


def restore_from_api(response: API_RESPONSE) -> API_RESPONSE:
    """
    Process a dictionary just returned from the API.
    This is the place where e.g. `{"$date": 123}` is
    converted back into a datetime object.
    """
    return cast(API_RESPONSE, _restore_response_value([], response))
