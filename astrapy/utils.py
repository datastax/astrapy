import requests
import logging
import re

from astrapy.defaults import DEFAULT_TIMEOUT

logger = logging.getLogger(__name__)


class http_methods:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


def make_request(
    base_url,
    auth_header,
    api_key,
    method=http_methods.POST,
    path=None,
    json_data=None,
    url_params=None,
):
    try:
        r = requests.request(
            method=method,
            url=f"{base_url}{path}",
            params=url_params,
            json=json_data,
            timeout=DEFAULT_TIMEOUT,
            headers={auth_header: api_key},
        )

        return r
    except Exception as e:
        logger.warning(e)

        return {"error": "An unknown error occurred", "details": str(e)}


def make_payload(top_level, **kwargs):
    params = {}
    for key, value in kwargs.items():
        params[key] = value

    json_query = {top_level: {}}

    # Adding keys only if they're provided
    for key, value in params.items():
        if value is not None:
            json_query[top_level][key] = value

    return json_query


def parse_endpoint_url(url):
    # Regular expression pattern to match the given URL format
    pattern = r"https://(?P<db_id>[a-fA-F0-9\-]{36})-(?P<db_region>[a-zA-Z0-9\-]+)\.(?P<db_hostname>[a-zA-Z0-9\-\.]+\.com)"

    match = re.match(pattern, url)
    if match:
        return (
            match.group("db_id"),
            match.group("db_region"),
            match.group("db_hostname"),
        )
    else:
        raise ValueError("Invalid URL format")
