import requests
import logging

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
    token,
    method=http_methods.POST,
    path=None,
    json_data=None,
    url_params=None,
    return_type="json"
):
    r = requests.request(
        method=method,
        url=f"{base_url}{path}",
        params=url_params,
        json=json_data,
        timeout=DEFAULT_TIMEOUT,
        headers={auth_header: token},
    )

    try:
        if return_type == "json":
            return r.json()
        else:
            return r
    except Exception as e:
        logger.warning(e)

        return None
