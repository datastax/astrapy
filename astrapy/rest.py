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

import logging
import requests

logger = logging.getLogger(__name__)


REQUESTED_WITH = "AstraPy"
DEFAULT_AUTH_PATH = "/api/rest/v1/auth"
DEFAULT_TIMEOUT = 30000
DEFAULT_AUTH_HEADER = "X-Cassandra-Token"


class http_methods():
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class AstraClient():
    def __init__(self, astra_database_id=None,
                 astra_database_region=None,
                 astra_application_token=None,
                 base_url=None,
                 auth_header=None):
        self.astra_database_id = astra_database_id
        self.astra_database_region = astra_database_region
        self.astra_application_token = astra_application_token
        self.base_url = base_url
        self.auth_header = DEFAULT_AUTH_HEADER

    def request(self, method=http_methods.GET, path=None, json_data=None, url_params=None):
        r = requests.request(method=method, url=f"{self.base_url}{path}",
                             params=url_params, json=json_data, timeout=DEFAULT_TIMEOUT,
                             headers={self.auth_header: self.astra_application_token})
        try:
            return r.json()
        except:
            return None


def create_client(astra_database_id=None,
                  astra_database_region=None,
                  astra_application_token=None,
                  base_url=None,
                  debug=False):
    if base_url is None:
        base_url = f"https://{astra_database_id}-{astra_database_region}.apps.astra.datastax.com"
    return AstraClient(astra_database_id=astra_database_id,
                       astra_database_region=astra_database_region,
                       astra_application_token=astra_application_token,
                       base_url=base_url)
