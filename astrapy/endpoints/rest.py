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

import json
from astrapyjson.config.rest import http_methods

DEFAULT_PAGE_SIZE = 20
PATH_PREFIX = "/api/rest/v2/keyspaces"


class AstraRest:
    def __init__(self, client=None):
        self.client = client
        self.path_prefix = PATH_PREFIX
        if client.auth_base_url is not None:
            self.path_prefix = "/v2/keyspaces"

    def search_table(self, keyspace="", table="", query=None, options=None):
        options = {} if options is None else options
        request_params = {"where": json.dumps(query), "page-size": DEFAULT_PAGE_SIZE}
        request_params.update(options)
        return self.client.request(
            method=http_methods.GET,
            path=f"{self.path_prefix}/{keyspace}/{table}",
            url_params=request_params,
        )

    def add_row(self, keyspace="", table="", row=None):
        return self.client.request(
            method=http_methods.POST,
            path=f"{self.path_prefix}/{keyspace}/{table}",
            json_data=row,
        )

    def get_rows(self, keyspace="", table="", key_path="", options=None):
        return self.client.request(
            method=http_methods.GET,
            path=f"{self.path_prefix}/{keyspace}/{table}/{key_path}",
            json_data=options,
        )

    def replace_rows(self, keyspace="", table="", key_path="", row=""):
        return self.client.request(
            method=http_methods.PUT,
            path=f"{self.path_prefix}/{keyspace}/{table}/{key_path}",
            json_data=row,
        )

    def update_rows(self, keyspace="", table="", key_path="", row=""):
        return self.client.request(
            method=http_methods.PATCH,
            path=f"{self.path_prefix}/{keyspace}/{table}/{key_path}",
            json_data=row,
        )

    def delete_rows(self, keyspace="", table="", key_path=""):
        return self.client.request(
            method=http_methods.DELETE,
            path=f"{self.path_prefix}/{keyspace}/{table}/{key_path}",
        )
