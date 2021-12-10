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

from astrapy.rest import http_methods

PATH_PREFIX = "/api/rest/v2/schemas"


class AstraSchemas():

    def __init__(self, client=None):
        self.client = client
        self.path_prefix = PATH_PREFIX
        if client.auth_base_url is not None:
            self.path_prefix = '/v2/schemas'

    def get_keyspaces(self):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces")
        return res.get("data", [])

    def get_keyspace(self, keyspace=""):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces/{keyspace}")
        return res.get("data")

    def create_table(self, keyspace="", table_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables",
                                   json_data=table_definition)

    def get_tables(self, keyspace=""):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces/{keyspace}/tables")
        return res.get("data")

    def get_table(self, keyspace="", table=""):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}")
        return res.get("data")

    def update_table(self, keyspace="", table_definition=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table_definition['name']}",
                                   json_data=table_definition)

    def delete_table(self, keyspace="", table=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}")

    def create_column(self, keyspace="", table="", column_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/columns",
                                   json_data=column_definition)

    def get_columns(self, keyspace="", table=""):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/columns")
        return res.get("data")

    def get_column(self, keyspace="", table="", column=""):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/columns/{column}")
        return res.get("data")

    def update_column(self, keyspace="", table="", column="", column_definition=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/columns/{column}",
                                   json_data=column_definition)

    def delete_column(self, keyspace="", table="", column=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/columns/{column}")

    def get_indexes(self, keyspace="", table=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/indexes")

    def create_index(self, keyspace="", table="", index_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/indexes",
                                   json_data=index_definition)

    def delete_index(self, keyspace="", table="", index=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/tables/{table}/indexes/{index}")

    def get_types(self, keyspace=""):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces/{keyspace}/types")
        return res.get("data")

    def get_type(self, keyspace="", udt=""):
        res = self.client.request(method=http_methods.GET,
                                  path=f"{self.path_prefix}/keyspaces/{keyspace}/types/{udt}")
        return res.get("data")

    def create_type(self, keyspace="", udt_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/types",
                                   json_data=udt_definition)

    def update_type(self, keyspace="", udt_definition=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/types",
                                   json_data=udt_definition)

    def delete_type(self, keyspace="", udt=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{self.path_prefix}/keyspaces/{keyspace}/types/{udt}")
