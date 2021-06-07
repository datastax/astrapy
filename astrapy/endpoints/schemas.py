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

    def get_keyspaces(self):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/keyspaces")

    def get_keyspace(self, keyspace=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}")

    def create_table(self, keyspace="", table_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables",
                                   json_data=table_definition)

    def get_tables(self, keyspace=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables")

    def get_table(self, keyspace="", table=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}")

    def update_table(self, keyspace="", table="", table_definition=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}",
                                   json_data=table_definition)

    def delete_table(self, keyspace="", table=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}")

    def create_column(self, keyspace="", table="", column_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}",
                                   json_data=column_definition)

    def get_columns(self, keyspace="", table=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}/columns")

    def get_column(self, keyspace="", table="", column=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}/columns/{column}")

    def update_column(self, keyspace="", table="", column="", column_definition=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}/columns/{column}",
                                   json_data=column_definition)

    def delete_column(self, keyspace="", table="", column=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}/columns/{column}")

    def get_indexes(self, keyspace="", table=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}/indexes")

    def create_index(self, keyspace="", table="", index_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}/indexes",
                                   json_data=index_definition)

    def delete_index(self, keyspace="", table="", index=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{PATH_PREFIX}/keyspaces/{keyspace}/tables/{table}/indexes/{index}")
