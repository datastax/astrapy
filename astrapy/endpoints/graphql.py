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
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.requests import log as requests_logger

requests_logger.setLevel(logging.WARNING)


class AstraGraphQL():

    def __init__(self, client=None):
        self.client = client
        self.gql = gql
        self.schema_gql_client = self.create_gql_client(
            url=f"{client.base_url}/api/graphql-schema")
        self.keyspace_clients = {}

    def create_gql_client(self, url=""):
        headers = {'X-Cassandra-Token': self.client.astra_application_token}
        transport = RequestsHTTPTransport(url=url,
                                          verify=True,
                                          retries=1,
                                          headers=headers)
        return Client(transport=transport, fetch_schema_from_transport=True)

    def get_keyspace_client(self, keyspace=""):
        return self.create_gql_client(url=f"{self.client.base_url}/api/graphql/{keyspace}")

    def execute(self, query="", variables=None, keyspace=""):
        gql_client = self.schema_gql_client
        if(keyspace != ""):
            gql_client = self.keyspace_clients.get(keyspace)
            if(gql_client is None):
                self.keyspace_clients[keyspace] = self.get_keyspace_client(
                    keyspace)
                gql_client = self.keyspace_clients[keyspace]
        return gql_client.execute(gql(query), variable_values=variables)
