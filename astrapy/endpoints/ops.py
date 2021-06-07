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

PATH_PREFIX = "/v2"


class AstraOps():

    def __init__(self, client=None):
        self.client = client

    def get_databases(self):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/databases")

    def create_database(self, database_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/databases",
                                   json_data=database_definition)

    def get_database(self, database=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/databases/{database}")

    def create_keyspace(self, database="", keyspace=""):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/databases/{database}/keyspaces/{keyspace}")

    def terminate_database(self, database=""):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/databases/{database}/terminate")

    def park_database(self, database=""):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/databases/{database}/park")

    def unpark_database(self, database=""):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/databases/{database}/unpark")

    def resize_database(self, database="", options=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/databases/{database}/resize",
                                   json_data=options)

    def reset_database_password(self, database="", options=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/databases/{database}/resetPassword",
                                   json_data=options)

    def get_available_regions(self):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/availableRegions")

    def get_roles(self):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/organizations/roles")

    def create_role(self, role_definition=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/organizations/roles",
                                   json_data=role_definition)

    def get_role(self, role=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/organizations/roles/{role}")

    def update_role(self, role="", role_definition=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{PATH_PREFIX}/organizations/roles/{role}",
                                   json_data=role_definition)

    def delete_role(self, role=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{PATH_PREFIX}/organizations/roles/{role}")

    def invite_user(self, user_definition=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{PATH_PREFIX}/organizations/users",
                                   json_data=user_definition)

    def get_users(self):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/organizations/users")

    def get_user(self, user=""):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/organizations/users/{user}")

    def remove_user(self, user=""):
        return self.client.request(method=http_methods.DELETE,
                                   path=f"{PATH_PREFIX}/organizations/users/{user}")

    def update_user_roles(self, user="", roles=None):
        return self.client.request(method=http_methods.PUT,
                                   path=f"{PATH_PREFIX}/organizations/users/{user}/roles",
                                   json_data=roles)

    def get_clients(self):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/clientIdSecrets")

    def create_token(self, roles=None):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/clientIdSecrets",
                                   json_data=roles)

    def delete_token(self, token=""):
        return self.client.request(method=http_methods.POST,
                                   path=f"{PATH_PREFIX}/clientIdSecret/{token}")

    def get_organization(self):
        return self.client.request(method=http_methods.GET,
                                   path=f"{PATH_PREFIX}/currentOrg")
