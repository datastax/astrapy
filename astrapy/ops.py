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

from astrapy.utils import make_request, http_methods

DEFAULT_HOST = "https://api.astra.datastax.com"
PATH_PREFIX = "/v2"


class AstraDBOps:
    def __init__(self, token):
        self.token = "Bearer " + token
        self.base_url = f"{DEFAULT_HOST}{PATH_PREFIX}"

    def _ops_request(self, method, path, options=None, json_data=None,
                     return_type="json"):
        options = {} if options is None else options

        return make_request(
            base_url=self.base_url,
            method=method,
            auth_header="Authorization",
            token=self.token,
            json_data=json_data,
            url_params=options,
            path=path,
            return_type=return_type
        )

    def get_databases(self, options=None):
        return self._ops_request(
            method=http_methods.GET, path="/databases", options=options
        )

    def create_database(self, database_definition=None):
        r = self._ops_request(
            method=http_methods.POST,
            path="/databases",
            json_data=database_definition,
            return_type="response"
        )

        if r.status_code == 201:
            return {"id": r.headers["Location"]}

        return None

    def terminate_database(self, database=""):
        r = self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/terminate",
            return_type="response"
        )

        if r.status_code == 202:
            return database

        return None

    def get_database(self, database="", options=None):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}",
            options=options,
        )

    def create_keyspace(self, database="", keyspace=""):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/keyspaces/{keyspace}",
        )

    def park_database(self, database=""):
        return self._ops_request(
            method=http_methods.POST, path=f"/databases/{database}/park"
        )

    def unpark_database(self, database=""):
        return self._ops_request(
            method=http_methods.POST, path=f"/databases/{database}/unpark"
        )

    def resize_database(self, database="", options=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/resize",
            json_data=options,
        )

    def reset_database_password(self, database="", options=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/resetPassword",
            json_data=options,
        )

    def get_secure_bundle(self, database=""):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/secureBundleURL",
        )

    def get_datacenters(self, database=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}/datacenters",
        )

    def create_datacenter(self, database="", options=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/datacenters",
            json_data=options,
        )

    def terminate_datacenter(self, database="", datacenter=""):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/datacenters/{datacenter}/terminate",
        )

    def get_access_list(self, database=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}/access-list",
        )

    def replace_access_list(self, database="", access_list=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/databases/{database}/access-list",
            json_data=access_list,
        )

    def update_access_list(self, database="", access_list=None):
        return self._ops_request(
            method=http_methods.PATCH,
            path=f"/databases/{database}/access-list",
            json_data=access_list,
        )

    def add_access_list_address(self, database="", address=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/access-list",
            json_data=address,
        )

    def delete_access_list(self, database=""):
        return self._ops_request(
            method=http_methods.DELETE,
            path=f"/databases/{database}/access-list",
        )

    def get_private_link(self, database=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/private-link",
        )

    def get_datacenter_private_link(self, database="", datacenter=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/private-link",
        )

    def create_datacenter_private_link(
        self, database="", datacenter="", private_link=None
    ):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/private-link",
            json_data=private_link,
        )

    def create_datacenter_endpoint(self, database="", datacenter="", endpoint=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoint",
            json_data=endpoint,
        )

    def update_datacenter_endpoint(self, database="", datacenter="", endpoint=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint['id']}",
            json_data=endpoint,
        )

    def get_datacenter_endpoint(self, database="", datacenter="", endpoint=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint}",
        )

    def delete_datacenter_endpoint(self, database="", datacenter="", endpoint=""):
        return self._ops_request(
            method=http_methods.DELETE,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint}",
        )

    def get_available_classic_regions(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/availableRegions"
        )

    def get_available_regions(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/regions/serverless"
        )

    def get_roles(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/roles"
        )

    def create_role(self, role_definition=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/organizations/roles",
            json_data=role_definition,
        )

    def get_role(self, role=""):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/roles/{role}"
        )

    def update_role(self, role="", role_definition=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/roles/{role}",
            json_data=role_definition,
        )

    def delete_role(self, role=""):
        return self._ops_request(
            method=http_methods.DELETE, path=f"/organizations/roles/{role}"
        )

    def invite_user(self, user_definition=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/users",
            json_data=user_definition,
        )

    def get_users(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/users"
        )

    def get_user(self, user=""):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/users/{user}"
        )

    def remove_user(self, user=""):
        return self._ops_request(
            method=http_methods.DELETE, path=f"/organizations/users/{user}"
        )

    def update_user_roles(self, user="", roles=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/users/{user}/roles",
            json_data=roles,
        )

    def get_clients(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/clientIdSecrets"
        )

    def create_token(self, roles=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/clientIdSecrets",
            json_data=roles,
        )

    def delete_token(self, token=""):
        return self._ops_request(
            method=http_methods.DELETE, path=f"/clientIdSecret/{token}"
        )

    def get_organization(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/currentOrg"
        )

    def get_access_lists(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/access-lists"
        )

    def get_access_list_template(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/access-list/template"
        )

    def validate_access_list(self):
        return self._ops_request(
            method=http_methods.POST, path=f"/access-list/validate"
        )

    def get_private_links(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/private-link"
        )

    def get_streaming_providers(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/streaming/providers"
        )

    def get_streaming_tenants(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/streaming/tenants"
        )

    def create_streaming_tenant(self, tenant=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/streaming/tenants",
            json_data=tenant,
        )

    def delete_streaming_tenant(self, tenant="", cluster=""):
        return self._ops_request(
            method=http_methods.DELETE,
            path=f"/streaming/tenants/{tenant}/clusters/{cluster}",
            json_data=tenant,
        )

    def get_streaming_tenant(self, tenant=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/streaming/tenants/{tenant}/limits",
        )
