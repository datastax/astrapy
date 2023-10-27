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
from astrapy.defaults import DEFAULT_DEV_OPS_PATH_PREFIX, DEFAULT_DEV_OPS_URL

import logging

logger = logging.getLogger(__name__)


class AstraDBOps:
    def __init__(self, token, dev_ops_url=None):
        dev_ops_url = dev_ops_url or DEFAULT_DEV_OPS_URL

        self.token = "Bearer " + token
        self.base_url = f"https://{dev_ops_url}{DEFAULT_DEV_OPS_PATH_PREFIX}"

    def _ops_request(self, method, path, options=None, json_data=None):
        options = {} if options is None else options

        return make_request(
            base_url=self.base_url,
            method=method,
            auth_header="Authorization",
            token=self.token,
            json_data=json_data,
            url_params=options,
            path=path,
        )

    def get_databases(self, options=None):
        response = self._ops_request(
            method=http_methods.GET, path="/databases", options=options
        ).json()

        return response

    def create_database(self, database_definition=None):
        r = self._ops_request(
            method=http_methods.POST, path="/databases", json_data=database_definition
        )

        if r.status_code == 201:
            return {"id": r.headers["Location"]}

        return None

    def terminate_database(self, database=""):
        r = self._ops_request(
            method=http_methods.POST, path=f"/databases/{database}/terminate"
        )

        if r.status_code == 202:
            return database

        return None

    def get_database(self, database="", options=None):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}",
            options=options,
        ).json()

    def create_keyspace(self, database="", keyspace=""):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/keyspaces/{keyspace}",
        )

    def park_database(self, database=""):
        return self._ops_request(
            method=http_methods.POST, path=f"/databases/{database}/park"
        ).json()

    def unpark_database(self, database=""):
        return self._ops_request(
            method=http_methods.POST, path=f"/databases/{database}/unpark"
        ).json()

    def resize_database(self, database="", options=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/resize",
            json_data=options,
        ).json()

    def reset_database_password(self, database="", options=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/resetPassword",
            json_data=options,
        ).json()

    def get_secure_bundle(self, database=""):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/secureBundleURL",
        ).json()

    def get_datacenters(self, database=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}/datacenters",
        ).json()

    def create_datacenter(self, database="", options=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/datacenters",
            json_data=options,
        ).json()

    def terminate_datacenter(self, database="", datacenter=""):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/datacenters/{datacenter}/terminate",
        ).json()

    def get_access_list(self, database=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}/access-list",
        ).json()

    def replace_access_list(self, database="", access_list=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/databases/{database}/access-list",
            json_data=access_list,
        ).json()

    def update_access_list(self, database="", access_list=None):
        return self._ops_request(
            method=http_methods.PATCH,
            path=f"/databases/{database}/access-list",
            json_data=access_list,
        ).json()

    def add_access_list_address(self, database="", address=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/access-list",
            json_data=address,
        ).json()

    def delete_access_list(self, database=""):
        return self._ops_request(
            method=http_methods.DELETE,
            path=f"/databases/{database}/access-list",
        ).json()

    def get_private_link(self, database=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/private-link",
        ).json()

    def get_datacenter_private_link(self, database="", datacenter=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/private-link",
        ).json()

    def create_datacenter_private_link(
        self, database="", datacenter="", private_link=None
    ):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/private-link",
            json_data=private_link,
        ).json()

    def create_datacenter_endpoint(self, database="", datacenter="", endpoint=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoint",
            json_data=endpoint,
        ).json()

    def update_datacenter_endpoint(self, database="", datacenter="", endpoint=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint['id']}",
            json_data=endpoint,
        ).json()

    def get_datacenter_endpoint(self, database="", datacenter="", endpoint=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint}",
        ).json()

    def delete_datacenter_endpoint(self, database="", datacenter="", endpoint=""):
        return self._ops_request(
            method=http_methods.DELETE,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint}",
        ).json()

    def get_available_classic_regions(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/availableRegions"
        ).json()

    def get_available_regions(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/regions/serverless"
        ).json()

    def get_roles(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/roles"
        ).json()

    def create_role(self, role_definition=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/organizations/roles",
            json_data=role_definition,
        ).json()

    def get_role(self, role=""):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/roles/{role}"
        ).json()

    def update_role(self, role="", role_definition=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/roles/{role}",
            json_data=role_definition,
        ).json()

    def delete_role(self, role=""):
        return self._ops_request(
            method=http_methods.DELETE, path=f"/organizations/roles/{role}"
        ).json()

    def invite_user(self, user_definition=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/users",
            json_data=user_definition,
        ).json()

    def get_users(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/users"
        ).json()

    def get_user(self, user=""):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/users/{user}"
        ).json()

    def remove_user(self, user=""):
        return self._ops_request(
            method=http_methods.DELETE, path=f"/organizations/users/{user}"
        ).json()

    def update_user_roles(self, user="", roles=None):
        return self._ops_request(
            method=http_methods.PUT,
            path=f"/organizations/users/{user}/roles",
            json_data=roles,
        ).json()

    def get_clients(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/clientIdSecrets"
        ).json()

    def create_token(self, roles=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/clientIdSecrets",
            json_data=roles,
        ).json()

    def delete_token(self, token=""):
        return self._ops_request(
            method=http_methods.DELETE, path=f"/clientIdSecret/{token}"
        ).json()

    def get_organization(self):
        return self._ops_request(method=http_methods.GET, path=f"/currentOrg").json()

    def get_access_lists(self):
        return self._ops_request(method=http_methods.GET, path=f"/access-lists").json()

    def get_access_list_template(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/access-list/template"
        ).json()

    def validate_access_list(self):
        return self._ops_request(
            method=http_methods.POST, path=f"/access-list/validate"
        ).json()

    def get_private_links(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/organizations/private-link"
        ).json()

    def get_streaming_providers(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/streaming/providers"
        ).json()

    def get_streaming_tenants(self):
        return self._ops_request(
            method=http_methods.GET, path=f"/streaming/tenants"
        ).json()

    def create_streaming_tenant(self, tenant=None):
        return self._ops_request(
            method=http_methods.POST,
            path=f"/streaming/tenants",
            json_data=tenant,
        ).json()

    def delete_streaming_tenant(self, tenant="", cluster=""):
        return self._ops_request(
            method=http_methods.DELETE,
            path=f"/streaming/tenants/{tenant}/clusters/{cluster}",
            json_data=tenant,
        ).json()

    def get_streaming_tenant(self, tenant=""):
        return self._ops_request(
            method=http_methods.GET,
            path=f"/streaming/tenants/{tenant}/limits",
        ).json()
