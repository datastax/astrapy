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

from astrapy.rest import AstraClient, http_methods
import logging
import json

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/rest/v2/namespaces"


class AstraCollection():
    def __init__(self, astra_client=None, namespace_name=None, collection_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.collection_name = collection_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}/collections/{collection_name}"

    def _get(self, path=None, options=None):
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self.astra_client.request(method=http_methods.GET,
                                             path=full_path,
                                             url_params=options)
        if isinstance(response, dict):
            return response["data"]
        return None

    def _put(self, path=None, document=None):
        return self.astra_client.request(method=http_methods.PUT,
                                         path=f"{self.base_path}/{path}",
                                         json_data=document)

    def get(self, path=None):
        return self._get(path=path)

    def find(self, query=None, options=None):
        options = {} if options is None else options
        request_params = {"where": json.dumps(
            query), "page-size": DEFAULT_PAGE_SIZE}
        request_params.update(options)
        return self._get(path=None, options=request_params)

    def find_one(self, query=None, options=None):
        options = {} if options is None else options
        request_params = {"where": json.dumps(query), "page-size": 1}
        request_params.update(options)
        response = self._get(path=None, options=request_params)
        if response is not None:
            return response[list(response.keys())[0]]
        return None

    def create(self, path=None, document=None):
        if path is not None:
            return self._put(path=path, document=document)
        return self.astra_client.request(method=http_methods.POST,
                                         path=self.base_path,
                                         json_data=document)

    def update(self, path, document):
        return self.astra_client.request(method=http_methods.PATCH,
                                         path=f"{self.base_path}/{path}",
                                         json_data=document)

    def replace(self, path, document):
        return self._put(path=path, document=document)

    def delete(self, path):
        return self.astra_client.request(method=http_methods.DELETE,
                                         path=f"{self.base_path}/{path}")


class AstraNamespace():
    def __init__(self, astra_client=None, namespace_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name

    def collection(self, collection_name):
        return AstraCollection(astra_client=self.astra_client,
                               namespace_name=self.namespace_name,
                               collection_name=collection_name)


class AstraDocumentClient():
    def __init__(self, astra_client=None):
        self.astra_client = astra_client

    def namespace(self, namespace_name):
        return AstraNamespace(astra_client=self.astra_client, namespace_name=namespace_name)


def create_client(astra_database_id=None,
                  astra_database_region=None,
                  astra_application_token=None,
                  debug=False):
    astra_client = AstraClient(astra_database_id=astra_database_id,
                               astra_database_region=astra_database_region,
                               astra_application_token=astra_application_token)
    return AstraDocumentClient(astra_client=astra_client)
