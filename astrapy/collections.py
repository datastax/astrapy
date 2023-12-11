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

from typing import List, Optional, Dict, Any
from astrapy.rest import AstraClient, http_methods
from astrapy.rest import create_client as create_astra_client
import logging
import json

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/rest/v2/namespaces"


class AstraCollection:
    def __init__(
        self,
        astra_client: AstraClient,
        namespace_name: Optional[str] = None,
        collection_name: Optional[str] = None,
    ) -> None:
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.collection_name = collection_name
        self.base_path = (
            f"{DEFAULT_BASE_PATH}/{namespace_name}/collections/{collection_name}"
        )
        if astra_client.auth_base_url is not None:
            self.base_path = (
                f"/v2/namespaces/{namespace_name}/collections/{collection_name}"
            )

    def _get(self, path: str, options: Optional[Dict[str, Any]]) -> Any:
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self.astra_client.request(
            method=http_methods.GET, path=full_path, url_params=options
        )
        if isinstance(response, dict):
            return response["data"]

        return None

    def _put(self, path: str, document: dict[str, Any]) -> Any:
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self.astra_client.request(
            method=http_methods.PUT, path=full_path, json_data=document
        )

        if isinstance(response, dict):
            return response

        return None

    def upgrade(self) -> Any:
        return self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}/upgrade"
        )

    def update_schema(self, schema: Dict[str, Any]) -> Any:
        return self.astra_client.request(
            method=http_methods.PUT,
            path=f"{self.base_path}/json-schema",
            json_data=schema,
        )

    def get(self, path: str, options: Optional[Dict[str, Any]]) -> Any:
        return self._get(path=path, options=options)

    def find(
        self,
        query: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        options = {} if options is None else options
        request_params = {"where": json.dumps(query), "page-size": DEFAULT_PAGE_SIZE}
        request_params.update(options)
        response = self.astra_client.request(
            method=http_methods.GET, path=self.base_path, url_params=request_params
        )
        if isinstance(response, dict):
            return response
        return None

    def find_one(
        self,
        query: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        options = {} if options is None else options
        request_params = {"where": json.dumps(query), "page-size": 1}
        request_params.update(options)
        response = self._get(path="", options=request_params)
        if response is not None:
            keys = list(response.keys())
            if len(keys) == 0:
                return None
            return response[keys[0]]
        return None

    def create(self, path: str, document: Dict[str, Any]) -> Any:
        return self._put(path=path, document=document)

    def replace(self, path: str, document: Any) -> Any:
        return self._put(path=path, document=document)

    def batch(self, documents: List[Dict[str, Any]], id_path: str = "") -> Any:
        if id_path == "":
            id_path = "documentId"
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/batch",
            json_data=documents,
            url_params={"id-path": id_path},
        )

    def push(self, path: Optional[str] = None, value: Any = None) -> Any:
        json_data: Dict[str, Any] = {"operation": "$push", "value": value}
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/{path}/function",
            json_data=json_data,
        )
        return res.get("data")

    def pop(self, path: Optional[str] = None) -> Any:
        json_data: Dict[str, Any] = {"operation": "$pop"}
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/{path}/function",
            json_data=json_data,
        )
        return res.get("data")


class AstraNamespace:
    def __init__(
        self, astra_client: Any = None, namespace_name: Optional[str] = None
    ) -> None:
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}"

    def collection(self, collection_name: str) -> AstraCollection:
        return AstraCollection(
            astra_client=self.astra_client,
            namespace_name=self.namespace_name,
            collection_name=collection_name,
        )

    def get_collections(self) -> Any:
        res = self.astra_client.request(
            method=http_methods.GET, path=f"{self.base_path}/collections"
        )
        return res.get("data")

    def create_collection(self, name: str = "") -> Any:
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/collections",
            json_data={"name": name},
        )

    def delete_collection(self, name: str = "") -> Any:
        return self.astra_client.request(
            method=http_methods.DELETE, path=f"{self.base_path}/collections/{name}"
        )


class AstraDocumentClient:
    def __init__(self, astra_client: Any = None):
        self.astra_client = astra_client

    def namespace(self, namespace_name: str) -> AstraNamespace:
        return AstraNamespace(
            astra_client=self.astra_client, namespace_name=namespace_name
        )


def create_client(
    astra_database_id: str,
    astra_database_region: str,
    astra_application_token: str,
    base_url: Optional[str] = None,
    auth_base_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    debug: bool = False,
) -> AstraDocumentClient:
    astra_client = create_astra_client(
        astra_database_id=astra_database_id,
        astra_database_region=astra_database_region,
        astra_application_token=astra_application_token,
        base_url=base_url,
        auth_base_url=auth_base_url,
        username=username,
        password=password,
        debug=debug,
    )
    return AstraDocumentClient(astra_client=astra_client)
