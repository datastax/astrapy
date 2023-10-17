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

from astrapy.base import AstraClient
from astrapy.ops import AstraOps
from astrapy.utils import http_methods

import logging

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/json/v1"


class AstraVectorCollection:
    def __init__(self, astra_client=None, namespace=None, collection=None):
        self.astra_client = astra_client
        self.namespace = namespace
        self.collection = collection
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace}/{collection}"

    def _get(self, path=None, options=None):
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self.astra_client.request(
            method=http_methods.GET, path=full_path, url_params=options
        )
        if isinstance(response, dict):
            return response
        return None

    def _post(self, path=None, document=None):
        return self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=document
        )

    def upgrade(self):
        return self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}/upgrade"
        )

    def get(self, path=None):
        return self._get(path=path)

    def find(self, filter=None, projection=None, sort=None, options=None):
        json_query = {
            "find": {
                "filter": filter,
                "projection": projection,
                "options": options,
                "sort": sort,
            }
        }

        response = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )
        return response

    def pop(self, filter, update, options):
        json_query = {
            "findOneAndUpdate": {"filter": filter, "update": update, "options": options}
        }
        response = self.astra_client.request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )
        return response

    def push(self, filter, update, options):
        json_query = {
            "findOneAndUpdate": {"filter": filter, "update": update, "options": options}
        }
        response = self.astra_client.request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )
        return response

    def find_one_and_replace(
        self, sort=None, filter=None, replacement=None, options=None
    ):
        json_query = {
            "findOneAndReplace": {
                "filter": filter,
                "replacement": replacement,
                "options": options,
                "sort": sort,
            }
        }
        response = self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )
        return response

    def find_one(self, filter={}, projection={}, sort=None, options=None):
        json_query = {
            "findOne": {
                "filter": filter,
                "projection": projection,
                "options": options,
                "sort": sort,
            }
        }
        response = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )
        if response is not None:
            keys = list(response.keys())
            if len(keys) == 0:
                return None
            return response[keys[0]]
        return None

    def create(self, path=None, document=None):
        json_query = {"insertOne": {"document": document}}
        return self.astra_client.request(
            method=http_methods.POST, path=self.base_path, json_data=json_query
        )

    def find_one_and_update(self, sort=None, update=None, filter=None, options=None):
        json_query = {
            "findOneAndUpdate": {
                "filter": filter,
                "update": update,
                "options": options,
                "sort": sort,
            }
        }

        response = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )
        return response

    def replace(self, path, document):
        return self._put(path=path, document=document)

    def delete(self, id):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteOne": {"filter": {"_id": id}}},
        )

    def delete_subdocument(self, id, subdoc):
        json_query = {
            "findOneAndUpdate": {
                "filter": {"_id": id},
                "update": {"$unset": {subdoc: ""}},
            }
        }
        return self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

    def insert_many(self, documents=None):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"insertMany": {"documents": documents}},
        )


class AstraVectorClient:
    def __init__(
        self,
        astra_client=None,
        db_id=None,
        token=None,
        db_region=None,
        namespace="default_namespace",
    ):
        if astra_client is not None:
            self.astra_client = astra_client
            token = self.astra_client.token
        else:
            if db_id is None or token is None:
                raise AssertionError("Must provide db_id and token")

            self.astra_client = AstraClient(
                db_id=db_id, token=token, db_region=db_region
            )

        self.namespace = namespace
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace}"

    def collection(self, collection):
        return AstraVectorCollection(
            astra_client=self.astra_client,
            namespace=self.namespace,
            collection=collection,
        )

    def get_collections(self):
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"findCollections": {}},
        )

        return res

    def create_vector_collection(self, size, name="", options=None, function="cosine"):
        if options is None:
            options = {"vector": {"size": size, "function": function}}

        json_query = {"createCollection": {"name": name, "options": options}}

        response = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

        return response

    def delete_collection(self, name=""):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": name}},
        )
