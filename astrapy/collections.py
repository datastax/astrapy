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

from astrapy.base import AstraClient, http_methods
import logging
import json

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/json/v1"


class AstraCollection:
    def __init__(self, astra_client=None, namespace_name=None, collection_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.collection_name = collection_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}/{collection_name}"

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

    def get(self, path=None):
        return self._get(path=path)

    def find(self, filter={}, projection={}, sort={}, options={}):
        json_query = {
            "find": {
                "filter": filter,
                "projection": projection,
                "options": options,
                "sort": sort,
            }
        }
        print(json_query)
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

    def find_one(self, filter={}, projection={}, sort={}, options={}):
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
        return response

    def insert_one(self, path=None, document=None):
        json_query = {"insertOne": {"document": document}}
        response = self.astra_client.request(
            method=http_methods.POST, path=self.base_path, json_data=json_query
        )
        return response

    def update_one(self, filter, update):
        json_query = {"updateOne": {"filter": filter, "update": update}}
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

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

    def insert_many(self, documents=None, id_path=""):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"insertMany": {"documents": documents}},
        )


class AstraNamespace:
    def __init__(self, astra_client=None, namespace_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}"

    def collection(self, collection_name):
        return AstraCollection(
            astra_client=self.astra_client,
            namespace_name=self.namespace_name,
            collection_name=collection_name,
        )

    def get_collections(self):
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"findCollections": {}},
        )
        return res

    def create_collection(self, size=None, options={}, function="", name=""):
        if size and not options:
            options = {"vector": {"size": size}}
            if function:
                options["vector"]["function"] = function
        if options:
            jsondata = {"name": name, "options": options}
        else:
            jsondata = {"name": name}
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": jsondata},
        )

    def delete_collection(self, name=""):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": name}},
        )


class AstraCollectionClient:
    def __init__(self, astra_client=None):
        self.astra_client = astra_client
        if self.astra_client == None:
            self.astra_client = AstraClient()

    def namespace(self, namespace_name):
        return AstraNamespace(
            astra_client=self.astra_client, namespace_name=namespace_name
        )


def create_client(
    astra_database_id=None,
    astra_database_region=None,
    astra_application_token=None,
    base_url=None,
    debug=False,
):
    astra_client = AstraClient(
        astra_database_id=astra_database_id,
        astra_database_region=astra_database_region,
        astra_application_token=astra_application_token,
        base_url=base_url,
        debug=debug,
    )
    return AstraCollectionClient(astra_client=astra_client)
