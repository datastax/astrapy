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

from astrapyjson.config.base import AstraClient
import logging
import json

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/json/v1"


class http_methods:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class AstraCollection:
    def __init__(self, astra_client=None, namespace_name=None, collection_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.collection_name = collection_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}/{collection_name}"
        self.namespace_path = f"{DEFAULT_BASE_PATH}/{namespace_name}"

    def get(self, path=None):
        return self._get(path=path)

    def _get(self, path=None, options=None):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/{path}",
            url_params=options,
        )

    def _post(self, path=None, document=None):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/{path}",
            json_data=document,
        )

    def find(self, filter=None, options=None, projection=None):
        filter = {} if filter is None else filter
        options = {} if options is None else options
        projection = {} if projection is None else projection
        json_request = {"find": {"filter": filter, "projection": projection}}
        response = self.astra_client.request(
            method=http_methods.POST, path=self.base_path, json_data=json_request
        )
        print("RESPONSE" + json.dumps(response))
        if isinstance(response, dict):
            return response
        return None

    def find_one(self, query=None, options=None):
        options = {} if options is None else options
        json_request = {"find": {"filter": query}}
        response = self.astra_client.request(
            method=http_methods.POST, path=self.base_path, json_data=json_request
        )
        if response is not None:
            keys = list(response.keys())
            if len(keys) == 0:
                return None
            return response[keys[0]]
        return None

    def create(self, document=None):
        query = {"insertOne": {"document": document}}
        return self.astra_client.request(
            method=http_methods.POST, path=self.base_path, json_data=query
        )

    def update_one(self, id, document):
        json_query = (
            {"updateOne": {"filter": {"_id": id}, "update": {"$set": document}}},
        )
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

    def find_one_and_replace(self, id, document):
        json_query = {
            "findOneAndReplace": {"filter": {"_id": id}, "replacement": document}
        }
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

    def delete_subdocument(self, id, subdoc):
        json_query = {
            "findOneAndUpdate": {
                "filter": {"_id": id},
                "update": {"$unset": {subdoc: ""}},
            }
        }
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

    def delete(self, id):
        json_data = {"deleteOne": {"filter": {"_id": id}}}
        return self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_data
        )

    def insert_many(self, documents=None):
        json_query = {"insertMany": {"documents": documents}}

        return self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

    def push(self, id, update, options):
        json_data = {
            "findOneAndUpdate": {
                "filter": {"_id": id},
                "update": update,
                "options": options,
            }
        }
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_data,
        )
        return res.get("data")

    def pop(self, id, update, options):
        json_data = {
            "findOneAndUpdate": {
                "filter": {"_id": id},
                "update": update,
                "options": options,
            }
        }
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_data,
        )
        return res.get("data")


class AstraNamespace:
    def __init__(self, astra_client=None, namespace_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}"
        self.namespace_path = f"{DEFAULT_BASE_PATH}/{namespace_name}"

    def collection(self, collection_name):
        return AstraCollection(
            astra_client=self.astra_client,
            namespace_name=self.namespace_name,
            collection_name=collection_name,
        )

    def get_collections(self):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.namespace_path}",
            json_data={"findCollections": {}},
        )

    def create_collection(self, name=""):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": {"name": name}},
        )

    def create_vector_collection(self, size, options=None, function="", name=""):
        if not options:
            if size:
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


class AstraJsonAPIClient(AstraClient):
    def __init__(self, astra_client=None):
        self.astra_client = astra_client

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
    if base_url is None:
        base_url = f"https://{astra_database_id}-{astra_database_region}.apps.astra.datastax.com"

    return AstraJsonAPIClient(astra_client=astra_client)
