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

from astrapy.base import AstraDbClient
from astrapy.utils import http_methods

import logging

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/json/v1"


class AstraDbCollection:
    def __init__(
            self,
            collection,
            astra_db=None,
            db_id=None,
            token=None,
            db_region=None
        ):
        if astra_db is not None:
            self.astra_db = astra_db
        else:
            if db_id is None or token is None:
                raise AssertionError("Must provide db_id and token")

            self.astra_db = AstraDb(
                db_id=db_id, token=token, db_region=db_region
            )
        self.astra_db_client = self.astra_db.astra_db_client
        self.namespace = self.astra_db.namespace
        self.collection = collection
        self.base_path = f"{self.astra_db.base_path}/{collection}"

    def _get(self, path=None, options=None):
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self.astra_db_client.request(
            method=http_methods.GET, path=full_path, url_params=options
        )
        if isinstance(response, dict):
            return response
        return None

    def _post(self, path=None, document=None):
        return self.astra_db_client.request(
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
        response = self.astra_db_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )
        return response

    def pop(self, filter, update, options):
        json_query = {
            "findOneAndUpdate": {"filter": filter, "update": update, "options": options}
        }
        response = self.astra_db_client.request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )
        return response

    def push(self, filter, update, options):
        json_query = {
            "findOneAndUpdate": {"filter": filter, "update": update, "options": options}
        }
        response = self.astra_db_client.request(
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
        response = self.astra_db_client.request(
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

        response = self.astra_db_client.request(
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
        response = self.astra_db_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )
        return response

    def insert_one(self, document):
        json_query = {"insertOne": {"document": document}}
        response = self.astra_db_client.request(
            method=http_methods.POST, path=self.base_path, json_data=json_query
        )
        return response

    def update_one(self, filter, update):
        json_query = {"updateOne": {"filter": filter, "update": update}}
        return self.astra_db_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

    def replace(self, path, document):
        return self._put(path=path, document=document)

    def delete(self, id):
        return self.astra_db_client.request(
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
        return self.astra_db_client.request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

    def insert_many(self, documents):
        return self.astra_db_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"insertMany": {"documents": documents}},
        )


class AstraDb:
    def __init__(
        self,
        astra_db_client=None,
        db_id=None,
        token=None,
        db_region=None,
        namespace="default_namespace",
    ):
        if astra_db_client is not None:
            self.astra_db_client = astra_db_client
            token = self.astra_db_client.token
        else:
            if db_id is None or token is None:
                raise AssertionError("Must provide db_id and token")

            self.astra_db_client = AstraDbClient(
                db_id=db_id, token=token, db_region=db_region
            )

        self.namespace = namespace
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace}"


    def collection(self, collection_name):
        return AstraDbCollection(
            astra_client=self.astra_db_client,
            namespace_name=self.namespace_name,
            collection_name=collection_name,
        )

    def get_collections(self):
        res = self.astra_db_client.request(
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
        return self.astra_db_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": jsondata},
        )

    def delete_collection(self, name=""):
        return self.astra_db_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": name}},
        )
