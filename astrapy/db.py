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

from astrapy.defaults import DEFAULT_AUTH_HEADER
from astrapy.ops import AstraDBOps
from astrapy.utils import make_request, http_methods

import logging

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/json/v1"


class AstraDBCollection:
    def __init__(
            self,
            collection,
            astra_db=None,
            db_id=None,
            token=None,
            db_region=None
        ):
        if astra_db is None:
            if db_id is None or token is None:
                raise AssertionError("Must provide db_id and token")

            astra_db = AstraDB(
                db_id=db_id, token=token, db_region=db_region
            )

        self.astra_db = astra_db
        self.collection = collection
        self.base_path = f"{self.astra_db.base_path}/{collection}"


    def _request(self, *args, **kwargs):
        result = make_request(
            *args,
            **kwargs,
            base_url=self.astra_db.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.astra_db.token,
        )

        return result

    def _get(self, path=None, options=None):
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self._request(
            method=http_methods.GET, path=full_path, url_params=options
        )
        if isinstance(response, dict):
            return response
        return None

    def _post(self, path=None, document=None):
        return self._request(
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
        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )
        return response

    def pop(self, filter, update, options):
        json_query = {
            "findOneAndUpdate": {"filter": filter, "update": update, "options": options}
        }
        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )
        return response

    def push(self, filter, update, options):
        json_query = {
            "findOneAndUpdate": {"filter": filter, "update": update, "options": options}
        }
        response = self._request(
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
        response = self._request(
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

        response = self._request(
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
        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )
        return response

    def insert_one(self, document):
        json_query = {"insertOne": {"document": document}}
        response = self._request(
            method=http_methods.POST, path=self.base_path, json_data=json_query
        )
        return response

    def update_one(self, filter, update):
        json_query = {"updateOne": {"filter": filter, "update": update}}
        return self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

    def replace(self, path, document):
        return self._put(path=path, document=document)

    def delete(self, id):
        return self._request(
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
        return self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

    def insert_many(self, documents):
        return self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"insertMany": {"documents": documents}},
        )


class AstraDB:
    def __init__(
        self,
        db_id=None,
        token=None,
        db_region=None,
        namespace="default_keyspace",
    ):
        if db_id is None or token is None:
            raise AssertionError("Must provide db_id and token")
        
        # Store the initial parameters
        self.db_id = db_id
        self.token = token
        
        # Handle the region parameter
        if not db_region:
            db_region = AstraDBOps(token=token).get_database(db_id)["info"]["region"]
        self.db_region = db_region

        # Set the Base URL for the API calls
        self.base_url = f"https://{db_id}-{db_region}.apps.astra.datastax.com"
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace}"

        # Set the namespace parameter
        self.namespace = namespace


    def _request(self, *args, **kwargs):
        result = make_request(
            *args,
            **kwargs,
            base_url=self.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.token,
        )

        return result


    def collection(self, collection):
        return AstraDBCollection(
            collection=collection,
            astra_db=self
        )

    def get_collections(self):
        res = self._request(
            method=http_methods.POST,
            path=self.base_path,
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
        return self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": jsondata},
        )

    def delete_collection(self, name=""):
        return self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": name}},
        )
