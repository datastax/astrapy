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

from astrapy.defaults import DEFAULT_AUTH_HEADER, DEFAULT_KEYSPACE_NAME
from astrapy.utils import make_payload, make_request, http_methods, parse_endpoint_url

import logging

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
DEFAULT_BASE_PATH = "/api/json/v1"


class AstraDBCollection:
    def __init__(
        self,
        collection_name,
        astra_db=None,
        api_key=None,
        api_endpoint=None,
        namespace=None,
    ):
        if astra_db is None:
            if api_key is None or api_endpoint is None:
                raise AssertionError("Must provide api_key and api_endpoint")

            astra_db = AstraDB(
                api_key=api_key, api_endpoint=api_endpoint, namespace=namespace
            )

        self.astra_db = astra_db
        self.collection_name = collection_name
        self.base_path = f"{self.astra_db.base_path}/{collection_name}"

    def _request(self, *args, **kwargs):
        return make_request(
            *args,
            **kwargs,
            base_url=self.astra_db.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            api_key=self.astra_db.api_key,
        )

    def _get(self, path=None, options=None):
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self._request(
            method=http_methods.GET, path=full_path, url_params=options
        ).json()
        if isinstance(response, dict):
            return response
        return None

    def _post(self, path=None, document=None):
        return self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=document
        ).json()

    def get(self, path=None):
        return self._get(path=path)

    def find(self, filter=None, projection=None, sort=None, options=None):
        json_query = make_payload(
            top_level="find",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        ).json()

        return response

    def pop(self, filter, update, options):
        json_query = make_payload(
            top_level="findOneAndUpdate", filter=filter, update=update, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        ).json()

        return response

    def push(self, filter, update, options):
        json_query = make_payload(
            top_level="findOneAndUpdate", filter=filter, update=update, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        ).json()

        return response

    def find_one_and_replace(
        self, sort=None, filter=None, replacement=None, options=None
    ):
        json_query = make_payload(
            top_level="findOneAndReplace",
            filter=filter,
            replacement=replacement,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        ).json()

        return response

    def find_one_and_update(self, sort=None, update=None, filter=None, options=None):
        json_query = make_payload(
            top_level="findOneAndUpdate",
            filter=filter,
            update=update,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        ).json()

        return response

    def find_one(self, filter={}, projection={}, sort={}, options={}):
        json_query = make_payload(
            top_level="findOne",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        ).json()

        return response

    def insert_one(self, document):
        json_query = make_payload(top_level="insertOne", document=document)

        response = self._request(
            method=http_methods.POST, path=self.base_path, json_data=json_query
        ).json()

        return response

    def insert_many(self, documents, options=None):
        json_query = make_payload(top_level="insertMany", documents=documents, options=options)

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        ).json()

        return response

    def update_one(self, filter, update):
        json_query = make_payload(top_level="updateOne", filter=filter, update=update)

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        ).json()

        return response

    def replace(self, path, document):
        return self._put(path=path, document=document)

    def delete(self, id):
        json_query = {
            "deleteOne": {
                "filter": {"_id": id},
            }
        }

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        ).json()

        return response

    def delete_subdocument(self, id, subdoc):
        json_query = {
            "findOneAndUpdate": {
                "filter": {"_id": id},
                "update": {"$unset": {subdoc: ""}},
            }
        }

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        ).json()

        return response


class AstraDB:
    def __init__(
        self,
        api_key=None,
        api_endpoint=None,
        namespace=None,
    ):
        if api_key is None or api_endpoint is None:
            raise AssertionError("Must provide api_key and api_endpoint")

        if namespace is None:
            logger.info(
                f"ASTRA_DB_KEYSPACE is not set. Defaulting to '{DEFAULT_KEYSPACE_NAME}'"
            )
            namespace = DEFAULT_KEYSPACE_NAME

        # Store the initial parameters
        self.api_key = api_key
        (
            self.database_id,
            self.database_region,
            self.database_domain,
        ) = parse_endpoint_url(api_endpoint)

        # Set the Base URL for the API calls
        self.base_url = (
            f"https://{self.database_id}-{self.database_region}.{self.database_domain}"
        )
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace}"

        # Set the namespace parameter
        self.namespace = namespace

    def _request(self, *args, **kwargs):
        result = make_request(
            *args,
            **kwargs,
            base_url=self.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            api_key=self.api_key,
        )

        return result

    def collection(self, collection_name):
        return AstraDBCollection(collection_name=collection_name, astra_db=self)

    def get_collections(self):
        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data={"findCollections": {}},
        ).json()

        return response

    def create_collection(self, size=None, options={}, function="", collection_name=""):
        if size and not options:
            options = {"vector": {"size": size}}
            if function:
                options["vector"]["function"] = function
        if options:
            jsondata = {"name": collection_name, "options": options}
        else:
            jsondata = {"name": collection_name}

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": jsondata},
        ).json()

        return response

    def delete_collection(self, collection_name=""):
        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": collection_name}},
        ).json()

        return response
