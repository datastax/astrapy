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

from astrapy.defaults import (
    DEFAULT_AUTH_HEADER,
    DEFAULT_KEYSPACE_NAME,
    DEFAULT_BASE_PATH,
)
from astrapy.utils import make_payload, make_request, http_methods, parse_endpoint_url

import logging
import json

logger = logging.getLogger(__name__)


class AstraDBCollection:
    def __init__(
        self,
        collection_name,
        astra_db=None,
        token=None,
        api_endpoint=None,
        namespace=None,
    ):
        if astra_db is None:
            if token is None or api_endpoint is None:
                raise AssertionError("Must provide token and api_endpoint")

            astra_db = AstraDB(
                token=token, api_endpoint=api_endpoint, namespace=namespace
            )

        self.astra_db = astra_db
        self.collection_name = collection_name
        self.base_path = f"{self.astra_db.base_path}/{collection_name}"

    def _request(self, *args, skip_error_check=False, **kwargs):
        response = make_request(
            *args,
            **kwargs,
            base_url=self.astra_db.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.astra_db.token,
        )
        responsebody = response.json()

        if not skip_error_check and "errors" in responsebody:
            raise ValueError(json.dumps(responsebody["errors"]))
        else:
            return responsebody

    def _get(self, path=None, options=None):
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self._request(
            method=http_methods.GET, path=full_path, url_params=options
        )
        if isinstance(response, dict):
            return response
        return None

    def _post(self, path=None, document=None):
        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=document
        )
        return response

    def _pre_process_find(self, vector, fields=None):
        # Must pass a vector
        if not vector:
            return ValueError("Must pass a vector")

        # Build the new vector parameter
        sort = {"$vector": vector}

        # Build the new fields parameter
        projection = {f: 1 for f in fields} if fields else {}

        # TODO: Do we always return the similarity?
        projection["$similarity"] = 1

        return sort, projection

    def _post_process_find(
        self,
        raw_find_result,
        include_similarity=True,
        _key="documents",
    ):
        # If we only have a single result, treat it as a list
        if type(raw_find_result["data"][_key]) == dict:
            raw_find_result["data"][_key] = [raw_find_result["data"][_key]]

        # Process list of documents
        final_result = []
        for document in raw_find_result["data"][_key]:
            # Pop the returned similarity score
            if "$similarity" in document:
                similarity = document.pop("$similarity")
                if include_similarity:
                    document["$similarity"] = similarity

            final_result.append(document)

        return final_result

    def get(self, path=None):
        return self._get(path=path)

    def find(self, filter=None, projection=None, sort={}, options=None):
        json_query = make_payload(
            top_level="find",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._post(
            document=json_query,
        )

        return response

    def vector_find(
        self,
        vector,
        *,
        limit,
        filter=None,
        fields=None,
        include_similarity=True,
    ):
        # Must pass a limit
        if not limit:
            return ValueError("Must pass a limit")

        # Pre-process the included arguments
        sort, projection = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find(
            filter=filter,
            projection=projection,
            sort=sort,
            options={"limit": limit},
        )

        # Post-process the return
        find_result = self._post_process_find(
            raw_find_result,
            include_similarity=include_similarity,
        )

        return find_result

    @staticmethod
    def paginate(*, method, options, **kwargs):
        response0 = method(options=options, **kwargs)
        next_page_state = response0["data"]["nextPageState"]
        options0 = options
        for document in response0["data"]["documents"]:
            yield document
        while next_page_state is not None:
            options1 = {**options0, **{"pagingState": next_page_state}}
            response1 = method(options=options1, **kwargs)
            for document in response1["data"]["documents"]:
                yield document
            next_page_state = response1["data"]["nextPageState"]

    def paginated_find(self, filter=None, projection=None, sort=None, options=None):
        return self.paginate(
            method=self.find,
            filter=filter,
            projection=projection,
            sort=sort,
            options=options,
        )

    def pop(self, filter, update, options):
        json_query = make_payload(
            top_level="findOneAndUpdate", filter=filter, update=update, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )

        return response

    def push(self, filter, update, options):
        json_query = make_payload(
            top_level="findOneAndUpdate", filter=filter, update=update, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )

        return response

    def find_one_and_replace(
        self, sort={}, filter=None, replacement=None, options=None
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
        )

        return response

    def vector_find_one_and_replace(
        self,
        vector,
        *,
        replacement=None,
        filter=None,
        fields=None,
    ):
        # Pre-process the included arguments
        sort, _ = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            sort=sort,
        )

        # Post-process the return
        find_result = self._post_process_find(
            raw_find_result,
            include_similarity=False,
            _key="document",
        )

        return find_result

    def find_one_and_update(self, sort={}, update=None, filter=None, options=None):
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
        )

        return response

    def vector_find_one_and_update(
        self,
        vector,
        *,
        update=None,
        filter=None,
        fields=None,
    ):
        # Pre-process the included arguments
        sort, _ = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find_one_and_update(
            update=update,
            filter=filter,
            sort=sort,
        )

        # Post-process the return
        find_result = self._post_process_find(
            raw_find_result,
            include_similarity=False,
            _key="document",
        )

        return find_result

    def find_one(self, filter={}, projection={}, sort={}, options={}):
        json_query = make_payload(
            top_level="findOne",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._post(
            document=json_query,
        )

        return response

    def vector_find_one(
        self,
        vector,
        *,
        filter=None,
        fields=None,
        include_similarity=True,
    ):
        # Pre-process the included arguments
        sort, projection = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find_one(
            filter=filter,
            projection=projection,
            sort=sort,
        )

        # Post-process the return
        find_result = self._post_process_find(
            raw_find_result,
            include_similarity=include_similarity,
            _key="document",
        )

        return find_result

    def insert_one(self, document):
        json_query = make_payload(top_level="insertOne", document=document)

        response = self._request(
            method=http_methods.POST, path=self.base_path, json_data=json_query
        )

        return response

    def insert_many(self, documents, options=None, partial_failures_allowed=False):
        json_query = make_payload(
            top_level="insertMany", documents=documents, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
            skip_error_check=partial_failures_allowed,
        )

        return response

    def update_one(self, filter, update):
        json_query = make_payload(top_level="updateOne", filter=filter, update=update)

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

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
        )

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
        )

        return response

    def upsert(self, document):
        """
        Emulate an upsert operation for a single document,
        whereby a document is inserted if its _id is new, or completely
        replaces and existing one if that _id is already saved in the collection.
        Returns: the _id of the inserted document.
        """
        # Attempt to insert the given document
        result = self.insert_one(document)

        # Check if we hit an error
        if (
            "errors" in result
            and "errorCode" in result["errors"][0]
            and result["errors"][0]["errorCode"] == "DOCUMENT_ALREADY_EXISTS"
        ):
            # Now we attempt to update
            result = self.find_one_and_replace(
                filter={"_id": document["_id"]},
                replacement=document,
            )
            upserted_id = result["data"]["document"]["_id"]
        else:
            upserted_id = result["status"]["insertedIds"][0]

        return upserted_id


class AstraDB:
    def __init__(
        self,
        token=None,
        api_endpoint=None,
        namespace=None,
    ):
        if token is None or api_endpoint is None:
            raise AssertionError("Must provide token and api_endpoint")

        if namespace is None:
            logger.info(
                f"ASTRA_DB_KEYSPACE is not set. Defaulting to '{DEFAULT_KEYSPACE_NAME}'"
            )
            namespace = DEFAULT_KEYSPACE_NAME

        # Store the initial parameters
        self.token = token
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

    def _request(self, *args, skip_error_check=False, **kwargs):
        response = make_request(
            *args,
            **kwargs,
            base_url=self.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.token,
        )

        responsebody = response.json()

        if not skip_error_check and "errors" in responsebody:
            raise ValueError(json.dumps(responsebody["errors"]))
        else:
            return responsebody

    def collection(self, collection_name):
        return AstraDBCollection(collection_name=collection_name, astra_db=self)

    def get_collections(self):
        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data={"findCollections": {}},
        )

        return response

    def create_collection(
        self, collection_name, *, options=None, dimension=None, metric=""
    ):
        # Make sure we provide a collection name
        if not collection_name:
            raise ValueError("Must provide a collection name")

        # Initialize options if not passed
        if not options:
            options = {"vector": {}}
        elif "vector" not in options:
            options["vector"] = {}

        # Now check the remaining parameters - dimension
        if dimension:
            if "dimension" not in options["vector"]:
                options["vector"]["dimension"] = dimension
            else:
                raise ValueError(
                    "dimension parameter provided both in options and as function parameter."
                )

        # Check the metric parameter
        if metric:
            if "metric" not in options["vector"]:
                options["vector"]["metric"] = metric
            else:
                raise ValueError(
                    "metric parameter provided both in options as function parameter."
                )

        # Build the final json payload
        jsondata = {"name": collection_name, "options": options}

        # Make the request to the endpoitn
        self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": jsondata},
        )

        # Get the instance object as the return of the call
        return AstraDBCollection(astra_db=self, collection_name=collection_name)

    def delete_collection(self, collection_name):
        # Make sure we provide a collection name
        if not collection_name:
            raise ValueError("Must provide a collection name")

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": collection_name}},
        )

        return response
