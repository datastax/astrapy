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

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic

from typing_extensions import override

from astrapy import AsyncCollection, AsyncTable, Collection, Table
from astrapy.constants import (
    FilterType,
    HybridSortType,
    ProjectionType,
    normalize_optional_projection,
)
from astrapy.data.cursors.cursor import TRAW, logger
from astrapy.data.cursors.reranked_result import RerankedResult
from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.exceptions import (
    UnexpectedDataAPIResponseException,
    _TimeoutContext,
)

# TODO: remove this (setting + function) once API testable
MOCK_FARR_API = True


def mock_farr_response(pl: dict[str, Any] | None) -> dict[str, Any]:
    docs = [
        {
            "_id": f"doc_{doc_i}",
            "pl": str(pl).replace(" ", ""),
            "content": f"content {doc_i}",
            "$vectorize": f"vectorize {doc_i}",
            "metadata": {"m": f"d<{doc_i}>"},
        }
        for doc_i in range(5)
    ]
    doc_responses = [
        {
            "_id": f"doc_{doc_i}",
            "scores": {
                "$rerank": (6 - doc_i) / 8,
                "$vector": (7 - doc_i) / 9,
                "$lexical": (8 - doc_i) / 10,
            },
        }
        for doc_i in range(5)
    ]

    return {
        "data": {
            "documents": docs,
            "nextPageState": None,
        },
        "status": {
            "documentResponses": doc_responses,
        },
    }

    return []


class _QueryEngine(ABC, Generic[TRAW]):
    @abstractmethod
    def _fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        """Run a query for one page and return (entries, next-page-state, response.status)."""
        ...

    @abstractmethod
    async def _async_fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        """Run a query for one page and return (entries, next-page-state, response.status)."""
        ...


class _CollectionFindQueryEngine(Generic[TRAW], _QueryEngine[TRAW]):
    collection: Collection[TRAW] | None
    async_collection: AsyncCollection[TRAW] | None
    f_r_subpayload: dict[str, Any]
    f_options0: dict[str, Any]

    def __init__(
        self,
        *,
        collection: Collection[TRAW] | None,
        async_collection: AsyncCollection[TRAW] | None,
        filter: FilterType | None,
        projection: ProjectionType | None,
        sort: dict[str, Any] | None,
        limit: int | None,
        include_similarity: bool | None,
        include_sort_vector: bool | None,
        skip: int | None,
    ) -> None:
        self.collection = collection
        self.async_collection = async_collection
        self.f_r_subpayload = {
            k: v
            for k, v in {
                "filter": filter,
                "projection": normalize_optional_projection(projection),
                "sort": sort,
            }.items()
            if v is not None
        }
        self.f_options0 = {
            k: v
            for k, v in {
                "limit": limit if limit != 0 else None,
                "skip": skip,
                "includeSimilarity": include_similarity,
                "includeSortVector": include_sort_vector,
            }.items()
            if v is not None
        }

    @override
    def _fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.collection is None:
            raise RuntimeError("Query engine has no sync collection.")
        f_payload = {
            "find": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        converted_f_payload = preprocess_collection_payload(
            f_payload, options=self.collection.api_options.serdes_options
        )

        _page_str = page_state if page_state else "(empty page state)"
        _coll_name = self.collection.name if self.collection else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}")
        raw_f_response = self.collection._api_commander.request(
            payload=converted_f_payload,
            timeout_context=timeout_context,
        )
        logger.info(f"cursor finished fetching a page: {_page_str} from {_coll_name}")

        f_response = postprocess_collection_response(
            raw_f_response, options=self.collection.api_options.serdes_options
        )
        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def _async_fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        f_payload = {
            "find": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        converted_f_payload = preprocess_collection_payload(
            f_payload, options=self.async_collection.api_options.serdes_options
        )

        _page_str = page_state if page_state else "(empty page state)"
        _coll_name = self.async_collection.name if self.async_collection else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}, async")
        raw_f_response = await self.async_collection._api_commander.async_request(
            payload=converted_f_payload,
            timeout_context=timeout_context,
        )
        logger.info(
            f"cursor finished fetching a page: {_page_str} from {_coll_name}, async"
        )

        f_response = postprocess_collection_response(
            raw_f_response,
            options=self.async_collection.api_options.serdes_options,
        )
        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)


class _TableFindQueryEngine(Generic[TRAW], _QueryEngine[TRAW]):
    table: Table[TRAW] | None
    async_table: AsyncTable[TRAW] | None
    include_similarity: bool | None
    include_sort_vector: bool | None
    f_r_subpayload: dict[str, Any]
    f_options0: dict[str, Any]

    def __init__(
        self,
        *,
        table: Table[TRAW] | None,
        async_table: AsyncTable[TRAW] | None,
        filter: FilterType | None,
        projection: ProjectionType | None,
        sort: dict[str, Any] | None,
        limit: int | None,
        include_similarity: bool | None,
        include_sort_vector: bool | None,
        skip: int | None,
    ) -> None:
        self.table = table
        self.async_table = async_table
        self.include_similarity = include_similarity
        self.include_sort_vector = include_sort_vector
        self.f_r_subpayload = {
            k: v
            for k, v in {
                "filter": filter,
                "projection": normalize_optional_projection(projection),
                "sort": sort,
            }.items()
            if v is not None
        }
        self.f_options0 = {
            k: v
            for k, v in {
                "limit": limit if limit != 0 else None,
                "skip": skip,
                "includeSimilarity": include_similarity,
                "includeSortVector": include_sort_vector,
            }.items()
            if v is not None
        }

    @override
    def _fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.table is None:
            raise RuntimeError("Query engine has no sync table.")
        f_payload = self.table._converter_agent.preprocess_payload(
            {
                "find": {
                    **self.f_r_subpayload,
                    "options": {
                        **self.f_options0,
                        **({"pageState": page_state} if page_state else {}),
                    },
                },
            },
            map2tuple_checker=None,
        )

        _page_str = page_state if page_state else "(empty page state)"
        _table_name = self.table.name if self.table else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_table_name}")
        f_response = self.table._api_commander.request(
            payload=f_payload,
            timeout_context=timeout_context,
        )
        logger.info(f"cursor finished fetching a page: {_page_str} from {_table_name}")

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'documents'.",
                raw_response=f_response,
            )
        if "projectionSchema" not in f_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'projectionSchema'.",
                raw_response=f_response,
            )
        p_documents = self.table._converter_agent.postprocess_rows(
            f_response["data"]["documents"],
            columns_dict=f_response["status"]["projectionSchema"],
            similarity_pseudocolumn="$similarity" if self.include_similarity else None,
        )
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def _async_fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.async_table is None:
            raise RuntimeError("Query engine has no async table.")
        f_payload = self.async_table._converter_agent.preprocess_payload(
            {
                "find": {
                    **self.f_r_subpayload,
                    "options": {
                        **self.f_options0,
                        **({"pageState": page_state} if page_state else {}),
                    },
                },
            },
            map2tuple_checker=None,
        )

        _page_str = page_state if page_state else "(empty page state)"
        _table_name = self.async_table.name if self.async_table else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_table_name}")
        f_response = await self.async_table._api_commander.async_request(
            payload=f_payload,
            timeout_context=timeout_context,
        )
        logger.info(f"cursor finished fetching a page: {_page_str} from {_table_name}")

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'documents'.",
                raw_response=f_response,
            )
        if "projectionSchema" not in f_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'projectionSchema'.",
                raw_response=f_response,
            )
        p_documents = self.async_table._converter_agent.postprocess_rows(
            f_response["data"]["documents"],
            columns_dict=f_response["status"]["projectionSchema"],
            similarity_pseudocolumn="$similarity" if self.include_similarity else None,
        )
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)


class _CollectionFindAndRerankQueryEngine(
    Generic[TRAW], _QueryEngine[RerankedResult[TRAW]]
):
    collection: Collection[TRAW] | None
    async_collection: AsyncCollection[TRAW] | None
    f_r_subpayload: dict[str, Any]
    f_options0: dict[str, Any]

    def __init__(
        self,
        *,
        collection: Collection[TRAW] | None,
        async_collection: AsyncCollection[TRAW] | None,
        filter: FilterType | None,
        projection: ProjectionType | None,
        sort: HybridSortType | None,
        limit: int | None,
        hybrid_limits: int | dict[str, int] | None,
        hybrid_projection: str | None,
        rerank_on: str | None,
    ) -> None:
        self.collection = collection
        self.async_collection = async_collection
        self.f_r_subpayload = {
            k: v
            for k, v in {
                "filter": filter,
                "projection": normalize_optional_projection(projection),
                "sort": sort,
            }.items()
            if v is not None
        }
        self.f_options0 = {
            k: v
            for k, v in {
                "limit": limit if limit != 0 else None,
                "hybridLimits": hybrid_limits if hybrid_limits != 0 else None,
                "rerankOn": rerank_on,
                "hybridProjection": hybrid_projection,
            }.items()
            if v is not None
        }

    @override
    def _fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[RerankedResult[TRAW]], str | None, dict[str, Any] | None]:
        if self.collection is None:
            raise RuntimeError("Query engine has no sync collection.")
        f_payload = {
            "findAndRerank": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        converted_f_payload = preprocess_collection_payload(
            f_payload, options=self.collection.api_options.serdes_options
        )

        _page_str = page_state if page_state else "(empty page state)"
        _coll_name = self.collection.name if self.collection else "(none)"

        f_response: dict[str, Any]
        if MOCK_FARR_API:
            logger.info("MOCKING FARR API: '%s'", str(converted_f_payload))
            f_response = mock_farr_response(converted_f_payload)
        else:
            logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}")
            raw_f_response = self.collection._api_commander.request(
                payload=converted_f_payload,
                timeout_context=timeout_context,
            )
            logger.info(
                f"cursor finished fetching a page: {_page_str} from {_coll_name}"
            )
            f_response = postprocess_collection_response(
                raw_f_response, options=self.collection.api_options.serdes_options
            )

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findAndRerank API command (no 'documents').",
                raw_response=f_response,
            )
        if "documentResponses" not in f_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findAndRerank API command (no 'documentResponses').",
                raw_response=f_response,
            )
        p_documents: list[RerankedResult[TRAW]]
        if f_response["data"]["documents"]:
            p_documents = [
                RerankedResult(document=document, scores=doc_response["scores"])
                for document, doc_response in zip(
                    f_response["data"]["documents"],
                    f_response["status"]["documentResponses"],
                )
            ]
        else:
            p_documents = []
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def _async_fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[RerankedResult[TRAW]], str | None, dict[str, Any] | None]:
        if self.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        f_payload = {
            "findAndRerank": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        converted_f_payload = preprocess_collection_payload(
            f_payload, options=self.async_collection.api_options.serdes_options
        )

        _page_str = page_state if page_state else "(empty page state)"
        _coll_name = self.async_collection.name if self.async_collection else "(none)"

        f_response: dict[str, Any]
        if MOCK_FARR_API:
            logger.info("MOCKING FARR API: '%s'", str(converted_f_payload))
            f_response = mock_farr_response(converted_f_payload)
        else:
            logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}, async")
            raw_f_response = await self.async_collection._api_commander.async_request(
                payload=converted_f_payload,
                timeout_context=timeout_context,
            )
            logger.info(
                f"cursor finished fetching a page: {_page_str} from {_coll_name}, async"
            )
            f_response = postprocess_collection_response(
                raw_f_response,
                options=self.async_collection.api_options.serdes_options,
            )

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findAndRerank API command (no 'documents').",
                raw_response=f_response,
            )
        if "documentResponses" not in f_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findAndRerank API command (no 'documentResponses').",
                raw_response=f_response,
            )
        p_documents: list[RerankedResult[TRAW]]
        if f_response["data"]["documents"]:
            p_documents = [
                RerankedResult(document=document, scores=doc_response["scores"])
                for document, doc_response in zip(
                    f_response["data"]["documents"],
                    f_response["status"]["documentResponses"],
                )
            ]
        else:
            p_documents = []
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)
