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
    filter: FilterType | None
    projection: ProjectionType | None
    sort: dict[str, Any] | None
    limit: int | None
    include_similarity: bool | None
    include_sort_vector: bool | None
    skip: int | None
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
        self.filter = filter
        self.projection = projection
        self.sort = sort
        self.limit = limit
        self.include_similarity = include_similarity
        self.include_sort_vector = include_sort_vector
        self.skip = skip
        self.f_r_subpayload = {
            k: v
            for k, v in {
                "filter": self.filter,
                "projection": normalize_optional_projection(self.projection),
                "sort": self.sort,
            }.items()
            if v is not None
        }
        self.f_options0 = {
            k: v
            for k, v in {
                "limit": self.limit or None,
                "skip": self.skip,
                "includeSimilarity": self.include_similarity,
                "includeSortVector": self.include_sort_vector,
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
    filter: FilterType | None
    projection: ProjectionType | None
    sort: dict[str, Any] | None
    limit: int | None
    include_similarity: bool | None
    include_sort_vector: bool | None
    skip: int | None
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
        self.filter = filter
        self.projection = projection
        self.sort = sort
        self.limit = limit
        self.include_similarity = include_similarity
        self.include_sort_vector = include_sort_vector
        self.skip = skip
        self.f_r_subpayload = {
            k: v
            for k, v in {
                "filter": self.filter,
                "projection": normalize_optional_projection(self.projection),
                "sort": self.sort,
            }.items()
            if v is not None
        }
        self.f_options0 = {
            k: v
            for k, v in {
                "limit": self.limit or None,
                "skip": self.skip,
                "includeSimilarity": self.include_similarity,
                "includeSortVector": self.include_sort_vector,
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
    filter: FilterType | None
    projection: ProjectionType | None
    sort: HybridSortType | None
    limit: int | None
    hybrid_limits: int | dict[str, int] | None
    include_scores: bool | None
    include_sort_vector: bool | None
    rerank_on: str | None
    rerank_query: str | None
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
        include_scores: bool | None,
        include_sort_vector: bool | None,
        rerank_on: str | None,
        rerank_query: str | None,
    ) -> None:
        self.collection = collection
        self.async_collection = async_collection
        self.filter = filter
        self.projection = projection
        self.sort = sort
        self.limit = limit
        self.hybrid_limits = hybrid_limits
        self.include_scores = include_scores
        self.include_sort_vector = include_sort_vector
        self.rerank_on = rerank_on
        self.rerank_query = rerank_query
        self.f_r_subpayload = {
            k: v
            for k, v in {
                "filter": self.filter,
                "projection": normalize_optional_projection(self.projection),
                "sort": self.sort,
            }.items()
            if v is not None
        }
        self.f_options0 = {
            k: v
            for k, v in {
                "limit": self.limit or None,
                "hybridLimits": self.hybrid_limits or None,
                "includeScores": self.include_scores,
                "includeSortVector": self.include_sort_vector,
                "rerankOn": self.rerank_on,
                "rerankQuery": self.rerank_query,
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

        logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}")
        raw_f_response = self.collection._api_commander.request(
            payload=converted_f_payload,
            timeout_context=timeout_context,
        )
        logger.info(f"cursor finished fetching a page: {_page_str} from {_coll_name}")
        f_response: dict[str, Any] = postprocess_collection_response(
            raw_f_response, options=self.collection.api_options.serdes_options
        )

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findAndRerank API command (no 'documents').",
                raw_response=f_response,
            )
        # the presence of 'documentResponses' is not guaranteed (depends on option flags)
        response_status = f_response.get("status") or {}
        documents: list[TRAW] = f_response["data"]["documents"]
        document_responses: list[dict[str, Any]]
        if "documentResponses" in response_status:
            document_responses = response_status["documentResponses"]
        else:
            document_responses = [{}] * len(documents)

        p_documents: list[RerankedResult[TRAW]]
        p_documents = [
            RerankedResult(document=document, scores=doc_response.get("scores") or {})
            for document, doc_response in zip(
                documents,
                document_responses,
            )
        ]
        n_p_state = (f_response.get("data") or {}).get("nextPageState")
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

        logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}, async")
        raw_f_response = await self.async_collection._api_commander.async_request(
            payload=converted_f_payload,
            timeout_context=timeout_context,
        )
        logger.info(
            f"cursor finished fetching a page: {_page_str} from {_coll_name}, async"
        )
        f_response: dict[str, Any] = postprocess_collection_response(
            raw_f_response,
            options=self.async_collection.api_options.serdes_options,
        )

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findAndRerank API command (no 'documents').",
                raw_response=f_response,
            )
        # the presence of 'documentResponses' is not guaranteed (depends on option flags)
        response_status = f_response.get("status") or {}
        documents: list[TRAW] = f_response["data"]["documents"]
        document_responses: list[dict[str, Any]]
        if "documentResponses" in response_status:
            document_responses = response_status["documentResponses"]
        else:
            document_responses = [{}] * len(documents)

        p_documents: list[RerankedResult[TRAW]]
        p_documents = [
            RerankedResult(document=document, scores=doc_response.get("scores") or {})
            for document, doc_response in zip(
                documents,
                document_responses,
            )
        ]
        n_p_state = (f_response.get("data") or {}).get("nextPageState")
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)
