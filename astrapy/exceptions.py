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

from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from astrapy.results import InsertManyResult


@dataclass
class DataAPIErrorDescriptor():
    error_code: Optional[str]
    message: Optional[str]
    def __init__(self, error_dict: Dict[str, str]) -> None:
        self.error_code = error_dict.get("errorCode")
        self.message = error_dict.get("message")


class DataAPIException(ValueError):
    text: Optional[str]
    error_descriptors: List[DataAPIErrorDescriptor]
    raw_responses: List[Dict[str, Any]]
    def __init__(
        self,
        text: Optional[str],
        *,
        error_descriptors: List[DataAPIErrorDescriptor],
        raw_responses: List[Dict[str, Any]],
    ) -> None:
        super().__init__(text)
        self.text = text
        self.error_descriptors = error_descriptors        
        self.raw_responses = raw_responses

    @staticmethod
    def from_response(
        raw_response: Dict[str, Any],
    ) -> DataAPIException:
        return DataAPIException.from_responses(raw_responses=[raw_response])

    @staticmethod
    def from_responses(
        raw_responses: List[Dict[str, Any]],
    ) -> DataAPIException:
        err_dicts = [
            err_dict
            for raw_response in raw_responses
            for err_dict in raw_response.get("errors", [])
        ]
        error_descriptors = [
            DataAPIErrorDescriptor(err_dict)
            for err_dict in err_dicts
        ]
        return DataAPIException(
            text=error_descriptors[0].message,
            error_descriptors=error_descriptors,
            raw_responses=raw_responses,
        )
        

@dataclass
class InsertManyException(Exception):
    base_exception: DataAPIException
    partial_result: InsertManyResult


# class PaginatedOperationException(DataAPIException):
#     previous_responses: List[Dict[str, Any]]
#     def __init__(self, text: str, *, raw_errors: List[Dict[str, Any]], previous_responses: List[Dict[str, Any]]) -> None:
#         super().__init__(text, raw_errors=raw_errors)
#         self.previous_responses = previous_responses


# # some of these
# class InsertManyException(PaginatedOperationException):
#     partial_result: InsertManyResult
#     def __init__(self, text: str, *, raw_errors: List[Dict[str, Any]], previous_responses: List[Dict[str, Any]], partial_result: InsertManyResult) -> None:
#         super().__init__(text, raw_errors=raw_errors, previous_responses=previous_responses)
#         self.partial_result = partial_result


# """
# e = DataAPIException("boo", [{'a':1}])

# """


# @dataclass
# class InsertManyException(Exception):
#     base_errors: List[List[Dict[str, Any]]]
#     result: InsertManyResult
