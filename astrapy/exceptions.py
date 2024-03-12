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
class DataAPIErrorDescriptor:
    error_code: Optional[str]
    message: Optional[str]
    attributes: Dict[str, Any]

    def __init__(self, error_dict: Dict[str, str]) -> None:
        self.error_code = error_dict.get("errorCode")
        self.message = error_dict.get("message")
        self.attributes = {
            k: v for k, v in error_dict.items() if k not in {"errorCode", "message"}
        }


@dataclass
class DataAPIDetailedErrorDescriptor:
    error_descriptors: List[DataAPIErrorDescriptor]
    command: Dict[str, Any]
    raw_response: Dict[str, Any]


@dataclass
class DataAPIException(ValueError):

    text: Optional[str]
    error_descriptors: List[DataAPIErrorDescriptor]
    detailed_error_descriptors: List[DataAPIDetailedErrorDescriptor]

    def __init__(
        self,
        text: Optional[str],
        *,
        error_descriptors: List[DataAPIErrorDescriptor],
        detailed_error_descriptors: List[DataAPIDetailedErrorDescriptor],
    ) -> None:
        super().__init__(text)
        self.text = text
        self.error_descriptors = error_descriptors
        self.detailed_error_descriptors = detailed_error_descriptors

    @classmethod
    def from_response(
        cls,
        command: Dict[str, Any],
        raw_response: Dict[str, Any],
        **kwargs: Any,
    ) -> DataAPIException:
        return cls.from_responses(
            commands=[command],
            raw_responses=[raw_response],
            **kwargs,
        )

    @classmethod
    def from_responses(
        cls,
        commands: List[Dict[str, Any]],
        raw_responses: List[Dict[str, Any]],
        **kwargs: Any,
    ) -> DataAPIException:
        detailed_error_descriptors: List[DataAPIDetailedErrorDescriptor] = []
        for command, raw_response in zip(commands, raw_responses):
            if raw_response.get("errors", []):
                error_descriptors = [
                    DataAPIErrorDescriptor(error_dict)
                    for error_dict in raw_response["errors"]
                ]
                detailed_error_descriptor = DataAPIDetailedErrorDescriptor(
                    error_descriptors=error_descriptors,
                    command=command,
                    raw_response=raw_response,
                )
                detailed_error_descriptors.append(detailed_error_descriptor)

        # flatten
        error_descriptors = [
            error_descriptor
            for d_e_d in detailed_error_descriptors
            for error_descriptor in d_e_d.error_descriptors
        ]

        if error_descriptors:
            text = error_descriptors[0].message
        else:
            text = ""

        return cls(
            text,
            error_descriptors=error_descriptors,
            detailed_error_descriptors=detailed_error_descriptors,
            **kwargs,
        )


@dataclass
class InsertManyException(DataAPIException):
    partial_result: InsertManyResult
