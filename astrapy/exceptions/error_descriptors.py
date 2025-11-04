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

from dataclasses import dataclass
from typing import Any


@dataclass
class DataAPIErrorDescriptor:
    """
    An object representing a single error, as returned from the Data API,
    typically with an error code, a text message and other properties.

    This object is used to describe errors received from the Data API,
    in the form of HTTP-200 ("success") responses containing errors

    Depending on the API command semantics, responses may express partial
    successes with some errors (for instance, an insertMany command inserting
    most of the documents/rows, but failing on a couple of incompatible inputs).

    Attributes:
        error_code: a string code as found in the API error's "errorCode" field.
        message: the text found in the API error's "message" field.
        title:  the text found in the API error's "title" field.
        family:  the text found in the API error's "family" field.
        scope:  the text found in the API error's "scope" field.
        id:  the text found in the API error's "id" field.
        attributes: a dict with any further key-value pairs returned by the API.
    """

    title: str | None
    error_code: str | None
    message: str | None
    family: str | None
    scope: str | None
    id: str | None
    attributes: dict[str, Any]

    _known_dict_fields = {
        "title",
        "errorCode",
        "message",
        "family",
        "scope",
        "id",
    }

    def __init__(self, error_dict: dict[str, str] | str) -> None:
        if isinstance(error_dict, str):
            self.message = error_dict
            self.title = None
            self.error_code = None
            self.family = None
            self.scope = None
            self.id = None
            self.attributes = {}
        else:
            self.title = error_dict.get("title")
            self.error_code = error_dict.get("errorCode")
            self.message = error_dict.get("message")
            self.family = error_dict.get("family")
            self.scope = error_dict.get("scope")
            self.id = error_dict.get("id")
            self.attributes = {
                k: v for k, v in error_dict.items() if k not in self._known_dict_fields
            }

    def __repr__(self) -> str:
        pieces = [
            f"{self.title.__repr__()}" if self.title else None,
            f"error_code={self.error_code.__repr__()}" if self.error_code else None,
            f"message={self.message.__repr__()}" if self.message else None,
            f"family={self.family.__repr__()}" if self.family else None,
            f"scope={self.scope.__repr__()}" if self.scope else None,
            f"id={self.id.__repr__()}" if self.id else None,
            f"attributes={self.attributes.__repr__()}" if self.attributes else None,
        ]
        return f"{self.__class__.__name__}({', '.join(pc for pc in pieces if pc)})"

    def __str__(self) -> str:
        return self.summary()

    def summary(self) -> str:
        """
        Determine a string succinct description of this descriptor.

        The precise format of this summary is determined by which fields are set.
        """
        non_code_part: str | None
        if self.title:
            if self.message:
                non_code_part = f"{self.title}: {self.message}"
            else:
                non_code_part = f"{self.title}"
        else:
            if self.message:
                non_code_part = f"{self.message}"
            else:
                non_code_part = None
        if self.error_code:
            if non_code_part:
                return f"{non_code_part} ({self.error_code})"
            else:
                return f"{self.error_code}"
        else:
            if non_code_part:
                return non_code_part
            else:
                return ""


@dataclass
class DataAPIWarningDescriptor(DataAPIErrorDescriptor):
    """
    An object representing a single warning, as returned from the Data API,
    typically with a code, a text message and other properties.

    This object is used to describe warnings received from the Data API,
    in the form of HTTP-200 ("success") responses with accompanying warnings.

    Attributes:
        error_code: a string code found in the API warning's "errorCode" field.
        message: the text found in the API warning's "message" field.
        title:  the text found in the API warning's "title" field.
        family:  the text found in the API warning's "family" field.
        scope:  the text found in the API warning's "scope" field.
        id:  the text found in the API warning's "id" field.
        attributes: a dict with any further key-value pairs returned by the API.
    """

    def __init__(self, error_dict: dict[str, str] | str) -> None:
        return DataAPIErrorDescriptor.__init__(self, error_dict=error_dict)
