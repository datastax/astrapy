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
from typing import Generic

from astrapy.data.cursors.cursor import TRAW
from astrapy.data_types import DataAPIVector


@dataclass
class FindPage(Generic[TRAW]):
    """
    A whole pageful of results from a find operation. This object represents
    the form taken by the returned items when using the pagination interface
    explicitly.

    Attributes:
        results: the list of entries obtained on the retrieved page (possibly
            after applying a mapping function, if one is specified in the cursor).
        next_page_state: a string encoding the pagination state. If the find
            operation does not admit any further page, this is returned as None.
            Otherwise, its value can be used to resume consuming the `find`
            results on another cursor instantiated independently later on.
        sort_vector: if the find operation was done with the "include
            sort vector" flag set to True, and the sort criterion is a vector sorting,
            this contains the query vector used for the search. The query vector is
            expressed as a list of floats or a DataAPIVector depending on the serdes
            settings for the collection/table that originated the cursor.
            If not applicable, this attribute is returned as None.
    """

    results: list[TRAW]
    next_page_state: str | None
    sort_vector: list[float] | DataAPIVector | None

    def __repr__(self) -> str:
        pieces = [
            pc
            for pc in (
                f"results=<{len(self.results)} entries>",
                "next_page_state=..." if self.next_page_state else None,
                "sort_vector=..." if self.sort_vector else None,
            )
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(pieces)})"


@dataclass
class FindAndRerankPage(Generic[TRAW]):
    """
    A whole pageful of results from a findAndRerank operation. This object represents
    the form taken by the returned items when using the pagination interface
    explicitly.

    Attributes:
        results: the list of entries obtained on the retrieved page (possibly
            after applying a mapping function, if one is specified in the cursor).
            In absence of mapping functions, this is a list of RerankedResult objects.
        next_page_state: a string encoding the pagination state. If the find-and-rerank
            operation does not admit any further page, this is returned as None.
            Otherwise, its value can be used to resume consuming the `find_and_rerank`
            results on another cursor instantiated independently later on.
        sort_vector: if the find-and-rerank operation was done with the "include
            sort vector" flag set to True, and the sort criterion is a vector sorting,
            this contains the query vector used for the search. The query vector is
            expressed as a list of floats or a DataAPIVector depending on the serdes
            settings for the collection/table that originated the cursor.
            If not applicable, this attribute is returned as None.
    """

    results: list[TRAW]
    next_page_state: str | None
    sort_vector: list[float] | DataAPIVector | None

    def __repr__(self) -> str:
        pieces = [
            pc
            for pc in (
                f"results=<{len(self.results)} entries>",
                "next_page_state=..." if self.next_page_state else None,
                "sort_vector=..." if self.sort_vector else None,
            )
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(pieces)})"
