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

from collections.abc import Callable, Iterable
from typing import (
    Any,
    TypeVar,
)

from astrapy.data.info.table_descriptor.type_creation import CreateTypeDefinition
from astrapy.data_types import DataAPIVector

DefaultDocumentType = dict[str, Any]
DefaultRowType = dict[str, Any]
ProjectionType = Iterable[str] | dict[str, bool | dict[str, int | Iterable[int]]]
SortType = dict[str, Any]
HybridSortType = dict[str, str | dict[str, str | list[float] | DataAPIVector]]
FilterType = dict[str, Any]
CallerType = tuple[str | None, str | None]
SerializerFunctionType = Callable[[Any], dict[str, Any]]
UDTDeserializerFunctionType = Callable[
    [dict[str, Any], CreateTypeDefinition | None], Any
]

ROW = TypeVar("ROW")
ROW2 = TypeVar("ROW2")
DOC = TypeVar("DOC")
DOC2 = TypeVar("DOC2")
