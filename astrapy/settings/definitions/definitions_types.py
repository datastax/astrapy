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

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from astrapy.data_types import DataAPIVector

if TYPE_CHECKING:
    from astrapy.info import CreateTypeDefinition


DefaultDocumentType = Dict[str, Any]
DefaultRowType = Dict[str, Any]
ProjectionType = Union[
    Iterable[str], Dict[str, Union[bool, Dict[str, Union[int, Iterable[int]]]]]
]
SortType = Dict[str, Any]
HybridSortType = Dict[
    str, Union[str, Dict[str, Union[str, List[float], DataAPIVector]]]
]
FilterType = Dict[str, Any]
CallerType = Tuple[Optional[str], Optional[str]]
SerializerFunctionType = Callable[[Any], Dict[str, Any]]
UDTDeserializerFunctionType = Callable[
    [Dict[str, Any], Optional["CreateTypeDefinition"]], Any
]

ROW = TypeVar("ROW")
ROW2 = TypeVar("ROW2")
DOC = TypeVar("DOC")
DOC2 = TypeVar("DOC2")
