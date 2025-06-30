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
from dataclasses import is_dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from astrapy.info import CreateTypeDefinition


UDT_TYPE = TypeVar("UDT_TYPE")
THE_DC_TYPE = TypeVar("THE_DC_TYPE")
SelfType = TypeVar("SelfType", bound="DataAPIUserDefinedType[Any]")


class DataAPIUserDefinedType(Generic[UDT_TYPE], ABC):
    """
    An abstract class wrapping some representation of a user-defined type (UDT)
    for use in a Table.

    This class signals the Data API that the wrapped object (e.g. a plain Python
    dictionary, or a third-party "model" such as one from Pydantic) is to be serialized
    with the wire format expected for UDTs on the write path.

    TODO details of the logic, usage sketch, extended example implementation ref.

    Attributes:
        TODO

    Example:
        TODO
    """

    _value: UDT_TYPE

    def __init__(self, value: UDT_TYPE) -> None:
        self._value = value

    @classmethod
    @abstractmethod
    def from_dict(
        cls: type[SelfType],
        raw_dict: dict[str, Any],
        *,
        definition: CreateTypeDefinition,
    ) -> SelfType: ...

    @abstractmethod
    def as_dict(self) -> dict[str, Any]: ...

    @property
    def value(self) -> UDT_TYPE:
        return self._value

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataAPIUserDefinedType):
            return self._value == other._value  # type: ignore[no-any-return]
        else:
            return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._value.__repr__()})"


class DictDataAPIUDT(DataAPIUserDefinedType[dict[str, Any]]):
    """
    TODO: docstring
    """

    def as_dict(self) -> dict[str, Any]:
        return self.value

    @classmethod
    def from_dict(
        cls: type[DictDataAPIUDT],
        raw_dict: dict[str, Any],
        *,
        definition: CreateTypeDefinition,
    ) -> DictDataAPIUDT:
        return DictDataAPIUDT(raw_dict)


def create_dataclass_userdefinedtype(
    _dataclass: type[THE_DC_TYPE],
) -> type[DataAPIUserDefinedType[THE_DC_TYPE]]:
    # TODO: docstring

    if not (is_dataclass(_dataclass) and isinstance(_dataclass, type)):
        raise TypeError(f"{_dataclass} is not a dataclass.")

    class DataclassDataAPIUDT(DataAPIUserDefinedType[THE_DC_TYPE]):
        def as_dict(self) -> dict[str, Any]:
            return self.value.__dict__

        @classmethod
        def from_dict(
            cls: type[DataclassDataAPIUDT],
            raw_dict: dict[str, Any],
            *,
            definition: CreateTypeDefinition,
        ) -> DataclassDataAPIUDT:
            return DataclassDataAPIUDT(_dataclass(**raw_dict))

    return DataclassDataAPIUDT
