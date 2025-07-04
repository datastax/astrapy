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
DA_UDT_WRAPPER = TypeVar("DA_UDT_WRAPPER", bound="DataAPIUserDefinedType[Any]")


class DataAPIUserDefinedType(Generic[UDT_TYPE], ABC):
    """
    A class wrapping a representation of a user-defined type (UDT) for a Table.

    This class (more precisely, concrete implementations of it) signals to the
    Data API that the wrapped object (e.g. a plain Python dictionary, or a
    third-party "model" such as one from Pydantic) is to be serialized with the
    wire format expected for UDTs on the write path.

    How to represent a UDT in the application code is the application's choice.
    Whatever the class used for this purpose, if a suitable DataAPIUserDefinedType
    wrapper class is available, it must be used to wrap the application-side
    representation, which ensures proper serialization when sending UDT values
    to the Data API.

    For the read path, i.e. when UDT values are encountered during reads, the same
    DataAPIUserDefinedType concrete subclass is used: this requires a mapping between
    the UDT names and the associated DataAPIUserDefinedType subclass to be specified
    as part of the serdes API options for Table object doing the read.
    Consult the documentation about SerdesOptions for more details and usage examples.

    Args:
        value: the object representing the UDT in the application. The type of this
            parameter is dictated by the application design. Popular choices may be
            a plain Python dictionary, a dataclass, a `TypedDict` or objects from
            third-party model frameworks such as Pydantic.
    """

    _value: UDT_TYPE

    def __init__(self, value: UDT_TYPE) -> None:
        self._value = value

    @classmethod
    @abstractmethod
    def from_dict(
        cls: type[DA_UDT_WRAPPER],
        raw_dict: dict[str, Any],
        *,
        definition: CreateTypeDefinition,
    ) -> DA_UDT_WRAPPER:
        """
        Convert a plain Python dictionary into the appropriate object that represents
        the user-defined type (UDT).

        Args:
            raw_dict: a Python dictionary expressing a UDT.
            definition: schema for the UDT, as returned by the Data API along with
                values, during each read. Concrete implementation of this method
                may choose to ignore this parameter unless they choose to perform
                special validation steps.

        Returns:
            an instance of the concrete implementation of this wrapper class.
        """

    @abstractmethod
    def as_dict(self) -> dict[str, Any]:
        """
        Convert this wrapped object, that represents a user-defined type (UDT),
        into a plain Python dictionary.

        Returns:
            a dictionary expressing the wrapped UDT.
        """

    @property
    def value(self) -> UDT_TYPE:
        """
        Access the wrapped object in this class, the one representing a user-defined type (UDT).
        """

        return self._value

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataAPIUserDefinedType):
            return self._value == other._value  # type: ignore[no-any-return]
        else:
            return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._value.__repr__()})"


class DataAPIUDT(DataAPIUserDefinedType[dict[str, Any]]):
    """
    A wrapper of plain Python dictionaries for columns of type
    'user-defined type' (UDT) in Tables.

    This is the default concrete implementation of `DataAPIUserDefinedType`, which will
    be used by the client to represent UDTs in absence of other specifications.

    See the `DataAPIUserDefinedType` class for the general mechanism.

    Example:
        table.insert_one({
            "id": "x",
            "player": DataAPIUDT({"name": "John", "age": 40}),
        })

        doc = table.find_one({"id": "x"})
        assert isinstance(doc["player"], DataAPIUDT)
        pl_dict = doc["player"].value
        print(f"{pl_dict['name']} ({pl_dict['age']})")
    """

    @classmethod
    def from_dict(
        cls: type[DataAPIUDT],
        raw_dict: dict[str, Any],
        *,
        definition: CreateTypeDefinition,
    ) -> DataAPIUDT:
        return DataAPIUDT(raw_dict)

    def as_dict(self) -> dict[str, Any]:
        return self.value


def create_dataclass_userdefinedtype(
    _dataclass: type[THE_DC_TYPE],
) -> type[DataAPIUserDefinedType[THE_DC_TYPE]]:
    """
    Utility factory function to quickly create a wrapper class, for use with Tables,
    out of an existing Python `dataclass.

    The wrapper class is then ready for use in both the write and read path, as
    illustrated for the general case of class `DataAPIUserDefinedType`.
    For reads, an additional step is required to associate a certain UDT with this
    wrapper (in the SerdesOptions for the Table performing the read).

    Example:
        from dataclasses import dataclass
        from astrapy.api_options import APIOptions, SerdesOptions

        @dataclass
        class Player:
            name: str
            age: int

        PlayerWrapper = create_dataclass_userdefinedtype(Player)
        my_player = Player(name="John", age=40)

        # associate the UDT named 'player_udt' to a specific UDT wrapper:
        table_dataclass = table.with_options(api_options=APIOptions(
            serdes_options=SerdesOptions(
                udt_class_map={"player_udt": PlayerWrapper},
            ),
        ))

        table_dataclass.insert_one({
            "id": "x",
            "player": PlayerWrapper(my_player),
        })

        doc = table_dataclass.find_one({"id": "x"})
        assert isinstance(doc["player"], PlayerWrapper)
        pl_object = doc["player"].value
        print(f"{pl_object.name} ({pl_object.age})")
    """

    if not (is_dataclass(_dataclass) and isinstance(_dataclass, type)):
        raise TypeError(f"{_dataclass} is not a dataclass.")

    class DataclassDataAPIUDT(DataAPIUserDefinedType[THE_DC_TYPE]):
        def as_dict(self) -> dict[str, Any]:
            return {k: v for k, v in self.value.__dict__.items() if v is not None}

        @classmethod
        def from_dict(
            cls: type[DataclassDataAPIUDT],
            raw_dict: dict[str, Any],
            *,
            definition: CreateTypeDefinition,
        ) -> DataclassDataAPIUDT:
            return DataclassDataAPIUDT(_dataclass(**raw_dict))

    return DataclassDataAPIUDT
