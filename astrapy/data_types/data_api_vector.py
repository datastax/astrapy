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

import struct
from collections import UserList
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    FloatList = UserList[float]
else:
    FloatList = UserList

# Floats are always encoded big-endian with 4 bytes per float.
ENDIANNESS_CHAR = ">"
BYTES_PER_FLOAT = 4


def floats_to_bytes(float_list: list[float], n: int | None = None) -> bytes:
    """
    Convert a list of floats into a binary blob according to the Data API's conventions,
    suitable for working with the "vector" table column type.

    Args:
        float_list: a list of n float numbers to convert.
        n: the number of components. If not provided, it is determined automatically.

    Returns:
        a bytes object expressing the input list of floats in binary-encoded form.
    """

    _n = len(float_list) if n is None else n
    fmt = f"{ENDIANNESS_CHAR}{'f' * _n}"
    return struct.pack(fmt, *float_list)


def bytes_to_floats(byte_blob: bytes, n: int | None = None) -> list[float]:
    """
    Convert a binary blob into a list of floats according to the Data API's conventions.

    Args:
        byte_blob: binary object encoding a list of floats.
        n: the number of components of the resulting list. If not provided,
            it is determined automatically.

    Returns:
        a list of floats, of the same contents as the input binary-encoded sequence.
    """

    _n = len(byte_blob) // BYTES_PER_FLOAT if n is None else n
    fmt = f"{ENDIANNESS_CHAR}{'f' * _n}"
    return list(struct.unpack(fmt, byte_blob))


@dataclass
class DataAPIVector(FloatList):
    r"""
    A class wrapping a list of float numbers to be treated as a "vector" within the
    Data API. This class has the same functionalities as the underlying `list[float]`,
    plus it can be used to signal the Data API that a certain list of numbers can
    be encoded as a binary object (which improves on the performance and bandwidth of
    the write operations to the Data API).

    Attributes:
        data: a list of float numbers, the underlying content of the vector
        n: the number of components, i.e. the length of the list.

    Example:
        >>> from astrapy.data_types import DataAPIVector
        >>>
        >>> v1 = DataAPIVector([0.1, -0.2, 0.3])
        >>> print(v1.to_bytes())
        b'=\xcc\xcc\xcd\xbeL\xcc\xcd>\x99\x99\x9a'
        >>> DataAPIVector.from_bytes(b"=\xcc\xcc\xcd\xbeL\xcc\xcd>\x99\x99\x9a")
        DataAPIVector([0.10000000149011612, -0.20000000298023224, 0.30000001192092896])
        >>> for i, x in enumerate(v1):
        ...     print(f"component {i} => {x}")
        ...
        component 0 => 0.1
        component 1 => -0.2
        component 2 => 0.3
    """

    data: list[float]
    n: int

    def __init__(self, vector: list[float] = []) -> None:
        self.data = vector
        self.n = len(self.data)

    def __iter__(self) -> Iterator[float]:
        return iter(self.data)

    def __hash__(self) -> int:
        return hash(tuple(self.data))

    def __repr__(self) -> str:
        if self.n < 5:
            return f"{self.__class__.__name__}({self.data})"
        else:
            data_start = f"[{', '.join(str(x) for x in self.data[:3])} ...]"
            return f"{self.__class__.__name__}({data_start}, n={self.n})"

    def __str__(self) -> str:
        if self.n < 5:
            return str(self.data)
        else:
            return f"[{', '.join(str(x) for x in self.data[:3])} ...]"

    def to_bytes(self) -> bytes:
        """
        Convert the vector into its binary blob (`bytes`) representation, according
        to the Data API convention (including endianness).

        Returns:
            a `bytes` object, expressing the vector values in a lossless way.
        """

        return floats_to_bytes(self.data, self.n)

    @staticmethod
    def from_bytes(byte_blob: bytes) -> DataAPIVector:
        """
        Create a DataAPIVector from a binary blob, decoding its contents according
        to the Data API convention (including endianness).

        Args:
            byte_blob: a binary sequence, encoding a vector of floats as specified
            by the Data API convention.

        Returns:
            a DataAPIVector corresponding to the provided blob.
        """

        return DataAPIVector(bytes_to_floats(byte_blob))
