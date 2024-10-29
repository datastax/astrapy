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
from typing import Iterator

# Floats are always encoded big-endian with 4 bytes per float.
ENDIANNESS_CHAR = ">"
BYTES_PER_FLOAT = 4


def floats_to_bytes(float_list: list[float], n: int | None = None) -> bytes:
    _n = len(float_list) if n is None else n
    fmt = f"{ENDIANNESS_CHAR}{'f' * _n}"
    return struct.pack(fmt, *float_list)


def bytes_to_floats(byte_blob: bytes, n: int | None = None) -> list[float]:
    _n = len(byte_blob) // BYTES_PER_FLOAT if n is None else n
    fmt = f"{ENDIANNESS_CHAR}{'f' * _n}"
    return list(struct.unpack(fmt, byte_blob))


@dataclass
class DataAPIVector(UserList[float]):
    """
    TODO
    """

    data: list[float]
    n: int

    def __init__(self, vector: list[float] = []) -> None:
        self.data = vector
        self.n = len(self.data)

    def __iter__(self) -> Iterator[float]:
        return iter(self.data)

    def to_bytes(self) -> bytes:
        return floats_to_bytes(self.data, self.n)

    @staticmethod
    def from_bytes(byte_blob: bytes) -> DataAPIVector:
        return DataAPIVector(bytes_to_floats(byte_blob))
