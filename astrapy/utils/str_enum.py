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

from enum import Enum, EnumMeta
from typing import TypeVar

T = TypeVar("T", bound="StrEnum")


class StrEnumMeta(EnumMeta):
    def _name_lookup(cls, value: str) -> str | None:
        """Return a proper key in the enum if some matching logic works, or None."""
        mmap = {k: v.value for k, v in cls._member_map_.items()}
        # try exact key match
        if value in mmap:
            return value
        # try case-insensitive key match
        u_value = value.upper()
        u_mmap = {k.upper(): k for k in mmap.keys()}
        if u_value in u_mmap:
            return u_mmap[u_value]
        # try case-insensitive *value* match
        v_mmap = {v.upper(): k for k, v in mmap.items()}
        if u_value in v_mmap:
            return v_mmap[u_value]
        return None

    def __contains__(cls, value: object) -> bool:
        """Return True if the provided string belongs to the enum."""
        if isinstance(value, str):
            return cls._name_lookup(value) is not None
        return False


class StrEnum(Enum, metaclass=StrEnumMeta):
    @classmethod
    def coerce(cls: type[T], value: str | T) -> T:
        """
        Accepts either a string or an instance of the Enum itself.
        If a string is passed, it converts it to the corresponding
        Enum value (case-insensitive).
        If an Enum instance is passed, it returns it as-is.
        Raises ValueError if the string does not match any enum member.
        """

        if isinstance(value, cls):
            return value
        elif isinstance(value, str):
            norm_value = cls._name_lookup(value)
            if norm_value is not None:
                return cls[norm_value]
            # no matches
            raise ValueError(
                f"Invalid value '{value}' for {cls.__name__}. "
                f"Allowed values are: {[e.value for e in cls]}"
            )
        raise ValueError(
            f"Invalid value '{value}' for {cls.__name__}. "
            f"Allowed values are: {[e.value for e in cls]}"
        )
