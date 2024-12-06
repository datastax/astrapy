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

from enum import Enum
from typing import TypeVar

T = TypeVar("T", bound="StrEnum")


class StrEnum(Enum):
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
            try:
                return cls[value.upper()]
            except KeyError:
                raise ValueError(
                    f"Invalid value '{value}' for {cls.__name__}. "
                    f"Allowed values are: {[e.name.lower() for e in cls]}"
                )
        raise ValueError(
            f"Invalid value '{value}' for {cls.__name__}. "
            f"Allowed values are: {[e.name.lower() for e in cls]}"
        )
