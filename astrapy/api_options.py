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
from typing import Optional, TypeVar


AO = TypeVar("AO", bound="BaseAPIOptions")


@dataclass
class BaseAPIOptions:
    """
    TODO_VECTORIZE
    """

    max_time_ms: Optional[int] = None

    def with_default(self: AO, default: Optional[BaseAPIOptions]) -> AO:
        if default:
            return self.__class__(
                **{
                    **default.__dict__,
                    **{k: v for k, v in self.__dict__.items() if v is not None},
                }
            )
        else:
            return self

    def with_override(self: AO, override: Optional[BaseAPIOptions]) -> AO:
        if override:
            return self.__class__(
                **{
                    **{k: v for k, v in self.__dict__.items() if v is not None},
                    **override.__dict__,
                }
            )
        else:
            return self


@dataclass
class CollectionAPIOptions(BaseAPIOptions):
    """
    TODO_VECTORIZE
    """

    embedding_api_key: Optional[str] = None
