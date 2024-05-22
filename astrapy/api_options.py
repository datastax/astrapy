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
    A description of the options about how to interact with the Data API.

    Attributes:
        max_time_ms: a default timeout, in millisecond, for the duration of each
            operation on the collection. Individual timeouts can be provided to
            each collection method call and will take precedence, with this value
            being an overall default.
            Note that for some methods involving multiple API calls (such as
            `find`, `delete_many`, `insert_many` and so on), it is strongly suggested
            to provide a specific timeout as the default one likely wouldn't make
            much sense.
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
                    **self.__dict__,
                    **{k: v for k, v in override.__dict__.items() if v is not None},
                }
            )
        else:
            return self


@dataclass
class CollectionAPIOptions(BaseAPIOptions):
    """
    A description of the options about how to interact with the Data API
    regarding a collection.
    Developers should not instantiate this class directly.

    Attributes:
        max_time_ms: a default timeout, in millisecond, for the duration of each
            operation on the collection. Individual timeouts can be provided to
            each collection method call and will take precedence, with this value
            being an overall default.
            Note that for some methods involving multiple API calls (such as
            `find`, `delete_many`, `insert_many` and so on), it is strongly suggested
            to provide a specific timeout as the default one likely wouldn't make
            much sense.
        embedding_api_key: an optional API key for interacting with the collection.
            If an embedding service is configured, and this attribute is set,
            each Data API call will include a "x-embedding-api-key" header
            with the value of this attribute.
    """

    embedding_api_key: Optional[str] = None
