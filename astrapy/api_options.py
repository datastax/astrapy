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

from dataclasses import dataclass, field
from typing import Optional, TypeVar

from astrapy.authentication import (
    EmbeddingHeadersProvider,
    StaticEmbeddingHeadersProvider,
)

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
                    **{k: v for k, v in self.__dict__.items() if bool(v)},
                }
            )
        else:
            return self

    def with_override(self: AO, override: Optional[BaseAPIOptions]) -> AO:
        if override:
            return self.__class__(
                **{
                    **self.__dict__,
                    **{k: v for k, v in override.__dict__.items() if bool(v)},
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
        embedding_api_key: an `astrapy.authentication.EmbeddingHeadersProvider`
            object, encoding embedding-related API keys that will be passed
            as headers when interacting with the collection (on each Data API request).
            The default value is `StaticEmbeddingHeadersProvider(None)`, i.e.
            no embedding-specific headers, whereas if the collection is configured
            with an embedding service other choices for this parameter can be
            meaningfully supplied. is configured for the collection,
    """

    embedding_api_key: EmbeddingHeadersProvider = field(
        default_factory=lambda: StaticEmbeddingHeadersProvider(None)
    )
