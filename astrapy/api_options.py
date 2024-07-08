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
    EmbeddingAPIKeyHeaderProvider,
    EmbeddingHeadersProvider,
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
        """
        Return a new instance created by completing this instance with a default
        API options object.

        In other words, `optA.with_default(optB)` will take fields from optA
        when possible and draw defaults from optB when optA has them set to anything
        evaluating to False. (This relies on the __bool__ definition of the values,
        such as that of the EmbeddingHeadersTokenProvider instances)

        Args:
            default: an API options instance to draw defaults from.

        Returns:
            a new instance of this class obtained by merging this one and the default.
        """
        if default:
            default_dict = default.__dict__
            return self.__class__(
                **{
                    k: self_v or default_dict.get(k)
                    for k, self_v in self.__dict__.items()
                }
            )
        else:
            return self

    def with_override(self: AO, override: Optional[BaseAPIOptions]) -> AO:
        """
        Return a new instance created by overriding the members of this instance
        with those taken from a supplied "override" API options object.

        In other words, `optA.with_default(optB)` will take fields from optB
        when possible and fall back to optA when optB has them set to anything
        evaluating to False. (This relies on the __bool__ definition of the values,
        such as that of the EmbeddingHeadersTokenProvider instances)

        Args:
            override: an API options instance to preferentially draw fields from.

        Returns:
            a new instance of this class obtained by merging the override and this one.
        """
        if override:
            self_dict = self.__dict__
            return self.__class__(
                **{
                    k: override_v or self_dict.get(k)
                    for k, override_v in override.__dict__.items()
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
            The default value is `EmbeddingAPIKeyHeaderProvider(None)`, i.e.
            no embedding-specific headers, whereas if the collection is configured
            with an embedding service other choices for this parameter can be
            meaningfully supplied. is configured for the collection,
    """

    embedding_api_key: EmbeddingHeadersProvider = field(
        default_factory=lambda: EmbeddingAPIKeyHeaderProvider(None)
    )
