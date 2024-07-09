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

import base64
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

EMBEDDING_HEADER_AWS_ACCESS_ID = "X-Embedding-Access-Id"
EMBEDDING_HEADER_AWS_SECRET_ID = "X-Embedding-Secret-Id"
EMBEDDING_HEADER_API_KEY = "X-Embedding-Api-Key"


def coerce_token_provider(token: Optional[Union[str, TokenProvider]]) -> TokenProvider:
    if isinstance(token, TokenProvider):
        return token
    else:
        return StaticTokenProvider(token)


def coerce_embedding_headers_provider(
    embedding_api_key: Optional[Union[str, EmbeddingHeadersProvider]],
) -> EmbeddingHeadersProvider:
    if isinstance(embedding_api_key, EmbeddingHeadersProvider):
        return embedding_api_key
    else:
        return EmbeddingAPIKeyHeaderProvider(embedding_api_key)


class TokenProvider(ABC):
    """
    Abstract base class for a token provider.
    The relevant method in this interface is returning a string to use as token.

    The __str__ / __repr__ methods are NOT to be used as source of tokens:
    use get_token instead.

    Note that equality (__eq__) checks if the generated tokens match
    under all circumstances (e.g. a literal passthrough matches a
    different-encoding token provider that yields the same token).
    If a token provider comes that encodes a recipe for nondeterministic
    periodic renewal, its __eq__ method will have to override the one in this class.
    """

    def __eq__(self, other: Any) -> bool:
        my_token = self.get_token()
        if isinstance(other, TokenProvider):
            if my_token is None:
                return other.get_token() is None
            else:
                return other.get_token() == my_token
        else:
            return False

    @abstractmethod
    def __repr__(self) -> str: ...

    def __or__(self, other: TokenProvider) -> TokenProvider:
        """
        Implement the logic as for "token_str_a or token_str_b" for the TokenProvider,
        with the None token being the 'falsey' case.
        This is useful for the inherit-with-optional-specialization-of-token
        pattern used in several spawn-and-inherit cases.
        """
        if self.get_token() is not None:
            return self
        else:
            return other

    def __bool__(self) -> bool:
        """
        All providers, unless their token is None, evaluate to True.
        This enables the "token_a or token_b" pattern used throughout,
        similarly as for the __or__ method.
        """
        return self.get_token() is not None

    @abstractmethod
    def get_token(self) -> Union[str, None]:
        """
        Produce a string for direct use as token in a subsequent API request,
        or None for no token.
        """
        ...


class StaticTokenProvider(TokenProvider):
    """
    A "pass-through" provider that wraps a supplied literal token.

    Args:
        token: an access token for subsequent use in the client.

    Example:
        >>> from astrapy import DataAPIClient
        >>> from astrapy.authentication import StaticTokenProvider
        >>> token_provider = StaticTokenProvider("AstraCS:xyz...")
        >>> database = DataAPIClient().get_database(
        ...     "https://01234567-...-eu-west1.apps.datastax.com",
        ...     token=token_provider,
        ... )
    """

    def __init__(self, token: Union[str, None]) -> None:
        self.token = token

    def __repr__(self) -> str:
        return self.token or "(none)"

    def get_token(self) -> Union[str, None]:
        return self.token


class UsernamePasswordTokenProvider(TokenProvider):
    """
    A token provider encoding username/password-based authentication,
    as used e.g. for DSE and HCD. These are base64-encoded and concatenated
    by colons, with a prepended suffix 'Cassandra', as required by
    this authentication scheme.

    Args:
        username: the username for accessing the database.
        password: the corresponding password.

    Example:
        >>> from astrapy import DataAPIClient
        >>> from astrapy.constants imort Environment
        >>> from astrapy.authentication import UsernamePasswordTokenProvider
        >>> token_provider = UsernamePasswordTokenProvider("username", "password")
        >>> database = DataAPIClient(environment=Environment.HCD).get_database(
        ...     "http://localhost:8181",
        ...     token=token_provider,
        ... )
    """

    PREFIX = "Cassandra"

    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self.token = (
            f"{self.PREFIX}:{self._b64(self.username)}:{self._b64(self.password)}"
        )

    def __repr__(self) -> str:
        return self.token

    @staticmethod
    def _b64(cleartext: str) -> str:
        return base64.b64encode(cleartext.encode()).decode()

    def get_token(self) -> str:
        return self.token


class EmbeddingHeadersProvider(ABC):
    """
    Abstract base class for a provider of embedding-related headers (such as API Keys).
    The relevant method in this interface is returning a dict to use as
    (part of the) headers in Data API requests for a collection.

    This class captures the fact that, depending on the embedding provider for
    the collection, there may be zero, one *or more* headers to be passed
    if relying on the HEADERS auth method for Vectorize.
    """

    def __eq__(self, other: Any) -> bool:
        my_headers = self.get_headers()
        if isinstance(other, EmbeddingHeadersProvider):
            return other.get_headers() == my_headers
        else:
            return False

    @abstractmethod
    def __repr__(self) -> str: ...

    def __bool__(self) -> bool:
        """
        All headers providers evaluate to True unless they yield the empty dict.
        This method enables the override mechanism in APIOptions.
        """
        return self.get_headers() != {}

    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """
        Produce a dictionary for use as (part of) the headers in HTTP requests
        to the Data API.
        """
        ...


class EmbeddingAPIKeyHeaderProvider(EmbeddingHeadersProvider):
    """
    A "pass-through" header provider representing the single-header
    (typically "X-Embedding-Api-Key") auth scheme, in use by most of the
    embedding models in Vectorize.

    Args:
        embedding_api_key: a string that will be the value for the header.
            If None is passed, this results in a no-headers provider (such
            as the one used for non-Vectorize collections).

    Example:
        >>> from astrapy import DataAPIClient
        >>> from astrapy.authentication import (
            CollectionVectorServiceOptions,
            EmbeddingAPIKeyHeaderProvider,
        )
        >>> my_emb_api_key = EmbeddingAPIKeyHeaderProvider("abc012...")
        >>> service_options = CollectionVectorServiceOptions(
        ...     provider="a-certain-provider",
        ...     model_name="some-embedding-model",
        ... )
        >>>
        >>> database = DataAPIClient().get_database(
        ...     "https://01234567-...-eu-west1.apps.datastax.com",
        ...     token="AstraCS:...",
        ... )
        >>> collection = database.create_collection(
        ...     "vectorize_collection",
        ...     service=service_options,
        ...     embedding_api_key=my_emb_api_key,
        ... )
        >>> # likewise:
        >>> collection_b = database.get_collection(
        ...     "vectorize_collection",
        ...     embedding_api_key=my_emb_api_key,
        ... )
    """

    def __init__(self, embedding_api_key: Optional[str]) -> None:
        self.embedding_api_key = embedding_api_key

    def __repr__(self) -> str:
        if self.embedding_api_key is None:
            return f"{self.__class__.__name__}(empty)"
        else:
            return f'{self.__class__.__name__}("{self.embedding_api_key[:5]}...")'

    def get_headers(self) -> Dict[str, str]:
        if self.embedding_api_key is not None:
            return {EMBEDDING_HEADER_API_KEY: self.embedding_api_key}
        else:
            return {}


class AWSEmbeddingHeadersProvider(EmbeddingHeadersProvider):
    """
    A header provider representing the two-header auth scheme in use
    by the Amazon Web Services (e.g. AWS Bedrock) when using header-based
    authentication.

    Args:
        embedding_access_id: value of the "Access ID" secret. This will become
            the value for the corresponding header.
        embedding_secret_id: value of the "Secret ID" secret. This will become
            the value for the corresponding header.

    Example:
        >>> from astrapy import DataAPIClient
        >>> from astrapy.authentication import (
            CollectionVectorServiceOptions,
            AWSEmbeddingHeadersProvider,
        )
        >>> my_aws_emb_api_key = AWSEmbeddingHeadersProvider(
            embedding_access_id="my-access-id-012...",
            embedding_secret_id="my-secret-id-abc...",
        )
        >>> service_options = CollectionVectorServiceOptions(
        ...     provider="bedrock",
        ...     model_name="some-aws-bedrock-model",
        ... )
        >>>
        >>> database = DataAPIClient().get_database(
        ...     "https://01234567-...-eu-west1.apps.datastax.com",
        ...     token="AstraCS:...",
        ... )
        >>> collection = database.create_collection(
        ...     "vectorize_aws_collection",
        ...     service=service_options,
        ...     embedding_api_key=my_aws_emb_api_key,
        ... )
        >>> # likewise:
        >>> collection_b = database.get_collection(
        ...     "vectorize_aws_collection",
        ...     embedding_api_key=my_aws_emb_api_key,
        ... )
    """

    def __init__(self, *, embedding_access_id: str, embedding_secret_id: str) -> None:
        self.embedding_access_id = embedding_access_id
        self.embedding_secret_id = embedding_secret_id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(embedding_access_id="
            f'"{self.embedding_access_id[:3]}...", '
            f'embedding_secret_id="{self.embedding_secret_id[:3]}...")'
        )

    def get_headers(self) -> Dict[str, str]:
        return {
            EMBEDDING_HEADER_AWS_ACCESS_ID: self.embedding_access_id,
            EMBEDDING_HEADER_AWS_SECRET_ID: self.embedding_secret_id,
        }
