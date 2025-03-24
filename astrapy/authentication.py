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
from typing import Any

from typing_extensions import override

from astrapy.settings.defaults import (
    EMBEDDING_HEADER_API_KEY,
    EMBEDDING_HEADER_AWS_ACCESS_ID,
    EMBEDDING_HEADER_AWS_SECRET_ID,
    FIXED_SECRET_PLACEHOLDER,
    RERANKING_HEADER_API_KEY,
    SECRETS_REDACT_CHAR,
    SECRETS_REDACT_ENDING,
    SECRETS_REDACT_ENDING_LENGTH,
)
from astrapy.utils.unset import _UNSET, UnsetType


def coerce_token_provider(
    token: str | TokenProvider,
) -> TokenProvider:
    if isinstance(token, TokenProvider):
        return token
    else:
        return StaticTokenProvider(token)


def coerce_possible_token_provider(
    token: str | TokenProvider | UnsetType,
) -> TokenProvider | UnsetType:
    if isinstance(token, UnsetType):
        return _UNSET
    else:
        return coerce_token_provider(token)


def coerce_embedding_headers_provider(
    embedding_api_key: str | EmbeddingHeadersProvider,
) -> EmbeddingHeadersProvider:
    if isinstance(embedding_api_key, EmbeddingHeadersProvider):
        return embedding_api_key
    else:
        return EmbeddingAPIKeyHeaderProvider(embedding_api_key)


def coerce_possible_embedding_headers_provider(
    embedding_api_key: str | EmbeddingHeadersProvider | UnsetType,
) -> EmbeddingHeadersProvider | UnsetType:
    if isinstance(embedding_api_key, UnsetType):
        return _UNSET
    else:
        return coerce_embedding_headers_provider(embedding_api_key)


def coerce_reranking_headers_provider(
    reranking_api_key: str | RerankingHeadersProvider,
) -> RerankingHeadersProvider:
    if isinstance(reranking_api_key, RerankingHeadersProvider):
        return reranking_api_key
    else:
        return RerankingAPIKeyHeaderProvider(reranking_api_key)


def coerce_possible_reranking_headers_provider(
    reranking_api_key: str | RerankingHeadersProvider | UnsetType,
) -> RerankingHeadersProvider | UnsetType:
    if isinstance(reranking_api_key, UnsetType):
        return _UNSET
    else:
        return coerce_reranking_headers_provider(reranking_api_key)


def _redact_secret(secret: str, max_length: int, hide_if_short: bool = True) -> str:
    """
    Return a shortened-if-necessary version of a 'secret' string (with ellipsis).

    Args:
        secret: a secret string to redact
        max_length: if the secret and the fixed ending exceed this size,
            shortening takes place.
        hide_if_short: this controls what to do when the input secret is
            shorter, i.e. when no shortening takes place.
            if False, the secret is returned as-is;
            If True, a masked string is returned of the same length as secret.

    Returns:
        a 'redacted' form of the secret string as per the rules outlined above.
    """
    secret_len = len(secret)
    if secret_len + SECRETS_REDACT_ENDING_LENGTH > max_length:
        return (
            secret[: max_length - SECRETS_REDACT_ENDING_LENGTH] + SECRETS_REDACT_ENDING
        )
    else:
        if hide_if_short:
            return SECRETS_REDACT_CHAR * len(secret)
        else:
            return secret


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
    def get_token(self) -> str | None:
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

    def __init__(self, token: str | None) -> None:
        self.token = token

    @override
    def __repr__(self) -> str:
        if self.token is None:
            return "(none)"
        else:
            return f"{self.__class__.__name__}({_redact_secret(self.token, 15)})"

    @override
    def get_token(self) -> str | None:
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
        >>> from astrapy.constants import Environment
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

    @override
    def __repr__(self) -> str:
        _r_username = _redact_secret(self.username, 6)
        _r_password = FIXED_SECRET_PLACEHOLDER
        return f'{self.__class__.__name__}("username={_r_username}, password={_r_password}")'

    @staticmethod
    def _b64(cleartext: str) -> str:
        return base64.b64encode(cleartext.encode()).decode()

    @override
    def get_token(self) -> str:
        return self.token


class HeadersProvider(ABC):
    """
    Abstract base class for a generic "headers provider", which supplies authentication
    for some service (embeddings, reranking, ...) in the form of a dictionary of
    (zero,) one or more HTTP headers (name, value).

    The relevant method in this interface is returning a dict to use as
    (part of the) headers in Data API requests for a collection.
    """

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, HeadersProvider):
            my_headers = self.get_headers()
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
    def get_headers(self) -> dict[str, str]:
        """
        Produce a dictionary for use as (part of) the headers in HTTP requests
        to the Data API.
        """
        ...


class EmbeddingHeadersProvider(HeadersProvider):
    """
    Abstract class for a provider of embedding-related headers (such as API Keys).

    This class captures the fact that, depending on the embedding provider for
    the collection, there may be zero, one *or more* headers to be passed
    if relying on the HEADERS auth method for Vectorize.
    """

    pass


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
        >>> from astrapy.authentication import EmbeddingAPIKeyHeaderProvider
        >>> from astrapy.info import CollectionDefinition, VectorServiceOptions
        >>> my_emb_api_key = EmbeddingAPIKeyHeaderProvider("abc012...")
        >>> service_options = VectorServiceOptions(
        ...     provider="a-certain-provider",
        ...     model_name="some-embedding-model",
        ... )
        >>>
        >>> database = DataAPIClient().get_database(
        ...     "https://01234567-...-eu-west1.apps.datastax.com",
        ...     token="AstraCS:...",
        ... )
        >>> collection = database.create_collection(
        ...     "vectorize_aws_collection",
        ...     definition=(
        ...         CollectionDefinition.builder()
        ...         .set_vector_service(service_options)
        ...         .build()
        ...     ),
        ...     embedding_api_key=my_emb_api_key,
        ... )
        >>> # likewise:
        >>> collection_b = database.get_collection(
        ...     "vectorize_collection",
        ...     embedding_api_key=my_emb_api_key,
        ... )
    """

    def __init__(self, embedding_api_key: str | None) -> None:
        self.embedding_api_key = embedding_api_key

    @override
    def __repr__(self) -> str:
        if self.embedding_api_key is None:
            return f"{self.__class__.__name__}(empty)"
        else:
            return (
                f"{self.__class__.__name__}"
                f'("{_redact_secret(self.embedding_api_key, 8)}")'
            )

    @override
    def get_headers(self) -> dict[str, str]:
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
        >>> from astrapy.authentication import AWSEmbeddingHeadersProvider
        >>> from astrapy.info import CollectionDefinition, VectorServiceOptions
        >>> my_aws_emb_api_key = AWSEmbeddingHeadersProvider(
            embedding_access_id="my-access-id-012...",
            embedding_secret_id="my-secret-id-abc...",
        )
        >>> service_options = VectorServiceOptions(
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
        ...     definition=(
        ...         CollectionDefinition.builder()
        ...         .set_vector_service(service_options)
        ...         .build()
        ...     ),
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

    @override
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(embedding_access_id="
            f'"{_redact_secret(self.embedding_access_id, 6)}", '
            f'embedding_secret_id="{_redact_secret(self.embedding_secret_id, 6)}")'
        )

    @override
    def get_headers(self) -> dict[str, str]:
        return {
            EMBEDDING_HEADER_AWS_ACCESS_ID: self.embedding_access_id,
            EMBEDDING_HEADER_AWS_SECRET_ID: self.embedding_secret_id,
        }


class RerankingHeadersProvider(HeadersProvider):
    """
    Abstract class for a provider of reranking-related authentication header(s).

    This class captures the fact that, depending on the reranker provider for
    the collection, there may be zero, one *or more* headers to be passed
    if relying on the HEADERS auth method for Vectorize.
    """

    pass


class RerankingAPIKeyHeaderProvider(RerankingHeadersProvider):
    """
    A "pass-through" header provider representing the single-header
    (typically "Reranking-Api-Key") auth scheme, for use with the (single-header)
    reranking authentication scheme.

    Args:
        reranking_api_key: a string that will be the value for the header.
            If None is passed, this results in a no-headers provider (amounting
            to no reranking authentication being set through request headers).

    Example:
        >>> from astrapy import DataAPIClient
        >>> from astrapy.authentication import RerankingAPIKeyHeaderProvider
        >>> from astrapy.info import CollectionDefinition, CollectionRerankOptions, RerankServiceOptions
        >>> my_rrk_api_key = RerankingAPIKeyHeaderProvider("xyz987...")
        >>> service_options = RerankServiceOptions(
        ...     provider="a-certain-provider",
        ...     model_name="some-reranking-model",
        ... )
        >>> database = DataAPIClient().get_database(
        ...     "https://01234567-...-eu-west1.apps.datastax.com",
        ...     token="AstraCS:...",
        ... )
        >>> collection = database.create_collection(
        ...     "my_reranking_collection",
        ...     definition=(
        ...         CollectionDefinition.builder()
        ...         .set_rerank(CollectionRerankOptions(service=service_options))
        ...         .build()
        ...     ),
        ...     reranking_api_key=my_rrk_api_key,
        ... )
        >>> # likewise:
        >>> collection_b = database.get_collection(
        ...     "my_reranking_collection",
        ...     reranking_api_key=my_rrk_api_key,
        ... )
    """

    def __init__(self, reranking_api_key: str | None) -> None:
        self.reranking_api_key = reranking_api_key

    @override
    def __repr__(self) -> str:
        if self.reranking_api_key is None:
            return f"{self.__class__.__name__}(empty)"
        else:
            return (
                f"{self.__class__.__name__}"
                f'("{_redact_secret(self.reranking_api_key, 6)}")'
            )

    @override
    def get_headers(self) -> dict[str, str]:
        if self.reranking_api_key is not None:
            return {RERANKING_HEADER_API_KEY: self.reranking_api_key}
        else:
            return {}
