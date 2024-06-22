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
from typing import Any, Union


def coerce_token_provider(token: Any) -> TokenProvider:
    if isinstance(token, TokenProvider):
        return token
    else:
        return StaticTokenProvider(token)


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

    @abstractmethod
    def get_token(self) -> Union[str, None]:
        """
        Produce a string for direct use as token in a subsequent API request,
        or None for no token.
        """
        ...

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
