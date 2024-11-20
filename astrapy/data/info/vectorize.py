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

import warnings
from dataclasses import dataclass
from typing import Any


@dataclass
class VectorServiceOptions:
    """
    The "vector.service" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        provider: the name of a service provider for embedding calculation.
        model_name: the name of a specific model for use by the service.
        authentication: a key-value dictionary for the "authentication" specification,
            if any, in the vector service options.
        parameters: a key-value dictionary for the "parameters" specification, if any,
            in the vector service options.
    """

    provider: str | None
    model_name: str | None
    authentication: dict[str, Any] | None = None
    parameters: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "provider": self.provider,
                "modelName": self.model_name,
                "authentication": self.authentication,
                "parameters": self.parameters,
            }.items()
            if v is not None
        }

    @staticmethod
    def _from_dict(
        raw_dict: dict[str, Any] | None,
    ) -> VectorServiceOptions | None:
        """
        Create an instance of VectorServiceOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return VectorServiceOptions(
                provider=raw_dict.get("provider"),
                model_name=raw_dict.get("modelName"),
                authentication=raw_dict.get("authentication"),
                parameters=raw_dict.get("parameters"),
            )
        else:
            return None

    @staticmethod
    def coerce(
        raw_input: VectorServiceOptions | dict[str, Any] | None,
    ) -> VectorServiceOptions | None:
        if isinstance(raw_input, VectorServiceOptions):
            return raw_input
        else:
            return VectorServiceOptions._from_dict(raw_input)


@dataclass
class EmbeddingProviderParameter:
    """
    A representation of a parameter as returned by the 'findEmbeddingProviders'
    Data API endpoint.

    Attributes:
        default_value: the default value for the parameter.
        help: a textual description of the parameter.
        name: the name to use when passing the parameter for vectorize operations.
        required: whether the parameter is required or not.
        parameter_type: a textual description of the data type for the parameter.
        validation: a dictionary describing a parameter-specific validation policy.
    """

    default_value: Any
    display_name: str | None
    help: str | None
    hint: str | None
    name: str
    required: bool
    parameter_type: str
    validation: dict[str, Any]

    def __repr__(self) -> str:
        return f"EmbeddingProviderParameter(name='{self.name}')"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "defaultValue": self.default_value,
                "displayName": self.display_name,
                "help": self.help,
                "hint": self.hint,
                "name": self.name,
                "required": self.required,
                "type": self.parameter_type,
                "validation": self.validation,
            }.items()
            if v is not None
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any]) -> EmbeddingProviderParameter:
        """
        Create an instance of EmbeddingProviderParameter from a dictionary
        such as one from the Data API.
        """

        residual_keys = raw_dict.keys() - {
            "defaultValue",
            "displayName",
            "help",
            "hint",
            "name",
            "required",
            "type",
            "validation",
        }
        if residual_keys:
            warnings.warn(
                "Unexpected key(s) encountered parsing a dictionary into "
                f"an `EmbeddingProviderParameter`: '{','.join(sorted(residual_keys))}'"
            )
        return EmbeddingProviderParameter(
            default_value=raw_dict.get("defaultValue"),
            display_name=raw_dict.get("displayName"),
            help=raw_dict.get("help"),
            hint=raw_dict.get("hint"),
            name=raw_dict["name"],
            required=raw_dict["required"],
            parameter_type=raw_dict["type"],
            validation=raw_dict["validation"],
        )


@dataclass
class EmbeddingProviderModel:
    """
    A representation of an embedding model as returned by the 'findEmbeddingProviders'
    Data API endpoint.

    Attributes:
        name: the model name as must be passed when issuing
            vectorize operations to the API.
        parameters: a list of the `EmbeddingProviderParameter` objects the model admits.
        vector_dimension: an integer for the dimensionality of the embedding model.
            if this is None, the dimension can assume multiple values as specified
            by a corresponding parameter listed with the model.
    """

    name: str
    parameters: list[EmbeddingProviderParameter]
    vector_dimension: int | None

    def __repr__(self) -> str:
        return f"EmbeddingProviderModel(name='{self.name}')"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "name": self.name,
            "parameters": [parameter.as_dict() for parameter in self.parameters],
            "vectorDimension": self.vector_dimension,
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any]) -> EmbeddingProviderModel:
        """
        Create an instance of EmbeddingProviderModel from a dictionary
        such as one from the Data API.
        """

        residual_keys = raw_dict.keys() - {
            "name",
            "parameters",
            "vectorDimension",
        }
        if residual_keys:
            warnings.warn(
                "Unexpected key(s) encountered parsing a dictionary into "
                f"an `EmbeddingProviderModel`: '{','.join(sorted(residual_keys))}'"
            )
        return EmbeddingProviderModel(
            name=raw_dict["name"],
            parameters=[
                EmbeddingProviderParameter._from_dict(param_dict)
                for param_dict in raw_dict["parameters"]
            ],
            vector_dimension=raw_dict["vectorDimension"],
        )


@dataclass
class EmbeddingProviderToken:
    """
    A representation of a "token", that is a specific secret string, needed by
    an embedding model; this models a part of the response from the
    'findEmbeddingProviders' Data API endpoint.

    Attributes:
        accepted: the name of this "token" as seen by the Data API. This is the
            name that should be used in the clients when supplying the secret,
            whether as header or by shared-secret.
        forwarded: the name used by the API when issuing the embedding request
            to the embedding provider. This is of no direct interest for the Data API user.
    """

    accepted: str
    forwarded: str

    def __repr__(self) -> str:
        return f"EmbeddingProviderToken('{self.accepted}')"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "accepted": self.accepted,
            "forwarded": self.forwarded,
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any]) -> EmbeddingProviderToken:
        """
        Create an instance of EmbeddingProviderToken from a dictionary
        such as one from the Data API.
        """

        residual_keys = raw_dict.keys() - {
            "accepted",
            "forwarded",
        }
        if residual_keys:
            warnings.warn(
                "Unexpected key(s) encountered parsing a dictionary into "
                f"an `EmbeddingProviderToken`: '{','.join(sorted(residual_keys))}'"
            )
        return EmbeddingProviderToken(
            accepted=raw_dict["accepted"],
            forwarded=raw_dict["forwarded"],
        )


@dataclass
class EmbeddingProviderAuthentication:
    """
    A representation of an authentication mode for using an embedding model,
    modeling the corresponding part of the response returned by the
    'findEmbeddingProviders' Data API endpoint (namely "supportedAuthentication").

    Attributes:
        enabled: whether this authentication mode is available for a given model.
        tokens: a list of `EmbeddingProviderToken` objects,
            detailing the secrets required for the authentication mode.
    """

    enabled: bool
    tokens: list[EmbeddingProviderToken]

    def __repr__(self) -> str:
        return (
            f"EmbeddingProviderAuthentication(enabled={self.enabled}, "
            f"tokens={','.join(str(token) for token in self.tokens)})"
        )

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "enabled": self.enabled,
            "tokens": [token.as_dict() for token in self.tokens],
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any]) -> EmbeddingProviderAuthentication:
        """
        Create an instance of EmbeddingProviderAuthentication from a dictionary
        such as one from the Data API.
        """

        residual_keys = raw_dict.keys() - {
            "enabled",
            "tokens",
        }
        if residual_keys:
            warnings.warn(
                "Unexpected key(s) encountered parsing a dictionary into "
                f"an `EmbeddingProviderAuthentication`: '{','.join(sorted(residual_keys))}'"
            )
        return EmbeddingProviderAuthentication(
            enabled=raw_dict["enabled"],
            tokens=[
                EmbeddingProviderToken._from_dict(token_dict)
                for token_dict in raw_dict["tokens"]
            ],
        )


@dataclass
class EmbeddingProvider:
    """
    A representation of an embedding provider, as returned by the 'findEmbeddingProviders'
    Data API endpoint.

    Attributes:
        display_name: a version of the provider name for display and pretty printing.
            Not to be used when issuing vectorize API requests (for the latter, it is
            the key in the providers dictionary that is required).
        models: a list of `EmbeddingProviderModel` objects pertaining to the provider.
        parameters: a list of `EmbeddingProviderParameter` objects common to all models
            for this provider.
        supported_authentication: a dictionary of the authentication modes for
            this provider. Note that disabled modes may still appear in this map,
            albeit with the `enabled` property set to False.
        url: a string template for the URL used by the Data API when issuing the request
            toward the embedding provider. This is of no direct concern to the Data API user.
    """

    def __repr__(self) -> str:
        return f"EmbeddingProvider(display_name='{self.display_name}', models={self.models})"

    display_name: str | None
    models: list[EmbeddingProviderModel]
    parameters: list[EmbeddingProviderParameter]
    supported_authentication: dict[str, EmbeddingProviderAuthentication]
    url: str | None

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "displayName": self.display_name,
            "models": [model.as_dict() for model in self.models],
            "parameters": [parameter.as_dict() for parameter in self.parameters],
            "supportedAuthentication": {
                sa_name: sa_value.as_dict()
                for sa_name, sa_value in self.supported_authentication.items()
            },
            "url": self.url,
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any]) -> EmbeddingProvider:
        """
        Create an instance of EmbeddingProvider from a dictionary
        such as one from the Data API.
        """

        residual_keys = raw_dict.keys() - {
            "displayName",
            "models",
            "parameters",
            "supportedAuthentication",
            "url",
        }
        if residual_keys:
            warnings.warn(
                "Unexpected key(s) encountered parsing a dictionary into "
                f"an `EmbeddingProvider`: '{','.join(sorted(residual_keys))}'"
            )
        return EmbeddingProvider(
            display_name=raw_dict["displayName"],
            models=[
                EmbeddingProviderModel._from_dict(model_dict)
                for model_dict in raw_dict["models"]
            ],
            parameters=[
                EmbeddingProviderParameter._from_dict(param_dict)
                for param_dict in raw_dict["parameters"]
            ],
            supported_authentication={
                sa_name: EmbeddingProviderAuthentication._from_dict(sa_dict)
                for sa_name, sa_dict in raw_dict["supportedAuthentication"].items()
            },
            url=raw_dict["url"],
        )


@dataclass
class FindEmbeddingProvidersResult:
    """
    A representation of the whole response from the 'findEmbeddingProviders'
    Data API endpoint.

    Attributes:
        embedding_providers: a dictionary of provider names to EmbeddingProvider objects.
        raw_info: a (nested) dictionary containing the original full response from the endpoint.
    """

    def __repr__(self) -> str:
        return (
            "FindEmbeddingProvidersResult(embedding_providers="
            f"{', '.join(sorted(self.embedding_providers.keys()))})"
        )

    embedding_providers: dict[str, EmbeddingProvider]
    raw_info: dict[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "embeddingProviders": {
                ep_name: e_provider.as_dict()
                for ep_name, e_provider in self.embedding_providers.items()
            },
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any]) -> FindEmbeddingProvidersResult:
        """
        Create an instance of FindEmbeddingProvidersResult from a dictionary
        such as one from the Data API.
        """

        residual_keys = raw_dict.keys() - {
            "embeddingProviders",
        }
        if residual_keys:
            warnings.warn(
                "Unexpected key(s) encountered parsing a dictionary into "
                f"a `FindEmbeddingProvidersResult`: '{','.join(sorted(residual_keys))}'"
            )
        return FindEmbeddingProvidersResult(
            raw_info=raw_dict,
            embedding_providers={
                ep_name: EmbeddingProvider._from_dict(ep_body)
                for ep_name, ep_body in raw_dict["embeddingProviders"].items()
            },
        )
