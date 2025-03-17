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
from typing import Any

from astrapy.utils.parsing import _warn_residual_keys


@dataclass
class RerankServiceOptions:
    """
    The "rerank.service" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        provider: the name of a service provider for reranking.
        model_name: the name of a specific model for use by the service.
        authentication: a key-value dictionary for the "authentication" specification,
            if any, in the reranking service options.
        parameters: a key-value dictionary for the "parameters" specification, if any,
            in the reranking service options.
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
    ) -> RerankServiceOptions | None:
        """
        Create an instance of RerankServiceOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return RerankServiceOptions(
                provider=raw_dict.get("provider"),
                model_name=raw_dict.get("modelName"),
                authentication=raw_dict.get("authentication"),
                parameters=raw_dict.get("parameters"),
            )
        else:
            return None

    @staticmethod
    def coerce(
        raw_input: RerankServiceOptions | dict[str, Any] | None,
    ) -> RerankServiceOptions | None:
        if isinstance(raw_input, RerankServiceOptions):
            return raw_input
        else:
            return RerankServiceOptions._from_dict(raw_input)


### QUIQUI


@dataclass
class RerankingProviderParameter:
    """
    A representation of a parameter as returned by the 'findRerankingProviders'
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
        return f"RerankingProviderParameter(name='{self.name}')"

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

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> RerankingProviderParameter:
        """
        Create an instance of RerankingProviderParameter from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "defaultValue",
                "displayName",
                "help",
                "hint",
                "name",
                "required",
                "type",
                "validation",
            },
        )
        return RerankingProviderParameter(
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
class RerankingProviderModel:
    """
    A representation of a reranking model as returned by the 'findRerankingProviders'
    Data API endpoint.

    Attributes:
        name: the model name as must be passed when issuing
            vectorize operations to the API.
        parameters: a list of the `RerankingProviderParameter` objects the model admits.
        vector_dimension: an integer for the dimensionality of the reranking model.
            if this is None, the dimension can assume multiple values as specified
            by a corresponding parameter listed with the model.
    """

    name: str
    is_default: bool
    url: str | None
    properties: dict[str, Any] | None
    parameters: list[RerankingProviderParameter]

    def __repr__(self) -> str:
        _default_desc = "<Default> " if self.is_default else ""
        return f"RerankingProviderModel({_default_desc}name='{self.name}')"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return dict(
            [
                pair
                for pair in [
                    ("name", self.name),
                    ("isDefault", self.is_default),
                    ("url", self.url),
                    ("properties", self.properties),
                    (
                        "parameters",
                        [parameter.as_dict() for parameter in self.parameters],
                    )
                    if self.parameters
                    else None,
                ]
                if pair is not None
            ]
        )

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> RerankingProviderModel:
        """
        Create an instance of RerankingProviderModel from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "name",
                "isDefault",
                "url",
                "properties",
                "parameters",
            },
        )
        return RerankingProviderModel(
            name=raw_dict["name"],
            is_default=raw_dict["isDefault"],
            url=raw_dict.get("url"),
            properties=raw_dict["properties"],
            parameters=[
                RerankingProviderParameter._from_dict(param_dict)
                for param_dict in raw_dict.get("parameters") or []
            ],
        )


@dataclass
class RerankingProviderToken:
    """
    A representation of a "token", that is a specific secret string, needed by
    a reranking model; this models a part of the response from the
    'findRerankingProviders' Data API endpoint.

    Attributes:
        accepted: the name of this "token" as seen by the Data API. This is the
            name that should be used in the clients when supplying the secret,
            whether as header or by shared-secret.
        forwarded: the name used by the API when issuing the reranking request
            to the reranking provider. This is of no direct interest for the Data API user.
    """

    accepted: str
    forwarded: str

    def __repr__(self) -> str:
        return f"RerankingProviderToken('{self.accepted}')"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "accepted": self.accepted,
            "forwarded": self.forwarded,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> RerankingProviderToken:
        """
        Create an instance of RerankingProviderToken from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"accepted", "forwarded"})
        return RerankingProviderToken(
            accepted=raw_dict["accepted"],
            forwarded=raw_dict["forwarded"],
        )


@dataclass
class RerankingProviderAuthentication:
    """
    A representation of an authentication mode for using a reranking model,
    modeling the corresponding part of the response returned by the
    'findRerankingProviders' Data API endpoint (namely "supportedAuthentication").

    Attributes:
        enabled: whether this authentication mode is available for a given model.
        tokens: a list of `RerankingProviderToken` objects,
            detailing the secrets required for the authentication mode.
    """

    enabled: bool
    tokens: list[RerankingProviderToken]

    def __repr__(self) -> str:
        return (
            f"RerankingProviderAuthentication(enabled={self.enabled}, "
            f"tokens={','.join(str(token) for token in self.tokens)})"
        )

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "enabled": self.enabled,
            "tokens": [token.as_dict() for token in self.tokens],
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> RerankingProviderAuthentication:
        """
        Create an instance of RerankingProviderAuthentication from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"enabled", "tokens"})
        return RerankingProviderAuthentication(
            enabled=raw_dict["enabled"],
            tokens=[
                RerankingProviderToken._from_dict(token_dict)
                for token_dict in raw_dict["tokens"]
            ],
        )


@dataclass
class RerankingProvider:
    """
    A representation of a reranking provider, as returned by the 'findRerankingProviders'
    Data API endpoint.

    Attributes:
        display_name: a version of the provider name for display and pretty printing.
            Not to be used when issuing vectorize API requests (for the latter, it is
            the key in the providers dictionary that is required).
        models: a list of `RerankingProviderModel` objects pertaining to the provider.
        parameters: a list of `RerankingProviderParameter` objects common to all models
            for this provider.
        supported_authentication: a dictionary of the authentication modes for
            this provider. Note that disabled modes may still appear in this map,
            albeit with the `enabled` property set to False.
        url: a string template for the URL used by the Data API when issuing the request
            toward the reranking provider. This is of no direct concern to the Data API user.
    """

    is_default: bool
    display_name: str | None
    supported_authentication: dict[str, RerankingProviderAuthentication]
    models: list[RerankingProviderModel]
    parameters: list[RerankingProviderParameter]
    url: str | None

    def __repr__(self) -> str:
        _default_desc = "<Default> " if self.is_default else ""
        return (
            f"RerankingProvider({_default_desc}display_name='{self.display_name}', "
            f"models={self.models})"
        )

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return dict(
            [
                pair
                for pair in [
                    ("isDefault", self.is_default),
                    ("displayName", self.display_name),
                    ("models", [model.as_dict() for model in self.models]),
                    (
                        "supportedAuthentication",
                        {
                            sa_name: sa_value.as_dict()
                            for sa_name, sa_value in self.supported_authentication.items()
                        },
                    ),
                    (
                        "parameters",
                        [parameter.as_dict() for parameter in self.parameters],
                    )
                    if self.parameters
                    else None,
                    ("url", self.url) if self.url else None,
                ]
                if pair is not None
            ]
        )

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> RerankingProvider:
        """
        Create an instance of RerankingProvider from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "isDefault",
                "displayName",
                "models",
                "parameters",
                "supportedAuthentication",
                "url",
            },
        )
        return RerankingProvider(
            is_default=raw_dict["isDefault"],
            display_name=raw_dict["displayName"],
            models=[
                RerankingProviderModel._from_dict(model_dict)
                for model_dict in raw_dict["models"]
            ],
            parameters=[
                RerankingProviderParameter._from_dict(param_dict)
                for param_dict in raw_dict.get("parameters") or []
            ],
            supported_authentication={
                sa_name: RerankingProviderAuthentication._from_dict(sa_dict)
                for sa_name, sa_dict in raw_dict["supportedAuthentication"].items()
            },
            url=raw_dict.get("url"),
        )


@dataclass
class FindRerankingProvidersResult:
    """
    A representation of the whole response from the 'findRerankingProviders'
    Data API endpoint.

    Attributes:
        reranking_providers: a dictionary of provider names to RerankingProvider objects.
        raw_info: a (nested) dictionary containing the original full response from the endpoint.
    """

    def __repr__(self) -> str:
        return (
            "FindRerankingProvidersResult(reranking_providers="
            f"{', '.join(sorted(self.reranking_providers.keys()))})"
        )

    reranking_providers: dict[str, RerankingProvider]
    raw_info: dict[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "rerankingProviders": {
                rp_name: r_provider.as_dict()
                for rp_name, r_provider in self.reranking_providers.items()
            },
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> FindRerankingProvidersResult:
        """
        Create an instance of FindRerankingProvidersResult from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"rerankingProviders"})
        return FindRerankingProvidersResult(
            raw_info=raw_dict,
            reranking_providers={
                rp_name: RerankingProvider._from_dict(rp_body)
                for rp_name, rp_body in raw_dict["rerankingProviders"].items()
            },
        )
