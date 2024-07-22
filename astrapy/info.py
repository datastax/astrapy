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
from typing import Any, Dict, List, Optional


@dataclass
class DatabaseInfo:
    """
    Represents the identifying information for a database,
    including the region the connection is established to.

    Attributes:
        id: the database ID.
        region: the ID of the region through which the connection to DB is done.
        namespace: the namespace this DB is set to work with. None if not set.
        name: the database name. Not necessarily unique: there can be multiple
            databases with the same name.
        environment: a label, whose value can be `Environment.PROD`,
            or another value in `Environment.*`.
        raw_info: the full response from the DevOPS API call to get this info.

    Note:
        The `raw_info` dictionary usually has a `region` key describing
        the default region as configured in the database, which does not
        necessarily (for multi-region databases) match the region through
        which the connection is established: the latter is the one specified
        by the "api endpoint" used for connecting. In other words, for multi-region
        databases it is possible that
            database_info.region != database_info.raw_info["region"]
        Conversely, in case of a DatabaseInfo not obtained through a
        connected database, such as when calling `Admin.list_databases()`,
        all fields except `environment` (e.g. namespace, region, etc)
        are set as found on the DevOps API response directly.
    """

    id: str
    region: str
    namespace: Optional[str]
    name: str
    environment: str
    raw_info: Optional[Dict[str, Any]]


@dataclass
class AdminDatabaseInfo:
    """
    Represents the full response from the DevOps API about a database info.

    Most attributes just contain the corresponding part of the raw response:
    for this reason, please consult the DevOps API documentation for details.

    Attributes:
        info: a DatabaseInfo instance for the underlying database.
            The DatabaseInfo is a subset of the information described by
            AdminDatabaseInfo - in terms of the DevOps API response,
            it corresponds to just its "info" subdictionary.
        available_actions: the "availableActions" value in the full API response.
        cost: the "cost" value in the full API response.
        cqlsh_url: the "cqlshUrl" value in the full API response.
        creation_time: the "creationTime" value in the full API response.
        data_endpoint_url: the "dataEndpointUrl" value in the full API response.
        grafana_url: the "grafanaUrl" value in the full API response.
        graphql_url: the "graphqlUrl" value in the full API response.
        id: the "id" value in the full API response.
        last_usage_time: the "lastUsageTime" value in the full API response.
        metrics: the "metrics" value in the full API response.
        observed_status: the "observedStatus" value in the full API response.
        org_id: the "orgId" value in the full API response.
        owner_id: the "ownerId" value in the full API response.
        status: the "status" value in the full API response.
        storage: the "storage" value in the full API response.
        termination_time: the "terminationTime" value in the full API response.
        raw_info: the full raw response from the DevOps API.
    """

    info: DatabaseInfo
    available_actions: Optional[List[str]]
    cost: Dict[str, Any]
    cqlsh_url: str
    creation_time: str
    data_endpoint_url: str
    grafana_url: str
    graphql_url: str
    id: str
    last_usage_time: str
    metrics: Dict[str, Any]
    observed_status: str
    org_id: str
    owner_id: str
    status: str
    storage: Dict[str, Any]
    termination_time: str
    raw_info: Optional[Dict[str, Any]]


@dataclass
class CollectionInfo:
    """
    Represents the identifying information for a collection,
    including the information about the database the collection belongs to.

    Attributes:
        database_info: a DatabaseInfo instance for the underlying database.
        namespace: the namespace where the collection is located.
        name: collection name. Unique within a namespace.
        full_name: identifier for the collection within the database,
            in the form "namespace.collection_name".
    """

    database_info: DatabaseInfo
    namespace: str
    name: str
    full_name: str


@dataclass
class CollectionDefaultIDOptions:
    """
    The "defaultId" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        default_id_type: string such as `objectId`, `uuid6` and so on.
    """

    default_id_type: str

    def as_dict(self) -> Dict[str, Any]:
        """Recast this object into a dictionary."""

        return {"type": self.default_id_type}

    @staticmethod
    def from_dict(
        raw_dict: Optional[Dict[str, Any]]
    ) -> Optional[CollectionDefaultIDOptions]:
        """
        Create an instance of CollectionDefaultIDOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionDefaultIDOptions(default_id_type=raw_dict["type"])
        else:
            return None


@dataclass
class CollectionVectorServiceOptions:
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

    provider: Optional[str]
    model_name: Optional[str]
    authentication: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None

    def as_dict(self) -> Dict[str, Any]:
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
    def from_dict(
        raw_dict: Optional[Dict[str, Any]]
    ) -> Optional[CollectionVectorServiceOptions]:
        """
        Create an instance of CollectionVectorServiceOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionVectorServiceOptions(
                provider=raw_dict.get("provider"),
                model_name=raw_dict.get("modelName"),
                authentication=raw_dict.get("authentication"),
                parameters=raw_dict.get("parameters"),
            )
        else:
            return None


@dataclass
class CollectionVectorOptions:
    """
    The "vector" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        dimension: an optional positive integer, the dimensionality of the vector space.
        metric: an optional metric among `VectorMetric.DOT_PRODUCT`,
            `VectorMetric.EUCLIDEAN` and `VectorMetric.COSINE`.
        service: an optional CollectionVectorServiceOptions object in case a
            service is configured for the collection.
    """

    dimension: Optional[int]
    metric: Optional[str]
    service: Optional[CollectionVectorServiceOptions]

    def as_dict(self) -> Dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "dimension": self.dimension,
                "metric": self.metric,
                "service": None if self.service is None else self.service.as_dict(),
            }.items()
            if v is not None
        }

    @staticmethod
    def from_dict(
        raw_dict: Optional[Dict[str, Any]]
    ) -> Optional[CollectionVectorOptions]:
        """
        Create an instance of CollectionVectorOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionVectorOptions(
                dimension=raw_dict.get("dimension"),
                metric=raw_dict.get("metric"),
                service=CollectionVectorServiceOptions.from_dict(
                    raw_dict.get("service")
                ),
            )
        else:
            return None


@dataclass
class CollectionOptions:
    """
    A structure expressing the options of a collection.
    See the Data API specifications for detailed specification and allowed values.

    Attributes:
        vector: an optional CollectionVectorOptions object.
        indexing: an optional dictionary with the "indexing" collection properties.
        default_id: an optional CollectionDefaultIDOptions object.
        raw_options: the raw response from the Data API for the collection configuration.
    """

    vector: Optional[CollectionVectorOptions]
    indexing: Optional[Dict[str, Any]]
    default_id: Optional[CollectionDefaultIDOptions]
    raw_options: Optional[Dict[str, Any]]

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                None if self.vector is None else f"vector={self.vector.__repr__()}",
                (
                    None
                    if self.indexing is None
                    else f"indexing={self.indexing.__repr__()}"
                ),
                (
                    None
                    if self.default_id is None
                    else f"default_id={self.default_id.__repr__()}"
                ),
            ]
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(not_null_pieces)})"

    def as_dict(self) -> Dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "vector": None if self.vector is None else self.vector.as_dict(),
                "indexing": self.indexing,
                "defaultId": (
                    None if self.default_id is None else self.default_id.as_dict()
                ),
            }.items()
            if v is not None
        }

    def flatten(self) -> Dict[str, Any]:
        """
        Recast this object as a flat key-value pair suitable for
        use as kwargs in a create_collection method call (including recasts).
        """

        _dimension: Optional[int]
        _metric: Optional[str]
        _indexing: Optional[Dict[str, Any]]
        _service: Optional[Dict[str, Any]]
        _default_id_type: Optional[str]
        if self.vector is not None:
            _dimension = self.vector.dimension
            _metric = self.vector.metric
            if self.vector.service is None:
                _service = None
            else:
                _service = self.vector.service.as_dict()
        else:
            _dimension = None
            _metric = None
            _service = None
        _indexing = self.indexing
        if self.default_id is not None:
            _default_id_type = self.default_id.default_id_type
        else:
            _default_id_type = None

        return {
            k: v
            for k, v in {
                "dimension": _dimension,
                "metric": _metric,
                "service": _service,
                "indexing": _indexing,
                "default_id_type": _default_id_type,
            }.items()
            if v is not None
        }

    @staticmethod
    def from_dict(raw_dict: Dict[str, Any]) -> CollectionOptions:
        """
        Create an instance of CollectionOptions from a dictionary
        such as one from the Data API.
        """

        return CollectionOptions(
            vector=CollectionVectorOptions.from_dict(raw_dict.get("vector")),
            indexing=raw_dict.get("indexing"),
            default_id=CollectionDefaultIDOptions.from_dict(raw_dict.get("defaultId")),
            raw_options=raw_dict,
        )


@dataclass
class CollectionDescriptor:
    """
    A structure expressing full description of a collection as the Data API
    returns it, i.e. its name and its `options` sub-structure.

    Attributes:
        name: the name of the collection.
        options: a CollectionOptions instance.
        raw_descriptor: the raw response from the Data API.
    """

    name: str
    options: CollectionOptions
    raw_descriptor: Optional[Dict[str, Any]]

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name.__repr__()}, "
            f"options={self.options.__repr__()})"
        )

    def as_dict(self) -> Dict[str, Any]:
        """
        Recast this object into a dictionary.
        Empty `options` will not be returned at all.
        """

        return {
            k: v
            for k, v in {
                "name": self.name,
                "options": self.options.as_dict(),
            }.items()
            if v
        }

    def flatten(self) -> Dict[str, Any]:
        """
        Recast this object as a flat key-value pair suitable for
        use as kwargs in a create_collection method call (including recasts).
        """

        return {
            **(self.options.flatten()),
            **{"name": self.name},
        }

    @staticmethod
    def from_dict(raw_dict: Dict[str, Any]) -> CollectionDescriptor:
        """
        Create an instance of CollectionDescriptor from a dictionary
        such as one from the Data API.
        """

        return CollectionDescriptor(
            name=raw_dict["name"],
            options=CollectionOptions.from_dict(raw_dict.get("options") or {}),
            raw_descriptor=raw_dict,
        )


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
    display_name: Optional[str]
    help: Optional[str]
    hint: Optional[str]
    name: str
    required: bool
    parameter_type: str
    validation: Dict[str, Any]

    def __repr__(self) -> str:
        return f"EmbeddingProviderParameter(name='{self.name}')"

    def as_dict(self) -> Dict[str, Any]:
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
    def from_dict(raw_dict: Dict[str, Any]) -> EmbeddingProviderParameter:
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
            default_value=raw_dict["defaultValue"],
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
    parameters: List[EmbeddingProviderParameter]
    vector_dimension: Optional[int]

    def __repr__(self) -> str:
        return f"EmbeddingProviderModel(name='{self.name}')"

    def as_dict(self) -> Dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "name": self.name,
            "parameters": [parameter.as_dict() for parameter in self.parameters],
            "vectorDimension": self.vector_dimension,
        }

    @staticmethod
    def from_dict(raw_dict: Dict[str, Any]) -> EmbeddingProviderModel:
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
                EmbeddingProviderParameter.from_dict(param_dict)
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

    def as_dict(self) -> Dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "accepted": self.accepted,
            "forwarded": self.forwarded,
        }

    @staticmethod
    def from_dict(raw_dict: Dict[str, Any]) -> EmbeddingProviderToken:
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
    tokens: List[EmbeddingProviderToken]

    def __repr__(self) -> str:
        return (
            f"EmbeddingProviderAuthentication(enabled={self.enabled}, "
            f"tokens={','.join(str(token) for token in self.tokens)})"
        )

    def as_dict(self) -> Dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "enabled": self.enabled,
            "tokens": [token.as_dict() for token in self.tokens],
        }

    @staticmethod
    def from_dict(raw_dict: Dict[str, Any]) -> EmbeddingProviderAuthentication:
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
                EmbeddingProviderToken.from_dict(token_dict)
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

    display_name: Optional[str]
    models: List[EmbeddingProviderModel]
    parameters: List[EmbeddingProviderParameter]
    supported_authentication: Dict[str, EmbeddingProviderAuthentication]
    url: Optional[str]

    def as_dict(self) -> Dict[str, Any]:
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
    def from_dict(raw_dict: Dict[str, Any]) -> EmbeddingProvider:
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
                EmbeddingProviderModel.from_dict(model_dict)
                for model_dict in raw_dict["models"]
            ],
            parameters=[
                EmbeddingProviderParameter.from_dict(param_dict)
                for param_dict in raw_dict["parameters"]
            ],
            supported_authentication={
                sa_name: EmbeddingProviderAuthentication.from_dict(sa_dict)
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

    embedding_providers: Dict[str, EmbeddingProvider]
    raw_info: Optional[Dict[str, Any]]

    def as_dict(self) -> Dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "embeddingProviders": {
                ep_name: e_provider.as_dict()
                for ep_name, e_provider in self.embedding_providers.items()
            },
        }

    @staticmethod
    def from_dict(raw_dict: Dict[str, Any]) -> FindEmbeddingProvidersResult:
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
                ep_name: EmbeddingProvider.from_dict(ep_body)
                for ep_name, ep_body in raw_dict["embeddingProviders"].items()
            },
        )
