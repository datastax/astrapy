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

from astrapy.data.info.database_info import AstraDBDatabaseInfo
from astrapy.data.info.reranking import RerankServiceOptions
from astrapy.data.info.vectorize import VectorServiceOptions
from astrapy.utils.parsing import _warn_residual_keys

INDEXING_ALLOWED_MODES = {"allow", "deny"}


@dataclass
class CollectionInfo:
    """
    Represents the identifying information for a collection,
    including the information about the database the collection belongs to.

    Attributes:
        database_info: an AstraDBDatabaseInfo instance for the underlying database.
        keyspace: the keyspace where the collection is located.
        name: collection name. Unique within a keyspace (across tables/collections).
        full_name: identifier for the collection within the database,
            in the form "keyspace.collection_name".
    """

    database_info: AstraDBDatabaseInfo
    keyspace: str
    name: str
    full_name: str


@dataclass
class CollectionDefaultIDOptions:
    """
    The "defaultId" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        default_id_type: this setting determines what type of IDs the Data API will
            generate when inserting documents that do not specify their
            `_id` field explicitly. Can be set to any of the values
            `DefaultIdType.UUID`, `DefaultIdType.OBJECTID`,
            `DefaultIdType.UUIDV6`, `DefaultIdType.UUIDV7`,
            `DefaultIdType.DEFAULT`.
    """

    default_id_type: str

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {"type": self.default_id_type}

    @staticmethod
    def _from_dict(
        raw_dict: dict[str, Any] | None,
    ) -> CollectionDefaultIDOptions | None:
        """
        Create an instance of CollectionDefaultIDOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionDefaultIDOptions(default_id_type=raw_dict["type"])
        else:
            return None


@dataclass
class CollectionVectorOptions:
    """
    The "vector" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        dimension: an optional positive integer, the dimensionality
            of the vector space (i.e. the number of components in each vector).
        metric: an optional choice of similarity metric to use in vector search.
            It must be a (string) value among `VectorMetric.DOT_PRODUCT`,
            `VectorMetric.EUCLIDEAN` and `VectorMetric.COSINE`.
        source_model: based on this value, the vector index can tune itself so as
            to achieve optimal performance for a given embedding model. See the
            Data API documentation for the allowed values. Defaults to "other".
        service: an optional VectorServiceOptions object in case a vectorize
            service is configured to achieve server-side embedding computation
            on the collection.
    """

    dimension: int | None = None
    metric: str | None = None
    source_model: str | None = None
    service: VectorServiceOptions | None = None

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "dimension": self.dimension,
                "metric": self.metric,
                "service": None if self.service is None else self.service.as_dict(),
                "sourceModel": None if self.source_model is None else self.source_model,
            }.items()
            if v is not None
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any] | None) -> CollectionVectorOptions | None:
        """
        Create an instance of CollectionVectorOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionVectorOptions(
                dimension=raw_dict.get("dimension"),
                metric=raw_dict.get("metric"),
                source_model=raw_dict.get("sourceModel"),
                service=VectorServiceOptions._from_dict(raw_dict.get("service")),
            )
        else:
            return None


@dataclass
class CollectionLexicalOptions:
    """
    The "lexical" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        enabled: use this flag to programmatically set 'lexical' to on/off.
        analyzer: either a string (e.g. "standard") or a full dictionary
            specifying a more customized configuration for the text analyzer.
            See the Data API documentation for more on the dictionary form.
    """

    enabled: bool
    analyzer: str | dict[str, Any] | None

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        analyzer: str | dict[str, Any] | None = None,
    ) -> None:
        self.enabled = True if enabled is None else enabled
        self.analyzer = analyzer

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "enabled": self.enabled,
                "analyzer": self.analyzer,
            }.items()
            if v is not None
        }

    @staticmethod
    def _from_dict(raw_dict: dict[str, Any] | None) -> CollectionLexicalOptions | None:
        """
        Create an instance of CollectionLexicalOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionLexicalOptions(
                enabled=raw_dict.get("enabled"),
                analyzer=raw_dict.get("analyzer"),
            )
        else:
            return None


@dataclass
class CollectionRerankOptions:
    """
    The "rerank" component of the collection options.
    See the Data API specifications for allowed values.

    Attributes:
        enabled: use this flag to programmatically set 'rerank' to on/off.
        service: A `RerankServiceOptions` object describing the desired reranker.
    """

    enabled: bool
    service: RerankServiceOptions | None

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        service: RerankServiceOptions | None = None,
    ) -> None:
        self.enabled = True if enabled is None else enabled
        self.service = service

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "enabled": self.enabled,
                "service": None if self.service is None else self.service.as_dict(),
            }.items()
            if v is not None
        }

    @staticmethod
    def _from_dict(
        raw_dict: dict[str, Any] | None,
    ) -> CollectionRerankOptions | None:
        """
        Create an instance of CollectionRerankOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionRerankOptions(
                enabled=raw_dict.get("enabled"),
                service=RerankServiceOptions._from_dict(raw_dict.get("service")),
            )
        else:
            return None


@dataclass
class CollectionDefinition:
    """
    A structure expressing the options of a collection.
    See the Data API specifications for detailed specification and allowed values.

    Instances of this object can be created in three ways: using a fluent interface,
    passing a fully-formed definition to the class constructor, or coercing an
    appropriately-shaped plain dictionary into this class.
    See the examples below and the Table documentation for more details.

    Attributes:
        vector: an optional CollectionVectorOptions object.
        lexical: A `CollectionLexicalOptions` object encoding the desired
            "lexical" settings. If omitted, the Data API defaults apply.
        rerank: A `CollectionRerankOptions` object encoding the desired
            "rerank" settings. If omitted, the Data API defaults apply.
        indexing: an optional dictionary with the "indexing" collection properties.
            This is in the form of a dictionary such as `{"deny": [...]}`
            or `{"allow": [...]}`, with a list of document paths, or alternatively
            just `["*"]`, to exclude from/include in collection indexing,
            respectively.
        default_id: an optional CollectionDefaultIDOptions object (see).

    Example:
        >>> from astrapy.constants import VectorMetric
        >>> from astrapy.info import CollectionDefinition, CollectionVectorOptions
        >>>
        >>> # Create a collection definition with the fluent interface:
        >>> collection_definition = (
        ...     CollectionDefinition.builder()
        ...     .set_vector_dimension(3)
        ...     .set_vector_metric(VectorMetric.DOT_PRODUCT)
        ...     .set_indexing("deny", ["annotations", "logs"])
        ...     .build()
        ... )
        >>>
        >>> # Create a collection definition passing everything to the constructor:
        >>> collection_definition_1 = CollectionDefinition(
        ...     vector=CollectionVectorOptions(
        ...         dimension=3,
        ...         metric=VectorMetric.DOT_PRODUCT,
        ...     ),
        ...     indexing={"deny": ["annotations", "logs"]},
        ... )
        >>>
        >>> # Coerce a dictionary into a collection definition:
        >>> collection_definition_2_dict = {
        ...     "indexing": {"deny": ["annotations", "logs"]},
        ...     "vector": {
        ...         "dimension": 3,
        ...         "metric": VectorMetric.DOT_PRODUCT,
        ...     },
        ... }
        >>> collection_definition_2 = CollectionDefinition.coerce(
        ...     collection_definition_2_dict
        ... )
        >>>
        >>> # The three created objects are exactly identical:
        >>> collection_definition_2 == collection_definition_1
        True
        >>> collection_definition_2 == collection_definition
        True
    """

    vector: CollectionVectorOptions | None = None
    lexical: CollectionLexicalOptions | None = None
    rerank: CollectionRerankOptions | None = None
    indexing: dict[str, Any] | None = None
    default_id: CollectionDefaultIDOptions | None = None

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                None if self.vector is None else f"vector={self.vector.__repr__()}",
                None if self.lexical is None else f"lexical={self.lexical.__repr__()}",
                None if self.rerank is None else f"rerank={self.rerank.__repr__()}",
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

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "vector": None if self.vector is None else self.vector.as_dict(),
                "lexical": None if self.lexical is None else self.lexical.as_dict(),
                "rerank": None if self.rerank is None else self.rerank.as_dict(),
                "indexing": self.indexing,
                "defaultId": (
                    None if self.default_id is None else self.default_id.as_dict()
                ),
            }.items()
            if v is not None
            if v != {}
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> CollectionDefinition:
        """
        Create an instance of CollectionDefinition from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls, raw_dict, {"vector", "lexical", "rerank", "indexing", "defaultId"}
        )
        return CollectionDefinition(
            vector=CollectionVectorOptions._from_dict(raw_dict.get("vector")),
            lexical=CollectionLexicalOptions._from_dict(raw_dict.get("lexical")),
            rerank=CollectionRerankOptions._from_dict(raw_dict.get("rerank")),
            indexing=raw_dict.get("indexing"),
            default_id=CollectionDefaultIDOptions._from_dict(raw_dict.get("defaultId")),
        )

    @classmethod
    def coerce(
        cls, raw_input: CollectionDefinition | dict[str, Any]
    ) -> CollectionDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a CollectionDefinition.
        """

        if isinstance(raw_input, CollectionDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @staticmethod
    def builder() -> CollectionDefinition:
        """
        Create an "empty" builder for constructing a collection definition through
        a fluent interface. The resulting object has no defined properties,
        traits that can be added progressively with the corresponding methods.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a CollectionDefinition for the simplest possible creatable collection.
        """

        return CollectionDefinition()

    def set_indexing(
        self, indexing_mode: str | None, indexing_target: list[str] | None = None
    ) -> CollectionDefinition:
        """
        Return a new collection definition object with a new indexing setting.
        The indexing can be set to something (fully overwriting any pre-existing
        configuration), or removed entirely. This method is for use within the
        fluent interface for progressively building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            indexing_mode: one of "allow" or "deny" to configure indexing, or None
                in case one wants to remove the setting.
            indexing_target: a list of the document paths covered by the allow/deny
                prescription. Passing this parameter when `indexing_mode` is None
                results in an error.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            indexing setting to this collection definition.
        """

        if indexing_mode is None:
            if indexing_target is not None:
                raise ValueError("Cannot pass an indexing target if unsetting indexing")
            return CollectionDefinition(
                vector=self.vector,
                lexical=self.lexical,
                rerank=self.rerank,
                indexing=None,
                default_id=self.default_id,
            )
        _i_mode = indexing_mode.lower()
        if _i_mode not in INDEXING_ALLOWED_MODES:
            msg = (
                f"Unknown indexing mode: '{indexing_mode}'. "
                f"Allowed values are: {', '.join(INDEXING_ALLOWED_MODES)}."
            )
            raise ValueError(msg)
        _i_target: list[str] = indexing_target or []
        return CollectionDefinition(
            vector=self.vector,
            lexical=self.lexical,
            rerank=self.rerank,
            indexing={indexing_mode: indexing_target},
            default_id=self.default_id,
        )

    def set_default_id(self, default_id_type: str | None) -> CollectionDefinition:
        """
        Return a new collection definition object with a new setting for the
        collection 'default ID type'. This method is for use within the
        fluent interface for progressively building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            default_id_type: one of the values of `astrapy.constants.DefaultIdType`
                (or the equivalent string) to set a default ID type for a collection;
                alternatively, None to remove the corresponding configuration.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            default ID type setting to this collection definition.
        """

        if default_id_type is None:
            return CollectionDefinition(
                vector=self.vector,
                lexical=self.lexical,
                rerank=self.rerank,
                indexing=self.indexing,
                default_id=None,
            )

        return CollectionDefinition(
            vector=self.vector,
            lexical=self.lexical,
            rerank=self.rerank,
            indexing=self.indexing,
            default_id=CollectionDefaultIDOptions(
                default_id_type=default_id_type,
            ),
        )

    def set_vector_dimension(self, dimension: int | None) -> CollectionDefinition:
        """
        Return a new collection definition object with a new setting for the
        collection's vector dimension. This method is for use within the
        fluent interface for progressively building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            dimension: an integer, the number of components of vectors in the
                collection. Setting even just one vector-related property makes
                the described collection a "vector collection".
                Providing None removes this setting.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            vector-related setting to this collection definition.
        """

        _vector_options = self.vector or CollectionVectorOptions()
        return CollectionDefinition(
            vector=CollectionVectorOptions(
                dimension=dimension,
                metric=_vector_options.metric,
                source_model=_vector_options.source_model,
                service=_vector_options.service,
            ),
            lexical=self.lexical,
            rerank=self.rerank,
            indexing=self.indexing,
            default_id=self.default_id,
        )

    def set_vector_metric(self, metric: str | None) -> CollectionDefinition:
        """
        Return a new collection definition object with a new setting for the
        collection's vector similarity metric. This method is for use within the
        fluent interface for progressively building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            metric: a value of those in `astrapy.constants.VectorMetric`, or an
                equivalent string such as "dot_product", used for vector search
                within the collection. Setting even just one vector-related property
                makes the described collection a "vector collection".
                Providing None removes this setting.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            vector-related setting to this collection definition.
        """

        _vector_options = self.vector or CollectionVectorOptions()
        return CollectionDefinition(
            vector=CollectionVectorOptions(
                dimension=_vector_options.dimension,
                metric=metric,
                source_model=_vector_options.source_model,
                service=_vector_options.service,
            ),
            lexical=self.lexical,
            rerank=self.rerank,
            indexing=self.indexing,
            default_id=self.default_id,
        )

    def set_vector_source_model(self, source_model: str | None) -> CollectionDefinition:
        """
        Return a new collection definition object with a new setting for the
        collection's vector 'source model' parameter. This method is for use within the
        fluent interface for progressively building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            source_model: an optional string setting for the vector index, to help
                it pick the set of parameters best suited to a specific embedding model.
                See the Data API documentation for more details.
                Setting even just one vector-related property makes the described
                collection a "vector collection". Providing None
                removes this setting - the Data API will use its defaults.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            vector-related setting to this collection definition.
        """

        _vector_options = self.vector or CollectionVectorOptions()
        return CollectionDefinition(
            vector=CollectionVectorOptions(
                dimension=_vector_options.dimension,
                metric=_vector_options.metric,
                source_model=source_model,
                service=_vector_options.service,
            ),
            lexical=self.lexical,
            rerank=self.rerank,
            indexing=self.indexing,
            default_id=self.default_id,
        )

    def set_vector_service(
        self,
        provider: str | VectorServiceOptions | None,
        model_name: str | None = None,
        *,
        authentication: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> CollectionDefinition:
        """
        Return a new collection definition object with a new setting for the
        collection's vectorize (i.e. server-side embeddings) service.
        This method is for use within the fluent interface for progressively
        building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            provider: this can be (1) a whole `VectorServiceOptions` object encoding
                all desired properties for a vectorize service; or (2) it can be None,
                to signify removal of the entire vectorize setting; alternatively,
                (3) it can be a string, the vectorize provider name as seen in the
                response from the database's `find_embedding_providers` method. In the
                latter case, the other parameters should also be provided as needed.
                See the examples below for an illustration of these usage patterns.
            model_name: a string, the name of the vectorize model to use (must be
                compatible with the chosen provider).
            authentication: a dictionary with the required authentication information
                if the vectorize makes use of secrets (API Keys) stored in the database
                Key Management System. See the Data API for more information on
                storing an API Key secret in one's Astra DB account.
            parameters: a free-form key-value mapping providing additional,
                model-dependent configuration settings. The allowed parameters for
                a given model are specified in the response of the Database
                `find_embedding_providers` method.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            vector-related setting to this collection definition.

        Example:
            >>> from astrapy.info import CollectionDefinition, VectorServiceOptions
            >>>
            >>> zero = CollectionDefinition.builder()
            >>>
            >>> svc1 = zero.set_vector_service(
            ...     "myProvider",
            ...     "myModelName",
            ...     parameters={"p": "z"},
            ... )
            >>> print(svc1.build().as_dict())
            {'vector': {'service': {'provider': 'myProvider', 'modelName': 'myModelName', 'parameters': {'p': 'z'}}}}
            >>>
            >>> myVecSvcOpt = VectorServiceOptions(
            ...     provider="myProvider",
            ...     model_name="myModelName",
            ...     parameters={"p": "z"},
            ... )
            >>> svc2 = zero.set_vector_service(myVecSvcOpt).build()
            >>> print(svc2.as_dict())
            {'vector': {'service': {'provider': 'myProvider', 'modelName': 'myModelName', 'parameters': {'p': 'z'}}}}
            >>>
            >>> reset = svc1.set_vector_service(None).build()
            >>> print(reset.as_dict())
            {}
        """

        _vector_options = self.vector or CollectionVectorOptions()
        if isinstance(provider, VectorServiceOptions):
            if (
                model_name is not None
                or authentication is not None
                or parameters is not None
            ):
                msg = (
                    "Parameters 'model_name', 'authentication' and 'parameters' "
                    "cannot be passed when setting a VectorServiceOptions directly."
                )
                raise ValueError(msg)
            return CollectionDefinition(
                vector=CollectionVectorOptions(
                    dimension=_vector_options.dimension,
                    metric=_vector_options.metric,
                    source_model=_vector_options.source_model,
                    service=provider,
                ),
                lexical=self.lexical,
                rerank=self.rerank,
                indexing=self.indexing,
                default_id=self.default_id,
            )
        else:
            new_service: VectorServiceOptions | None
            if provider is None:
                if (
                    model_name is not None
                    or authentication is not None
                    or parameters is not None
                ):
                    msg = (
                        "Parameters 'model_name', 'authentication' and 'parameters' "
                        "cannot be passed when unsetting the vector service."
                    )
                    raise ValueError(msg)
                new_service = None
            else:
                new_service = VectorServiceOptions(
                    provider=provider,
                    model_name=model_name,
                    authentication=authentication,
                    parameters=parameters,
                )
            return CollectionDefinition(
                vector=CollectionVectorOptions(
                    dimension=_vector_options.dimension,
                    metric=_vector_options.metric,
                    source_model=_vector_options.source_model,
                    service=new_service,
                ),
                lexical=self.lexical,
                rerank=self.rerank,
                indexing=self.indexing,
                default_id=self.default_id,
            )

    def set_rerank(
        self,
        provider: str | CollectionRerankOptions | RerankServiceOptions | None,
        model_name: str | None = None,
        *,
        authentication: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
        enabled: bool | None = None,
    ) -> CollectionDefinition:
        """
        Return a new collection definition object with a new setting for the
        collection's rerank service.
        This method is for use within the fluent interface for progressively
        building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            provider: this can be (1) a `RerankServiceOptions` object encoding
                all desired properties for a reranking service;
                (2) a `CollectionRerankOptions`, that is likewise being set
                as the collection reranking configuration; or (3) it can be None,
                to signify removal of the entire rerank setting,
                leaving the API to its defaults; alternatively,
                (4) it can be a string, the reranking provider name as seen in the
                response from the database's `find_rerank_providers` method. In the
                latter case, the other parameters should also be provided as needed.
                See the examples below for an illustration of these usage patterns.
            model_name: a string, the name of the reranker model to use (must be
                compatible with the chosen provider).
            authentication: a dictionary with the required authentication information
                if the reranking makes use of secrets (API Keys) stored in the database
                Key Management System. See the Data API for more information on
                storing an API Key secret in one's Astra DB account.
            parameters: a free-form key-value mapping providing additional,
                model-dependent configuration settings. The allowed parameters for
                a given model are specified in the response of the Database
                `find_rerank_providers` method.
            enabled: if passed, this flag is used in the reranking definition
                for the collection. If omitted, defaults to True.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            reranking-related setting to this collection definition.

        Example:
            >> from astrapy.info import CollectionDefinition, RerankServiceOptions
            >>> from astrapy.data.info.collection_descriptor import CollectionRerankOptions
            >>>
            >>> zero = CollectionDefinition.builder()
            >>>
            >>> svc1 = zero.set_rerank(
            ...     "myProvider",
            ...     "myModelName",
            ...     parameters={"p": "z"},
            ... )
            >>> print(svc1.build().as_dict())
            {'rerank': {'enabled': True, 'service': {'provider': 'myProvider', 'modelName': 'myModelName', 'parameters': {'p': 'z'}}}}
            >>>
            >>> myRrkSvcOpt = RerankServiceOptions(
            ...     provider="myProvider",
            ...     model_name="myModelName",
            ...     parameters={"p": "z"},
            ... )
            >>> svc2 = zero.set_reranking(myRrkSvcOpt).build()
            >>> print(svc2.as_dict())
            {'rerank': {'enabled': True, 'service': {'provider': 'myProvider', 'modelName': 'myModelName', 'parameters': {'p': 'z'}}}}
            >>>
            >>> myColRrkOpt = CollectionRerankOptions(
            ...     enabled=False,
            ...     service=None,
            ... )
            >>> svc3 = zero.set_reranking(myColRrkOpt).build()
            >>> print(svc3.as_dict())
            {'rerank': {'enabled': False}}
            >>>
            >>> reset = svc1.set_rerank(None).build()
            >>> print(reset.as_dict())
            {}
        """

        if isinstance(provider, RerankServiceOptions):
            if (
                model_name is not None
                or authentication is not None
                or parameters is not None
                or enabled is not None
            ):
                msg = (
                    "Parameters 'model_name', 'authentication', 'parameters' and "
                    "'enabled' cannot be passed when setting a "
                    "RerankServiceOptions directly."
                )
                raise ValueError(msg)
            return CollectionDefinition(
                vector=self.vector,
                lexical=self.lexical,
                rerank=CollectionRerankOptions(
                    enabled=enabled,
                    service=provider,
                ),
                indexing=self.indexing,
                default_id=self.default_id,
            )
        elif isinstance(provider, CollectionRerankOptions):
            if (
                model_name is not None
                or authentication is not None
                or parameters is not None
                or enabled is not None
            ):
                msg = (
                    "Parameters 'model_name', 'authentication', 'parameters' and "
                    "'enabled' cannot be passed when setting a "
                    "CollectionRerankOptions directly."
                )
                raise ValueError(msg)
            return CollectionDefinition(
                vector=self.vector,
                lexical=self.lexical,
                rerank=provider,
                indexing=self.indexing,
                default_id=self.default_id,
            )
        else:
            new_service: CollectionRerankOptions | None
            if provider is None:
                if (
                    model_name is not None
                    or authentication is not None
                    or parameters is not None
                ):
                    msg = (
                        "Parameters 'model_name', 'authentication' and 'parameters' "
                        "cannot be passed when unsetting the rerank."
                    )
                    raise ValueError(msg)
                new_service = None
            else:
                new_service = CollectionRerankOptions(
                    enabled=enabled,
                    service=RerankServiceOptions(
                        provider=provider,
                        model_name=model_name,
                        authentication=authentication,
                        parameters=parameters,
                    ),
                )
            return CollectionDefinition(
                vector=self.vector,
                lexical=self.lexical,
                rerank=new_service,
                indexing=self.indexing,
                default_id=self.default_id,
            )

    def set_lexical(
        self,
        analyzer: str | dict[str, Any] | CollectionLexicalOptions | None,
        *,
        enabled: bool | None = None,
    ) -> CollectionDefinition:
        """
        Return a new collection definition object with a new 'lexical' setting.

        This method is for use within the fluent interface for progressively
        building a complete collection definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            analyzer: this can be (1) a string or free-form dictionary, specifying
                the configuration for the collection analyzer; or (2) a ready
                `CollectionLexicalOptions` object encoding said configuration;
                alternatively (3) None, to remove the lexical setting from the
                collection definition hence letting the API use its defaults.
                See the examples below for an illustration of these usage patterns.
            enabled: if passed, this flag is used in the lexical definition
                for the collection. If omitted, defaults to True.

        Returns:
            a CollectionDefinition obtained by adding (or replacing) the desired
            lexical setting to this collection definition.

        Example:
            >>> from astrapy.info import CollectionDefinition, CollectionLexicalOptions
            >>>
            >>> zero = CollectionDefinition.builder()
            >>>
            >>> anz1 = zero.set_lexical(
            ...     "analyzer_setting",
            ... )
            >>> print(anz1.build().as_dict())
            {'lexical': {'enabled': True, 'analyzer': 'analyzer_setting'}}
            >>> myLexOpt = CollectionLexicalOptions(analyzer="analyzer_setting")
            >>> anz2 = zero.set_lexical(myLexOpt).build()
            >>> print(anz2.as_dict())
            {'lexical': {'enabled': True, 'analyzer': 'analyzer_setting'}}
            >>> reset = anz1.set_lexical(None).build()
            >>> print(reset.as_dict())
            {}
        """

        if analyzer is None:
            if enabled is not None:
                msg = "Parameter 'enabled' cannot be passed when disabling 'lexical'."
                raise ValueError(msg)
            return CollectionDefinition(
                vector=self.vector,
                lexical=None,
                rerank=self.rerank,
                indexing=self.indexing,
                default_id=self.default_id,
            )
        elif isinstance(analyzer, CollectionLexicalOptions):
            if enabled is not None:
                msg = (
                    "Parameter 'enabled' cannot be passed when setting 'lexical' "
                    "through a CollectionLexicalOptions object."
                )
                raise ValueError(msg)
            return CollectionDefinition(
                vector=self.vector,
                lexical=analyzer,
                rerank=self.rerank,
                indexing=self.indexing,
                default_id=self.default_id,
            )
        else:
            new_lexical = CollectionLexicalOptions(
                enabled=enabled,
                analyzer=analyzer,
            )
            return CollectionDefinition(
                vector=self.vector,
                lexical=new_lexical,
                rerank=self.rerank,
                indexing=self.indexing,
                default_id=self.default_id,
            )

    def build(self) -> CollectionDefinition:
        """
        The final step in the fluent (builder) interface. Calling this method
        finalizes the definition that has been built so far and makes it into a
        collection definition ready for use in e.g. table creation.

        Note that this step may be automatically invoked by the receiving methods:
        however it is a good practice - and also adds to the readability of the code -
        to call it explicitly.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a CollectionDefinition obtained by finalizing the definition being
                built so far.
        """

        return self


@dataclass
class CollectionDescriptor:
    """
    A structure expressing full description of a collection as the Data API
    returns it, i.e. its name and its definition.

    Attributes:
        name: the name of the collection.
        definition: a CollectionDefinition instance.
        raw_descriptor: the raw response from the Data API.

    Note:
        although the API format has the collection settings in a field called
        "options" (both in payloads and in responses, consistently), the corresponding
        attribute of this object is called `definition` to keep consistency with the
        TableDescriptor class and the attribute's data type (`CollectionDefinition`).
        As a consequence, when coercing a plain dictionary into this class, care must
        be taken that the plain dictionary key be "options", as could a response from
        the API have it.
    """

    name: str
    definition: CollectionDefinition
    raw_descriptor: dict[str, Any] | None

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                f"name={self.name.__repr__()}",
                f"definition={self.definition.__repr__()}",
                None if self.raw_descriptor is None else "raw_descriptor=...",
            ]
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(not_null_pieces)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CollectionDescriptor):
            return self.name == other.name and self.definition == other.definition
        else:
            return False

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        Empty `definition` will not be returned at all.
        """

        return {
            k: v
            for k, v in {
                "name": self.name,
                "options": self.definition.as_dict(),
            }.items()
            if v
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> CollectionDescriptor:
        """
        Create an instance of CollectionDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"name", "options"})
        return CollectionDescriptor(
            name=raw_dict["name"],
            definition=CollectionDefinition._from_dict(raw_dict.get("options") or {}),
            raw_descriptor=raw_dict,
        )

    @classmethod
    def coerce(
        cls, raw_input: CollectionDescriptor | dict[str, Any]
    ) -> CollectionDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a CollectionDescriptor.
        """

        if isinstance(raw_input, CollectionDescriptor):
            return raw_input
        else:
            return cls._from_dict(raw_input)
