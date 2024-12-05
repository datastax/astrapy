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
        ...     CollectionDefinition.zero()
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
    indexing: dict[str, Any] | None = None
    default_id: CollectionDefaultIDOptions | None = None

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

    def as_dict(self) -> dict[str, Any]:
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
            if v != {}
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> CollectionDefinition:
        """
        Create an instance of CollectionDefinition from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"vector", "indexing", "defaultId"})
        return CollectionDefinition(
            vector=CollectionVectorOptions._from_dict(raw_dict.get("vector")),
            indexing=raw_dict.get("indexing"),
            default_id=CollectionDefaultIDOptions._from_dict(raw_dict.get("defaultId")),
        )

    @classmethod
    def coerce(
        cls, raw_input: CollectionDefinition | dict[str, Any]
    ) -> CollectionDefinition:
        """TODO"""
        if isinstance(raw_input, CollectionDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @staticmethod
    def zero() -> CollectionDefinition:
        """TODO"""
        return CollectionDefinition()

    def set_indexing(
        self, indexing_mode: str | None, indexing_target: list[str] | None = None
    ) -> CollectionDefinition:
        """TODO"""
        if indexing_mode is None:
            if indexing_target is not None:
                raise ValueError("Cannot pass an indexing target if unsetting indexing")
            return CollectionDefinition(
                vector=self.vector,
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
            indexing={indexing_mode: indexing_target},
            default_id=self.default_id,
        )

    def set_default_id(self, default_id_type: str | None) -> CollectionDefinition:
        """TODO"""
        if default_id_type is None:
            return CollectionDefinition(
                vector=self.vector,
                indexing=self.indexing,
                default_id=None,
            )

        return CollectionDefinition(
            vector=self.vector,
            indexing=self.indexing,
            default_id=CollectionDefaultIDOptions(
                default_id_type=default_id_type,
            ),
        )

    def set_vector_dimension(self, dimension: int | None) -> CollectionDefinition:
        """TODO"""
        _vector_options = self.vector or CollectionVectorOptions()
        return CollectionDefinition(
            vector=CollectionVectorOptions(
                dimension=dimension,
                metric=_vector_options.metric,
                source_model=_vector_options.source_model,
                service=_vector_options.service,
            ),
            indexing=self.indexing,
            default_id=self.default_id,
        )

    def set_vector_metric(self, metric: str | None) -> CollectionDefinition:
        """TODO"""
        _vector_options = self.vector or CollectionVectorOptions()
        return CollectionDefinition(
            vector=CollectionVectorOptions(
                dimension=_vector_options.dimension,
                metric=metric,
                source_model=_vector_options.source_model,
                service=_vector_options.service,
            ),
            indexing=self.indexing,
            default_id=self.default_id,
        )

    def set_vector_source_model(self, source_model: str | None) -> CollectionDefinition:
        """TODO"""
        _vector_options = self.vector or CollectionVectorOptions()
        return CollectionDefinition(
            vector=CollectionVectorOptions(
                dimension=_vector_options.dimension,
                metric=_vector_options.metric,
                source_model=source_model,
                service=_vector_options.service,
            ),
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
        TODO
        Three valid patterns: (1) pass a ready-made service,
        (2) pass its attributes, or (3) None, to unset.
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
                indexing=self.indexing,
                default_id=self.default_id,
            )

    def build(self) -> CollectionDefinition:
        """TODO"""
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
        """TODO"""
        if isinstance(raw_input, CollectionDescriptor):
            return raw_input
        else:
            return cls._from_dict(raw_input)
