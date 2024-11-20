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
        default_id_type: string such as `objectId`, `uuid6` and so on.
    """

    default_id_type: str

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {"type": self.default_id_type}

    @staticmethod
    def from_dict(raw_dict: dict[str, Any] | None) -> CollectionDefaultIDOptions | None:
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
        dimension: an optional positive integer, the dimensionality of the vector space.
        metric: an optional metric among `VectorMetric.DOT_PRODUCT`,
            `VectorMetric.EUCLIDEAN` and `VectorMetric.COSINE`.
        source_model: a specification of the embedding model the embeddings come from,
            which the index uses internally to optimize its internal settings.
            Defaults to "other".
        service: an optional VectorServiceOptions object in case a
            service is configured for the collection.
    """

    dimension: int | None
    metric: str | None
    source_model: str | None
    service: VectorServiceOptions | None

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
    def from_dict(raw_dict: dict[str, Any] | None) -> CollectionVectorOptions | None:
        """
        Create an instance of CollectionVectorOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return CollectionVectorOptions(
                dimension=raw_dict.get("dimension"),
                metric=raw_dict.get("metric"),
                source_model=raw_dict.get("sourceModel"),
                service=VectorServiceOptions.from_dict(raw_dict.get("service")),
            )
        else:
            return None


@dataclass
class CollectionDefinition:
    """
    A structure expressing the options of a collection.
    See the Data API specifications for detailed specification and allowed values.

    Attributes:
        vector: an optional CollectionVectorOptions object.
        indexing: an optional dictionary with the "indexing" collection properties.
        default_id: an optional CollectionDefaultIDOptions object.
    """

    vector: CollectionVectorOptions | None
    indexing: dict[str, Any] | None
    default_id: CollectionDefaultIDOptions | None

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
        }

    def flatten(self) -> dict[str, Any]:
        """
        Recast this object as a flat key-value pair suitable for
        use as kwargs in a create_collection method call (including recasts).
        """

        _dimension: int | None
        _metric: str | None
        _indexing: dict[str, Any] | None
        _service: dict[str, Any] | None
        _default_id_type: str | None
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
    def from_dict(raw_dict: dict[str, Any]) -> CollectionDefinition:
        """
        Create an instance of CollectionDefinition from a dictionary
        such as one from the Data API.
        """

        return CollectionDefinition(
            vector=CollectionVectorOptions.from_dict(raw_dict.get("vector")),
            indexing=raw_dict.get("indexing"),
            default_id=CollectionDefaultIDOptions.from_dict(raw_dict.get("defaultId")),
        )


@dataclass
class CollectionDescriptor:
    """
    A structure expressing full description of a collection as the Data API
    returns it, i.e. its name and its `options` sub-structure.

    Attributes:
        name: the name of the collection.
        options: a CollectionDefinition instance.
        raw_descriptor: the raw response from the Data API.
    """

    name: str
    options: CollectionDefinition
    raw_descriptor: dict[str, Any] | None

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                f"name={self.name.__repr__()}",
                f"options={self.options.__repr__()}",
                None if self.raw_descriptor is None else "raw_descriptor=...",
            ]
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(not_null_pieces)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CollectionDescriptor):
            return self.name == other.name and self.options == other.options
        else:
            return False

    def as_dict(self) -> dict[str, Any]:
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

    def flatten(self) -> dict[str, Any]:
        """
        Recast this object as a flat key-value pair suitable for
        use as kwargs in a create_collection method call (including recasts).
        """

        return {
            **(self.options.flatten()),
            **{"name": self.name},
        }

    @staticmethod
    def from_dict(raw_dict: dict[str, Any]) -> CollectionDescriptor:
        """
        Create an instance of CollectionDescriptor from a dictionary
        such as one from the Data API.
        """

        return CollectionDescriptor(
            name=raw_dict["name"],
            options=CollectionDefinition.from_dict(raw_dict.get("options") or {}),
            raw_descriptor=raw_dict,
        )
