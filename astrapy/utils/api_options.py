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

import datetime
from dataclasses import dataclass
from typing import Iterable, Sequence

from astrapy.authentication import (
    EmbeddingAPIKeyHeaderProvider,
    EmbeddingHeadersProvider,
    RerankingAPIKeyHeaderProvider,
    RerankingHeadersProvider,
    StaticTokenProvider,
    TokenProvider,
    coerce_possible_embedding_headers_provider,
    coerce_possible_reranking_headers_provider,
    coerce_possible_token_provider,
)
from astrapy.constants import CallerType, Environment, MapEncodingMode
from astrapy.settings.defaults import (
    API_PATH_ENV_MAP,
    API_VERSION_ENV_MAP,
    DEFAULT_ACCEPT_NAIVE_DATETIMES,
    DEFAULT_BINARY_ENCODE_VECTORS,
    DEFAULT_COLLECTION_ADMIN_TIMEOUT_MS,
    DEFAULT_CUSTOM_DATATYPES_IN_READING,
    DEFAULT_DATABASE_ADMIN_TIMEOUT_MS,
    DEFAULT_DATETIME_TZINFO,
    DEFAULT_ENCODE_MAPS_AS_LISTS_IN_TABLES,
    DEFAULT_GENERAL_METHOD_TIMEOUT_MS,
    DEFAULT_KEYSPACE_ADMIN_TIMEOUT_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    DEFAULT_TABLE_ADMIN_TIMEOUT_MS,
    DEFAULT_UNROLL_ITERABLES_TO_LISTS,
    DEFAULT_USE_DECIMALS_IN_COLLECTIONS,
    DEV_OPS_URL_ENV_MAP,
    DEV_OPS_VERSION_ENV_MAP,
    FIXED_SECRET_PLACEHOLDER,
)
from astrapy.utils.unset import _UNSET, UnsetType


@dataclass
class TimeoutOptions:
    """
    The group of settings for the API Options concerning the configured timeouts
    for various kinds of API operations.

    All timeout values are integers expressed
    in milliseconds. A timeout of zero signifies that no timeout is imposed at all
    on that kind of operation.

    All methods that issue HTTP requests allow for a per-invocation override
    of the relevant timeouts involved (see the method docstring and signature
    for details).

    This class is used to override default settings when creating objects such
    as DataAPIClient, Database, Table, Collection and so on. Values that are left
    unspecified will keep the values inherited from the parent "spawner" class.
    See the `APIOptions` master object for more information and usage examples.

    Attributes:
        request_timeout_ms: the timeout imposed on a single HTTP request.
            This is applied to all HTTP requests to both the Data API and the DevOps
            API, with the specific exception of the one request issued by the
            Database's create_collection method (which despite being a single request
            may take a considerable amount of time to complete). Defaults to 10 s.
        general_method_timeout_ms: a timeout to use on the overall duration of a
            method invocation, valid for DML methods which are not concerned with
            schema or admin operations. For some methods (such as `find_one` for
            the Table or Collection classes), this coincides with `request_timeout_ms`:
            in that case, the minimum value of the two is used to limit the request
            duration. For other methods, which possibly comprise several HTTP requests
            (for example `insert_many`), this timeout limits the overall duration time
            of the method invocation, while the per-request timeout is still determined
            by `request_timeout_ms`, separately. Defaults to 30 s.
        collection_admin_timeout_ms: a timeout for all collection-related schema and
            admin operations: creating/dropping a collection, as well as listing them.
            With the exception of collection creation, each individual request issued as
            part of collection-schema operations still must obey `request_timeout_ms`.
            Defaults to 60 s.
        table_admin_timeout_ms: a timeout for all table-related schema and
            admin operations: creating/altering/dropping a table or a table index,
            as well as listing tables/indexes. Each individual request issued as part
            of table-schema operations still must obey `request_timeout_ms`.
            Defaults to 30 s.
        database_admin_timeout_ms: a timeout for all database-related admin operations.
            Creating/dropping a database, listing databases, getting database info,
            querying for the available embedding providers, and others, are all subject
            to this timeout. The longest-running operations in this class are
            the creation and the destruction of a database: if called with the
            `wait_until_complete=True` parameter, these can last several minutes.
            This is the timeout that control if and when the method invocation should
            ever error with an `astrapy.exceptions.DataAPITimeoutException`.
            Note that individual API request issued within those operation still must
            obey the per-request `request_timeout_ms` additional constraint.
            Defaults to 10 m.
        keyspace_admin_timeout_ms: a timeout for all keyspace-related admin operations.
            Creating, altering and dropping a keyspace, as well as listing keyspaces,
            are operations controlled by this timeout. Individual API request issued
            within those operation still must obey the per-request
            `request_timeout_ms` additional constraint. Defaults to 30 s.
    """

    request_timeout_ms: int | UnsetType = _UNSET
    general_method_timeout_ms: int | UnsetType = _UNSET
    collection_admin_timeout_ms: int | UnsetType = _UNSET
    table_admin_timeout_ms: int | UnsetType = _UNSET
    database_admin_timeout_ms: int | UnsetType = _UNSET
    keyspace_admin_timeout_ms: int | UnsetType = _UNSET


@dataclass
class FullTimeoutOptions(TimeoutOptions):
    """
    The group of settings for the API Options concerning the configured timeouts
    for various kinds of API operations.

    All timeout values are integers expressed
    in milliseconds. A timeout of zero signifies that no timeout is imposed at all
    on that kind of operation.

    All methods that issue HTTP requests allow for a per-invocation override
    of the relevant timeouts involved (see the method docstring and signature
    for details).

    This is the "full" version of the class, with the guarantee that all of its members
    have defined values. As such, this is what classes such as DataAPIClient, Database,
    Table, Collection and so on have in their `.api_options` attribute -- as opposed
    to the (non-full) `TimeoutOptions` counterpart class: the latter admits "unset"
    attributes and is used to override specific settings.

    Attributes:
        request_timeout_ms: the timeout imposed on a single HTTP request.
            This is applied to all HTTP requests to both the Data API and the DevOps
            API, with the specific exception of the one request issued by the
            Database's create_collection method (which despite being a single request
            may take a considerable amount of time to complete). Defaults to 10 s.
        general_method_timeout_ms: a timeout to use on the overall duration of a
            method invocation, valid for DML methods which are not concerned with
            schema or admin operations. For some methods (such as `find_one` for
            the Table or Collection classes), this coincides with `request_timeout_ms`:
            in that case, the minimum value of the two is used to limit the request
            duration. For other methods, which possibly comprise several HTTP requests
            (for example `insert_many`), this timeout limits the overall duration time
            of the method invocation, while the per-request timeout is still determined
            by `request_timeout_ms`, separately. Defaults to 30 s.
        collection_admin_timeout_ms: a timeout for all collection-related schema and
            admin operations: creating/dropping a collection, as well as listing them.
            With the exception of collection creation, each individual request issued as
            part of collection-schema operations still must obey `request_timeout_ms`.
            Defaults to 60 s.
        table_admin_timeout_ms: a timeout for all table-related schema and
            admin operations: creating/altering/dropping a table or a table index,
            as well as listing tables/indexes. Each individual request issued as part
            of table-schema operations still must obey `request_timeout_ms`.
            Defaults to 30 s.
        database_admin_timeout_ms: a timeout for all database-related admin operations.
            Creating/dropping a database, listing databases, getting database info,
            querying for the available embedding providers, and others, are all subject
            to this timeout. The longest-running operations in this class are
            the creation and the destruction of a database: if called with the
            `wait_until_complete=True` parameter, these can last several minutes.
            This is the timeout that control if and when the method invocation should
            ever error with an `astrapy.exceptions.DataAPITimeoutException`.
            Note that individual API request issued within those operation still must
            obey the per-request `request_timeout_ms` additional constraint.
            Defaults to 10 m.
        keyspace_admin_timeout_ms: a timeout for all keyspace-related admin operations.
            Creating, altering and dropping a keyspace, as well as listing keyspaces,
            are operations controlled by this timeout. Individual API request issued
            within those operation still must obey the per-request
            `request_timeout_ms` additional constraint. Defaults to 30 s.
    """

    request_timeout_ms: int
    general_method_timeout_ms: int
    collection_admin_timeout_ms: int
    table_admin_timeout_ms: int
    database_admin_timeout_ms: int
    keyspace_admin_timeout_ms: int

    def __init__(
        self,
        *,
        request_timeout_ms: int,
        general_method_timeout_ms: int,
        collection_admin_timeout_ms: int,
        table_admin_timeout_ms: int,
        database_admin_timeout_ms: int,
        keyspace_admin_timeout_ms: int,
    ) -> None:
        TimeoutOptions.__init__(
            self,
            request_timeout_ms=request_timeout_ms,
            general_method_timeout_ms=general_method_timeout_ms,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            table_admin_timeout_ms=table_admin_timeout_ms,
            database_admin_timeout_ms=database_admin_timeout_ms,
            keyspace_admin_timeout_ms=keyspace_admin_timeout_ms,
        )

    def with_override(self, other: TimeoutOptions) -> FullTimeoutOptions:
        """
        Given an "overriding" set of options, possibly not defined in all its
        attributes, apply the override logic and return a new full options object.

        Args:
            other: a not-necessarily-fully-specified options object. All its defined
                settings take precedence.
        """

        return FullTimeoutOptions(
            request_timeout_ms=(
                other.request_timeout_ms
                if not isinstance(other.request_timeout_ms, UnsetType)
                else self.request_timeout_ms
            ),
            general_method_timeout_ms=(
                other.general_method_timeout_ms
                if not isinstance(other.general_method_timeout_ms, UnsetType)
                else self.general_method_timeout_ms
            ),
            collection_admin_timeout_ms=(
                other.collection_admin_timeout_ms
                if not isinstance(other.collection_admin_timeout_ms, UnsetType)
                else self.collection_admin_timeout_ms
            ),
            table_admin_timeout_ms=(
                other.table_admin_timeout_ms
                if not isinstance(other.table_admin_timeout_ms, UnsetType)
                else self.table_admin_timeout_ms
            ),
            database_admin_timeout_ms=(
                other.database_admin_timeout_ms
                if not isinstance(other.database_admin_timeout_ms, UnsetType)
                else self.database_admin_timeout_ms
            ),
            keyspace_admin_timeout_ms=(
                other.keyspace_admin_timeout_ms
                if not isinstance(other.keyspace_admin_timeout_ms, UnsetType)
                else self.keyspace_admin_timeout_ms
            ),
        )


@dataclass
class SerdesOptions:
    """
    The group of settings for the API Options concerning serialization and
    deserialization of values to and from the Data API. Write-path settings
    affect how values are encoded in the JSON payload to API requests, and
    read-path settings determine the choice of data types used to represent
    values found in the API responses when a method returns data.

    This class is used to override default settings when creating objects such
    as DataAPIClient, Database, Table, Collection and so on. Values that are left
    unspecified will keep the values inherited from the parent "spawner" class.
    See the `APIOptions` master object for more information and usage examples.

    Only the classes that directly exchange data with the API, i.e. Table and
    Collection, make actual use of the settings in this group. Nevertheless,
    all objects in the hierarchy (for example DataAPIClient) have their own
    customizable SerdesOptions settings: this makes it easy to configure one's
    preference once and just have each spawned Collection or Table inherit the
    desired settings.

    Attributes:
        binary_encode_vectors: Write-Path. Whether to encode vectors using the faster,
            more efficient binary encoding as opposed to sending plain lists
            of numbers. For Tables, this affects vectors passed to write methods
            as instances of `DataAPIVector`, while for collections this affects
            the encoding of the quantity found in the "$vector" field, if present,
            regardless of its representation in the method argument. Defaults to True.
        custom_datatypes_in_reading: Read-Path. This setting determines whether return
            values from read methods should use astrapy custom classes (default setting
            of True), or try to use only standard-library data types instead (False).
            The astrapy classes are designed to losslessly express DB values.
            For Collections, this setting only affects timestamp and vector values,
            while when reading from a Table there are several other implications.
            Keep in mind that for some of the data types, choosing the stdlib fallback
            may lead to approximate results and even lead to errors (for instance if a
            date stored on database falls outside of the range the stdlib can express).
            Here is a list of the fallbacks (see the individual classes for more info):
            * DataAPIVector => list[float]: no loss of expressivity.
            * DataAPITimestamp  => datetime.datetime: shorter year range (1AD-9999AD)
            and lossy storage of sub-second part for ancient years.
            * DataAPIDate => datetime.date: same year range limitations.
            * DataAPITime => datetime.time: approximation (datetime.time
            has microsecond precision, so the nanosecond part, if present, is lost).
            * DataAPIDuration => datetime.timedelta: durations are intrinsically a
            different thing (see `DataAPIDuration` class). If requested, a coercion
            into a timedelta is attempted, but (a) months are not expressable, (b)
            days are always interpreted as made of 24 hours (despite the occasional
            23- or 25-hour day), and (c) nanoseconds are lost just like for DataAPITime.
            * DataAPIMap => dict: would raise an error if non-hashable data types (e.g.
            lists) are allowed as keys in map-columns on the Table.
            * DataAPISet => set: would raise an error if non-hashable data types (e.g.
            lists) are allowed as entries in set-columns on the Table.
        unroll_iterables_to_lists: Write-Path. If this is set to True, a wider group
            of data types can be provided where a list of values is expected: anything
            that can be iterated over is automatically "unrolled" into a list prior to
            insertion. This means, for example, that one can directly use classes such
            as numpy objects or generators for writes where a vector is expected.
            This setting defaults to False, as it incurs some performance cost (due
            to preemptive inspection of all values in dictionaries provided for writes).
        use_decimals_in_collections: both Read- and Write-path. The `decimal.Decimal`
            standard library class represents lossless numbers with exact arithmetic
            (as opposed to the standard Python floats which hold a finite number
            of significant digits, thus possibly with approximate arithmetic).
            This settings, relevant for Collections only, affects both paths:
            * for the Write-Path, if set to True, `Decimal` instances are accepted
            for storage into collections and are written losslessly. The default
            value of False means that `Decimal` numbers are not accepted for writes.
            * for the Read-Path, if this is set to True, then *all* numeric values
            found in documents from the collection (except the values in "$vector")
            are returned as `Decimal` instances. Conversely, with the default setting
            of `use_decimals_in_collections = False`, read documents are returned as
            containing only regular integers and floats.
            Before switching this setting to True, one should consider the actual need
            for lossless arbitrary decimal precision in their application: besides the
            fact that every number is then returned as an instance of `Decimal` when
            reading from collections, an additional performance cost is required to
            manage the serialization/deserialization of objects exchanged with the API.
        encode_maps_as_lists_in_tables: Write-Path. Whether 'maps' (`dict` and/or
            `DataAPIMap` objects alike) are automatically converted into association
            lists if they are found in suitable portions of the write payload for Table
            operations, i.e. if they represent parts of a row (including updates, etc).
            Takes values in the `astrapy.constants.MapEncodingMode` enum: for "NEVER",
            no such conversion occurs; for "DATAAPIMAPS" it takes place only for
            instances of `DataAPIMap`, for "ALWAYS" also `dict` objects are transformed.
            When (and where) enabled, the automatic conversions have the following form:
            * `{10: "ten"} ==> [[10, "ten"]]`
            * `DataAPIMap([('a', 1), ('b', 2)]) ==> [["a", 1], ["b", 2]]`.
            Defaults to "NEVER".
        accept_naive_datetimes: Write-Path. Python datetimes can be either "naive" or
            "aware" of a timezone/offset information. Only the latter type can be
            translated unambiguously and without implied assumptions into a well-defined
            timestamp. Because the Data API always stores timestamps, by default astrapy
            will raise an error if a write is attempted that uses a naive datetime.
            If this setting is changed to True (from its default of False), then astrapy
            will stop complaining about naive datetimes and accept them as valid
            timestamps for writes. These will be converted into timestamps using
            their `.timestamp()` method, which uses the system locale implicitly.
            It is important to appreciate the possible consequences, for example
            if a table or collection is shared by instances of the application
            running with different system locales.
        datetime_tzinfo: Read-Path. When reading timestamps from tables or collection
            with the setting `custom_datatypes_in_reading = False`, ordinary
            `datetime.datetime` objects are returned for timestamps read from the
            database. This setting (defaulting to `datetime.timezone.utc`) determines
            the timezone used in the returned datetime objects. Setting this value
            to None results in naive datetimes being returned (not recommended).
    """

    binary_encode_vectors: bool | UnsetType
    custom_datatypes_in_reading: bool | UnsetType
    unroll_iterables_to_lists: bool | UnsetType
    use_decimals_in_collections: bool | UnsetType
    encode_maps_as_lists_in_tables: MapEncodingMode | UnsetType
    accept_naive_datetimes: bool | UnsetType
    datetime_tzinfo: datetime.timezone | None | UnsetType

    def __init__(
        self,
        *,
        binary_encode_vectors: bool | UnsetType = _UNSET,
        custom_datatypes_in_reading: bool | UnsetType = _UNSET,
        unroll_iterables_to_lists: bool | UnsetType = _UNSET,
        use_decimals_in_collections: bool | UnsetType = _UNSET,
        encode_maps_as_lists_in_tables: str | MapEncodingMode | UnsetType = _UNSET,
        accept_naive_datetimes: bool | UnsetType = _UNSET,
        datetime_tzinfo: datetime.timezone | None | UnsetType = _UNSET,
    ) -> None:
        self.binary_encode_vectors = binary_encode_vectors
        self.custom_datatypes_in_reading = custom_datatypes_in_reading
        self.unroll_iterables_to_lists = unroll_iterables_to_lists
        self.use_decimals_in_collections = use_decimals_in_collections
        if isinstance(encode_maps_as_lists_in_tables, str):
            self.encode_maps_as_lists_in_tables = MapEncodingMode.coerce(
                encode_maps_as_lists_in_tables
            )
        else:
            self.encode_maps_as_lists_in_tables = encode_maps_as_lists_in_tables
        self.accept_naive_datetimes = accept_naive_datetimes
        self.datetime_tzinfo = datetime_tzinfo


@dataclass
class FullSerdesOptions(SerdesOptions):
    """
    The group of settings for the API Options concerning serialization and
    deserialization of values to and from the Data API. Write-path settings
    affect how values are encoded in the JSON payload to API requests, and
    read-path settings determine the choice of data types used to represent
    values found in the API responses when a method returns data.

    This is the "full" version of the class, with the guarantee that all of its members
    have defined values. As such, this is what classes such as DataAPIClient, Database,
    Table, Collection and so on have in their `.api_options` attribute -- as opposed
    to the (non-full) `SerdesOptions` counterpart class: the latter admits "unset"
    attributes and is used to override specific settings.

    Only the classes that directly exchange data with the API, i.e. Table and
    Collection, make actual use of the settings in this group. Nevertheless,
    all objects in the hierarchy (for example DataAPIClient) have their own
    customizable SerdesOptions settings: this makes it easy to configure one's
    preference once and just have each spawned Collection or Table inherit the
    desired settings.

    Attributes:
        binary_encode_vectors: Write-Path. Whether to encode vectors using the faster,
            more efficient binary encoding as opposed to sending plain lists
            of numbers. For Tables, this affects vectors passed to write methods
            as instances of `DataAPIVector`, while for collections this affect
            the encoding of the quantity found in the "$vector" field, if present,
            regardless of its representation in the method argument. Defaults to True.
        custom_datatypes_in_reading: Read-Path. This setting determines whether return
            values from read methods should use astrapy custom classes (default setting
            of True), or try to use only standard-library data types instead (False).
            The astrapy classes are designed to losslessly express DB values.
            For Collections, this setting only affects timestamp and vector values,
            while when reading from a Table there are several other implications.
            Keep in mind that for some of the data types, choosing the stdlib fallback
            may lead to approximate results and even lead to errors (for instance if a
            date stored on database falls outside of the range the stdlib can express).
            Here is a list of the fallbacks (see the individual classes for more info):
            * DataAPIVector => list[float]: no loss of expressivity.
            * DataAPITimestamp  => datetime.datetime: shorter year range (1AD-9999AD)
            and lossy storage of sub-second part for ancient years.
            * DataAPIDate => datetime.date: same year range limitations.
            * DataAPITime => datetime.time: approximation (datetime.time
            has microsecond precision, so the nanosecond part, if present, is lost).
            * DataAPIDuration => datetime.timedelta: durations are intrinsically a
            different thing (see `DataAPIDuration` class). If requested, a coercion
            into a timedelta is attempted, but (a) months are not expressable, (b)
            days are always interpreted as made of 24 hours (despite the occasional
            23- or 25-hour day), and (c) nanoseconds are lost just like for DataAPITime.
            * DataAPIMap => dict: would raise an error if non-hashable data types (e.g.
            lists) are allowed as keys in map-columns on the Table.
            * DataAPISet => set: would raise an error if non-hashable data types (e.g.
            lists) are allowed as entries in set-columns on the Table.
        unroll_iterables_to_lists: Write-Path. If this is set to True, a wider group
            of data types can be provided where a list of values is expected: anything
            that can be iterated over is automatically "unrolled" into a list prior to
            insertion. This means, for example, that one can directly use classes such
            as numpy objects or generators for writes where a vector is expected.
            This setting defaults to False, as it incurs some performance cost (due
            to preemptive inspection of all values in dictionaries provided for writes).
        use_decimals_in_collections: both Read- and Write-path. The `decimal.Decimal`
            standard library class represents lossless numbers with exact arithmetic
            (as opposed to the standard Python floats which hold a finite number
            of significant digits, thus possibly with approximate arithmetic).
            This settings, relevant for Collections only, affects both paths:
            * for the Write-Path, if set to True, `Decimal` instances are accepted
            for storage into collections and are written losslessly. The default
            value of False means that `Decimal` numbers are not accepted for writes.
            * for the Read-Path, if this is set to True, then *all* numeric values
            found in documents from the collection (except the values in "$vector")
            are returned as `Decimal` instances. Conversely, with the default setting
            of `use_decimals_in_collections = False`, read documents are returned as
            containing only regular integers and floats.
            Before switching this setting to True, one should consider the actual need
            for lossless arbitrary decimal precision in their application: besides the
            fact that every number is then returned as an instance of `Decimal` when
            reading from collections, an additional performance cost is required to
            manage the serialization/deserialization of objects exchanged with the API.
        encode_maps_as_lists_in_tables: Write-Path. Whether 'maps' (`dict` and/or
            `DataAPIMap` objects alike) are automatically converted into association
            lists if they are found in suitable portions of the write payload for Table
            operations, i.e. if they represent parts of a row (including updates, etc).
            Takes values in the `astrapy.constants.MapEncodingMode` enum: for "NEVER",
            no such conversion occurs; for "DATAAPIMAPS" it takes place only for
            instances of `DataAPIMap`, for "ALWAYS" also `dict` objects are transformed.
            When (and where) enabled, the automatic conversions have the following form:
            * `{10: "ten"} ==> [[10, "ten"]]`
            * `DataAPIMap([('a', 1), ('b', 2)]) ==> [["a", 1], ["b", 2]]`.
            Defaults to "NEVER".
        accept_naive_datetimes: Write-Path. Python datetimes can be either "naive" or
            "aware" of a timezone/offset information. Only the latter type can be
            translated unambiguously and without implied assumptions into a well-defined
            timestamp. Because the Data API always stores timestamps, by default astrapy
            will raise an error if a write is attempted that uses a naive datetime.
            If this setting is changed to True (from its default of False), then astrapy
            will stop complaining about naive datetimes and accept them as valid
            timestamps for writes. These will be converted into timestamps using
            their `.timestamp()` method, which uses the system locale implicitly.
            It is important to appreciate the possible consequences, for example
            if a table or collection is shared by instances of the application
            running with different system locales.
        datetime_tzinfo: Read-Path. When reading timestamps from tables or collection
            with the setting `custom_datatypes_in_reading = False`, ordinary
            `datetime.datetime` objects are returned for timestamps read from the
            database. This setting (defaulting to `datetime.timezone.utc`) determines
            the timezone used in the returned datetime objects. Setting this value
            to None results in naive datetimes being returned (not recommended).
    """

    binary_encode_vectors: bool
    custom_datatypes_in_reading: bool
    unroll_iterables_to_lists: bool
    use_decimals_in_collections: bool
    encode_maps_as_lists_in_tables: MapEncodingMode
    accept_naive_datetimes: bool
    datetime_tzinfo: datetime.timezone | None

    def __init__(
        self,
        *,
        binary_encode_vectors: bool,
        custom_datatypes_in_reading: bool,
        unroll_iterables_to_lists: bool,
        use_decimals_in_collections: bool,
        encode_maps_as_lists_in_tables: str | MapEncodingMode,
        accept_naive_datetimes: bool,
        datetime_tzinfo: datetime.timezone | None,
    ) -> None:
        SerdesOptions.__init__(
            self,
            binary_encode_vectors=binary_encode_vectors,
            custom_datatypes_in_reading=custom_datatypes_in_reading,
            unroll_iterables_to_lists=unroll_iterables_to_lists,
            use_decimals_in_collections=use_decimals_in_collections,
            encode_maps_as_lists_in_tables=encode_maps_as_lists_in_tables,
            accept_naive_datetimes=accept_naive_datetimes,
            datetime_tzinfo=datetime_tzinfo,
        )

    def with_override(self, other: SerdesOptions) -> FullSerdesOptions:
        """
        Given an "overriding" set of options, possibly not defined in all its
        attributes, apply the override logic and return a new full options object.

        Args:
            other: a not-necessarily-fully-specified options object. All its defined
                settings take precedence.
        """

        return FullSerdesOptions(
            binary_encode_vectors=(
                other.binary_encode_vectors
                if not isinstance(other.binary_encode_vectors, UnsetType)
                else self.binary_encode_vectors
            ),
            custom_datatypes_in_reading=(
                other.custom_datatypes_in_reading
                if not isinstance(other.custom_datatypes_in_reading, UnsetType)
                else self.custom_datatypes_in_reading
            ),
            unroll_iterables_to_lists=(
                other.unroll_iterables_to_lists
                if not isinstance(other.unroll_iterables_to_lists, UnsetType)
                else self.unroll_iterables_to_lists
            ),
            use_decimals_in_collections=(
                other.use_decimals_in_collections
                if not isinstance(other.use_decimals_in_collections, UnsetType)
                else self.use_decimals_in_collections
            ),
            encode_maps_as_lists_in_tables=(
                other.encode_maps_as_lists_in_tables
                if not isinstance(other.encode_maps_as_lists_in_tables, UnsetType)
                else self.encode_maps_as_lists_in_tables
            ),
            accept_naive_datetimes=(
                other.accept_naive_datetimes
                if not isinstance(other.accept_naive_datetimes, UnsetType)
                else self.accept_naive_datetimes
            ),
            datetime_tzinfo=(
                other.datetime_tzinfo
                if not isinstance(other.datetime_tzinfo, UnsetType)
                else self.datetime_tzinfo
            ),
        )


@dataclass
class DataAPIURLOptions:
    """
    The group of settings for the API Options that determines the URL used to
    reach the Data API.

    This class is used to override default settings when creating objects such
    as DataAPIClient, Database, Table, Collection and so on. Values that are left
    unspecified will keep the values inherited from the parent "spawner" class.
    See the `APIOptions` master object for more information and usage examples.
    Keep in mind, However, that only in very specific customized scenarios should
    it be necessary to override the default settings for this class.

    Attributes:
        api_path: path to append to the API Endpoint. For Astra DB environments,
            this can be left to its default of "/api/json"; for other environment,
            the default of "" is correct (unless there are specific redirects in place).
        api_version: version specifier to append to the API path. The default values
            of "v1" should not be changed except specific redirects are in place on
            non-Astra environments.
    """

    api_path: str | None | UnsetType = _UNSET
    api_version: str | None | UnsetType = _UNSET


@dataclass
class FullDataAPIURLOptions(DataAPIURLOptions):
    """
    The group of settings for the API Options that determines the URL used to
    reach the Data API.

    This is the "full" version of the class, with the guarantee that all of its members
    have defined values. As such, this is what classes such as DataAPIClient, Database,
    Table, Collection and so on have in their `.api_options` attribute -- as opposed
    to the (non-full) `DataAPIURLOptions` counterpart class: the latter admits "unset"
    attributes and is used to override specific settings. However, only in very specific
    customized scenarios should it be necessary to override the default settings.

    Attributes:
        api_path: path to append to the API Endpoint. For Astra DB environments,
            this can be left to its default of "/api/json"; for other environment,
            the default of "" is correct (unless there are specific redirects in place).
        api_version: version specifier to append to the API path. The default values
            of "v1" should not be changed except specific redirects are in place on
            non-Astra environments.
    """

    api_path: str | None
    api_version: str | None

    def __init__(
        self,
        *,
        api_path: str | None,
        api_version: str | None,
    ) -> None:
        DataAPIURLOptions.__init__(
            self,
            api_path=api_path,
            api_version=api_version,
        )

    def with_override(self, other: DataAPIURLOptions) -> FullDataAPIURLOptions:
        """
        Given an "overriding" set of options, possibly not defined in all its
        attributes, apply the override logic and return a new full options object.

        Args:
            other: a not-necessarily-fully-specified options object. All its defined
                settings take precedence.
        """

        return FullDataAPIURLOptions(
            api_path=(
                other.api_path
                if not isinstance(other.api_path, UnsetType)
                else self.api_path
            ),
            api_version=(
                other.api_version
                if not isinstance(other.api_version, UnsetType)
                else self.api_version
            ),
        )


@dataclass
class DevOpsAPIURLOptions:
    """
    The group of settings for the API Options that determines the URL used to
    reach the DevOps API.

    This class is used to override default settings when creating objects such
    as DataAPIClient, Database, Table, Collection and so on. Values that are left
    unspecified will keep the values inherited from the parent "spawner" class.
    See the `APIOptions` master object for more information and usage examples.

    The default settings for this class depend on the environment; in particular,
    for non-Astra environments (such as HCD or DSE) there is no DevOps API, hence
    these settings are irrelevant.

    Attributes:
        dev_ops_url: This can be used to specify the URL to the DevOps API. The default
            for production Astra DB is "https://api.astra.datastax.com" and should
            never need to be overridden.
        dev_ops_api_version: this specifies a version for the DevOps API
            (the default is "v2"). It should never need to be overridden.
    """

    dev_ops_url: str | UnsetType = _UNSET
    dev_ops_api_version: str | None | UnsetType = _UNSET


@dataclass
class FullDevOpsAPIURLOptions(DevOpsAPIURLOptions):
    """
    The group of settings for the API Options that determines the URL used to
    reach the DevOps API.

    This is the "full" version of the class, with the guarantee that all of its members
    have defined values. As such, this is what classes such as DataAPIClient, Database,
    Table, Collection and so on have in their `.api_options` attribute -- as opposed
    to the (non-full) `DevOpsAPIURLOptions` counterpart class: the latter admits "unset"
    attributes and is used to override specific settings. However, only in very specific
    customized scenarios should it be necessary to override the default settings.

    The default settings for this class depend on the environment; in particular,
    for non-Astra environments (such as HCD or DSE) there is no DevOps API, hence
    these settings are irrelevant.

    Attributes:
        dev_ops_url: This can be used to specify the URL to the DevOps API. The default
            for production Astra DB is "https://api.astra.datastax.com" and should
            never need to be overridden.
        dev_ops_api_version: this specifies a version for the DevOps API
            (the default is "v2"). It should never need to be overridden.
    """

    dev_ops_url: str
    dev_ops_api_version: str | None

    def __init__(
        self,
        *,
        dev_ops_url: str,
        dev_ops_api_version: str | None,
    ) -> None:
        DevOpsAPIURLOptions.__init__(
            self,
            dev_ops_url=dev_ops_url,
            dev_ops_api_version=dev_ops_api_version,
        )

    def with_override(self, other: DevOpsAPIURLOptions) -> FullDevOpsAPIURLOptions:
        """
        Given an "overriding" set of options, possibly not defined in all its
        attributes, apply the override logic and return a new full options object.

        Args:
            other: a not-necessarily-fully-specified options object. All its defined
                settings take precedence.
        """

        return FullDevOpsAPIURLOptions(
            dev_ops_url=(
                other.dev_ops_url
                if not isinstance(other.dev_ops_url, UnsetType)
                else self.dev_ops_url
            ),
            dev_ops_api_version=(
                other.dev_ops_api_version
                if not isinstance(other.dev_ops_api_version, UnsetType)
                else self.dev_ops_api_version
            ),
        )


@dataclass
class APIOptions:
    """
    This class represents all settings that can be configured for how astrapy
    interacts with the API (both Data API and DevOps API). Each object in the
    abstraction hierarchy (DataAPIClient, Database, Table, Collection, ...)
    has a full set of these options that determine how it behaves when performing
    actions toward the API.

    In order to customize the behavior from its preset defaults, one should create
    an `APIOptions` object and either:
    (a) pass it as the `api_options` argument to the DataAPIClient constructor or
    any of the `.with_options` and `.to_[a]sync` methods, to get a new instance of
    an object with some settings changed.
    (b) pass it as the `spawn_api_options` argument to "spawning methods", such as
    `get/create_collection`, `get/create_table`, `get/create_database` or
    `get_database_admin`, to set these option overrides to the returned object.
    The APIOptions object passed as argument can define zero, some or all of its
    members, overriding the corresponding settings and keeping, for all unspecified
    settings, the values inherited from the object whose method is invoked (see some
    examples below).
    Some of these methods admit a few shorthands for settings to be passed directly,
    such as `token="..."`, as an alternative to wrapping the setting into its own
    APIOptions object: this however, while taking precedence over the full object
    if provided at the same time, only covers the few most important settings one
    may want to customize when spawning objects.

    With the exception of the "database/admin additional headers" attributes and
    the "redacted header names", which are merged with the inherited ones,
    the override logic is the following: if an override is provided (even if it
    is None), it completely replaces the inherited value.

    The structure of APIOptions is one and the same throughout the object hierarchy:
    that makes it possible to set, for example, a serialization option for reading
    from Collections at the Database level, so that each Collection spawned from it
    will have the desired behavior; similarly, this makes it possible to set a
    DevOps-API-related option (such as `dev_ops_api_url_options.dev_ops_url`)
    for a Table, an action which has no effect whatsoever.

    Attributes:
        environment: an identifier for the environment for the Data API. This can
            describe an Astra DB environment (such as the default of "prod"), or
            a self-deployed setup (such as "dse" or "hcd"). This setting cannot be
            overridden through customization: it can only be provided when creating
            the DataAPIClient top object in the abstraction hierarchy.
        callers: an iterable of "caller identities" to be used in identifying the
            caller, through the User-Agent header, when issuing requests to the
            Data API. Each caller identity is a `(name, version)` 2-item tuple whose
            elements can be strings or None.
        database_additional_headers: free-form dictionary of additional headers to
            employ when issuing requests to the Data API from Database, Table and
            Collection classes. Passing a key with a value of None means that a certain
            header is suppressed when issuing the request.
        admin_additional_headers: free-form dictionary of additional headers to
            employ when issuing requests to both the Data API and the DevOps API
            from AstraDBAdmin, AstraDBDatabaseAdmin and DataAPIDatabaseAdmin classes.
            Passing a key with a value of None means that a certain header is
            suppressed when issuing the request.
        redacted_header_names: A set of (case-insensitive) strings denoting the headers
            that contain secrets, thus are to be masked when logging request details.
        token: an instance of TokenProvider to provide authentication to requests.
            Passing a string, or None, to this constructor parameter will get it
            automatically converted into the appropriate TokenProvider object.
            Depending on the target (Data API or DevOps API), this one attribute
            is encoded in the request appropriately.
        embedding_api_key: an instance of EmbeddingHeadersProvider should it be needed
            for vectorize-related data operations (used by Tables and Collections).
            Passing a string, or None, to this constructor parameter will get it
            automatically converted into the appropriate EmbeddingHeadersProvider
            object.
        reranking_api_key: an instance of RerankingHeadersProvider should it be needed
            for reranking-related data operations (used by Tables and Collections).
            Passing a string, or None, to this constructor parameter will get it
            automatically converted into the appropriate RerankingHeadersProvider
            object.
        timeout_options: an instance of `TimeoutOptions` (see) to control the timeout
            behavior for the various kinds of operations involving the Data/DevOps API.
        serdes_options: an instance of `SerdesOptions` (see) to customize the
            serializing/deserializing behavior related to writing to and reading from
            tables and collections.
        data_api_url_options: an instance of `DataAPIURLOptions` (see) to customize
            the full URL used to reach the Data API (customizing this setting
            is rarely needed).
        dev_ops_api_url_options: an instance of `DevOpsAPIURLOptions` (see) to
            customize the URL used to reach the DevOps API (customizing this setting
            is rarely needed; relevant only for Astra DB environments).

    Examples:
            >>> from astrapy import DataAPIClient
            >>> from astrapy.api_options import (
            ...     APIOptions,
            ...     SerdesOptions,
            ...     TimeoutOptions,
            ... )
            >>> from astrapy.authentication import (
            ...     StaticTokenProvider,
            ...     AWSEmbeddingHeadersProvider,
            ... )
            >>>
            >>> # Disable custom datatypes in all reads:
            >>> no_cdt_options = APIOptions(
            ...     serdes_options=SerdesOptions(
            ...         custom_datatypes_in_reading=False,
            ...     )
            ... )
            >>> my_client = DataAPIClient(api_options=no_cdt_options)
            >>>
            >>> # These spawned objects inherit that setting:
            >>> my_database = my_client.get_database(
            ...     "https://...",
            ...     token="my-token-1",
            ... )
            >>> my_table = my_database.get_table("my_table")
            >>>
            >>> # Make a copy of `table` with some redefined timeouts
            >>> # and a certain header-based authentication for its vectorize provider:
            >>> my_table_timeouts = TimeoutOptions(
            ...     request_timeout_ms=15000,
            ...     general_method_timeout_ms=30000,
            ...     table_admin_timeout_ms=120000,
            ... )
            >>> my_table_apikey_provider = AWSEmbeddingHeadersProvider(
            ...     embedding_access_id="my-access-id",
            ...     embedding_secret_id="my-secret-id",
            ... )
            >>> my_table_slow_copy = my_table.with_options(
            ...     api_options=APIOptions(
            ...         embedding_api_key=my_table_apikey_provider,
            ...         timeout_options=my_table_timeouts,
            ...     ),
            ... )
            >>>
            >>> # Create another 'Database' with a different auth token
            >>> # (for get_database, the 'token=' shorthand shown above does the same):
            >>> my_other_database = my_client.get_database(
            ...     "https://...",
            ...     spawn_api_options=APIOptions(
            ...         token="my-token-2",
            ...     ),
            ... )
            >>>
            >>> # Spawn a collection from a database and set it to use
            >>> # another token and a different policy with Decimals:
            >>> my_other_table = my_database.get_collection(
            ...     "my_other_table",
            ...     spawn_api_options=APIOptions(
            ...         token="my-token-3",
            ...         serdes_options=SerdesOptions(
            ...             use_decimals_in_collections=True,
            ...         )
            ...     ),
            ... )
    """

    environment: str | UnsetType = _UNSET
    callers: Sequence[CallerType] | UnsetType = _UNSET
    database_additional_headers: dict[str, str | None] | UnsetType = _UNSET
    admin_additional_headers: dict[str, str | None] | UnsetType = _UNSET
    redacted_header_names: set[str] | UnsetType = _UNSET
    token: TokenProvider | UnsetType = _UNSET
    embedding_api_key: EmbeddingHeadersProvider | UnsetType = _UNSET
    reranking_api_key: RerankingHeadersProvider | UnsetType = _UNSET

    timeout_options: TimeoutOptions | UnsetType = _UNSET
    serdes_options: SerdesOptions | UnsetType = _UNSET
    data_api_url_options: DataAPIURLOptions | UnsetType = _UNSET
    dev_ops_api_url_options: DevOpsAPIURLOptions | UnsetType = _UNSET

    def __init__(
        self,
        *,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        database_additional_headers: dict[str, str | None] | UnsetType = _UNSET,
        admin_additional_headers: dict[str, str | None] | UnsetType = _UNSET,
        redacted_header_names: Iterable[str] | UnsetType = _UNSET,
        token: str | TokenProvider | UnsetType = _UNSET,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        timeout_options: TimeoutOptions | UnsetType = _UNSET,
        serdes_options: SerdesOptions | UnsetType = _UNSET,
        data_api_url_options: DataAPIURLOptions | UnsetType = _UNSET,
        dev_ops_api_url_options: DevOpsAPIURLOptions | UnsetType = _UNSET,
    ) -> None:
        # Special conversions and type coercions occur here
        self.environment = _UNSET
        self.callers = callers
        self.database_additional_headers = database_additional_headers
        self.admin_additional_headers = admin_additional_headers
        self.redacted_header_names = (
            _UNSET
            if isinstance(redacted_header_names, UnsetType)
            else set(redacted_header_names)
        )
        self.token = coerce_possible_token_provider(token)
        self.embedding_api_key = coerce_possible_embedding_headers_provider(
            embedding_api_key,
        )
        self.reranking_api_key = coerce_possible_reranking_headers_provider(
            reranking_api_key,
        )
        self.timeout_options = timeout_options
        self.serdes_options = serdes_options
        self.data_api_url_options = data_api_url_options
        self.dev_ops_api_url_options = dev_ops_api_url_options

    def __repr__(self) -> str:
        # special items
        _admin_additional_headers: dict[str, str | None] | UnsetType
        _redacted_header_names = (
            set()
            if isinstance(self.redacted_header_names, UnsetType)
            else self.redacted_header_names
        )
        if not isinstance(self.admin_additional_headers, UnsetType):
            _admin_additional_headers = {
                k: v if k not in _redacted_header_names else FIXED_SECRET_PLACEHOLDER
                for k, v in self.admin_additional_headers.items()
            }
        else:
            _admin_additional_headers = _UNSET
        _database_additional_headers: dict[str, str | None] | UnsetType
        if not isinstance(self.database_additional_headers, UnsetType):
            _database_additional_headers = {
                k: v if k not in _redacted_header_names else FIXED_SECRET_PLACEHOLDER
                for k, v in self.database_additional_headers.items()
            }
        else:
            _database_additional_headers = _UNSET
        _token_desc: str | None
        if not isinstance(self.token, UnsetType) and self.token:
            _token_desc = f"token={self.token}"
        else:
            _token_desc = None

        non_unset_pieces = [
            pc
            for pc in (
                None
                if isinstance(self.callers, UnsetType)
                else f"callers={self.callers}",
                None
                if isinstance(_database_additional_headers, UnsetType)
                else f"database_additional_headers={_database_additional_headers}",
                None
                if isinstance(_admin_additional_headers, UnsetType)
                else f"admin_additional_headers={_admin_additional_headers}",
                None
                if isinstance(self.redacted_header_names, UnsetType)
                else f"redacted_header_names={self.redacted_header_names}",
                _token_desc,
                None
                if isinstance(self.embedding_api_key, UnsetType)
                else f"embedding_api_key={self.embedding_api_key}",
                None
                if isinstance(self.reranking_api_key, UnsetType)
                else f"reranking_api_key={self.reranking_api_key}",
                None
                if isinstance(self.timeout_options, UnsetType)
                else f"timeout_options={self.timeout_options}",
                None
                if isinstance(self.serdes_options, UnsetType)
                else f"serdes_options={self.serdes_options}",
                None
                if isinstance(self.data_api_url_options, UnsetType)
                else f"data_api_url_options={self.data_api_url_options}",
                None
                if isinstance(self.dev_ops_api_url_options, UnsetType)
                else f"dev_ops_api_url_options={self.dev_ops_api_url_options}",
            )
            if pc is not None
        ]
        inner_desc = ", ".join(non_unset_pieces)
        return f"{self.__class__.__name__}({inner_desc})"


@dataclass
class FullAPIOptions(APIOptions):
    """
    This class represents all settings that can be configured for how astrapy
    interacts with the API (both Data API and DevOps API). Each object in the
    abstraction hierarchy (DataAPIClient, Database, Table, Collection, ...)
    has a full set of these options that determine how it behaves when performing
    actions toward the API.

    This is the "full" version of the class, with the guarantee that all of its members
    have defined values. As such, this is what classes such as DataAPIClient, Database,
    Table, Collection and so on have as their `.api_options` attribute -- as opposed
    to the (non-full) `APIOptions` counterpart class: the latter admits "unset"
    attributes and is used to override specific settings. Please refer to the
    documentation for the `APIOptions` class for more details on how to customize
    the objects' behavior.

    Attributes:
        environment: an identifier for the environment for the Data API. This can
            describe an Astra DB environment (such as the default of "prod"), or
            a self-deployed setup (such as "dse" or "hcd"). This setting cannot be
            overridden through customization: it can only be provided when creating
            the DataAPIClient top object in the abstraction hierarchy.
        callers: an iterable of "caller identities" to be used in identifying the
            caller, through the User-Agent header, when issuing requests to the
            Data API. Each caller identity is a `(name, version)` 2-item tuple whose
            elements can be strings or None.
        database_additional_headers: free-form dictionary of additional headers to
            employ when issuing requests to the Data API from Database, Table and
            Collection classes. Passing a key with a value of None means that a certain
            header is suppressed when issuing the request.
        admin_additional_headers: free-form dictionary of additional headers to
            employ when issuing requests to both the Data API and the DevOps API
            from AstraDBAdmin, AstraDBDatabaseAdmin and DataAPIDatabaseAdmin classes.
            Passing a key with a value of None means that a certain header is
            suppressed when issuing the request.
        redacted_header_names: A set of (case-insensitive) strings denoting the headers
            that contain secrets, thus are to be masked when logging request details.
        token: an instance of TokenProvider to provide authentication to requests.
            Passing a string, or None, to this constructor parameter will get it
            automatically converted into the appropriate TokenProvider object.
            Depending on the target (Data API or DevOps API), this one attribute
            is encoded in the request appropriately.
        embedding_api_key: an instance of EmbeddingHeadersProvider should it be needed
            for vectorize-related data operations (used by Tables and Collections).
            Passing a string, or None, to this constructor parameter will get it
            automatically converted into the appropriate EmbeddingHeadersProvider
            object.
        reranking_api_key: an instance of RerankingHeadersProvider should it be needed
            for reranking-related data operations (used by Tables and Collections).
            Passing a string, or None, to this constructor parameter will get it
            automatically converted into the appropriate RerankingHeadersProvider
            object.
        timeout_options: an instance of `TimeoutOptions` (see) to control the timeout
            behavior for the various kinds of operations involving the Data/DevOps API.
        serdes_options: an instance of `SerdesOptions` (see) to customize the
            serializing/deserializing behavior related to writing to and reading from
            tables and collections.
        data_api_url_options: an instance of `DataAPIURLOptions` (see) to customize
            the full URL used to reach the Data API (customizing this setting
            is rarely needed).
        dev_ops_api_url_options: an instance of `DevOpsAPIURLOptions` (see) to
            customize the URL used to reach the DevOps API (customizing this setting
            is rarely needed; relevant only for Astra DB environments).
    """

    environment: str
    callers: Sequence[CallerType]
    database_additional_headers: dict[str, str | None]
    admin_additional_headers: dict[str, str | None]
    redacted_header_names: set[str]
    token: TokenProvider
    embedding_api_key: EmbeddingHeadersProvider
    reranking_api_key: RerankingHeadersProvider

    timeout_options: FullTimeoutOptions
    serdes_options: FullSerdesOptions
    data_api_url_options: FullDataAPIURLOptions
    dev_ops_api_url_options: FullDevOpsAPIURLOptions

    def __init__(
        self,
        *,
        environment: str,
        callers: Sequence[CallerType],
        database_additional_headers: dict[str, str | None],
        admin_additional_headers: dict[str, str | None],
        redacted_header_names: set[str],
        token: str | TokenProvider,
        embedding_api_key: str | EmbeddingHeadersProvider,
        reranking_api_key: str | RerankingHeadersProvider,
        timeout_options: FullTimeoutOptions,
        serdes_options: FullSerdesOptions,
        data_api_url_options: FullDataAPIURLOptions,
        dev_ops_api_url_options: FullDevOpsAPIURLOptions,
    ) -> None:
        APIOptions.__init__(
            self,
            callers=callers,
            database_additional_headers=database_additional_headers,
            admin_additional_headers=admin_additional_headers,
            redacted_header_names=redacted_header_names,
            token=token,
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            timeout_options=timeout_options,
            serdes_options=serdes_options,
            data_api_url_options=data_api_url_options,
            dev_ops_api_url_options=dev_ops_api_url_options,
        )
        self.environment = environment

    def __repr__(self) -> str:
        # special items
        _token_desc: str | None
        if self.token:
            _token_desc = f"token={self.token}"
        else:
            _token_desc = None

        non_unset_pieces = [
            pc
            for pc in (
                None
                if self.environment == Environment.PROD
                else f"environment={self.environment}",
                _token_desc,
                f"embedding_api_key={self.embedding_api_key}"
                if self.embedding_api_key
                else None,
                f"reranking_api_key={self.reranking_api_key}"
                if self.reranking_api_key
                else None,
                "...",
            )
            if pc is not None
        ]
        inner_desc = ", ".join(non_unset_pieces)
        return f"{self.__class__.__name__}({inner_desc})"

    def with_override(self, other: APIOptions | None | UnsetType) -> FullAPIOptions:
        """
        Given an "overriding" set of options, possibly not defined in all its
        attributes, apply the override logic and return a new full options object.

        The override logic acts hierarchically, so as to deal with attributes that
        are, in turn, options object of one type of another.

        The override logic is such that defined attributes completely replace the
        pre-existing ones, except for the case of `database_additional_headers`,
        `admin_additional_headers` and `redacted_header_names`, in which cases
        merging takes place.

        Args:
            other: a not-necessarily-fully-specified options object. All its defined
                settings take precedence.
        """

        if isinstance(other, UnsetType) or other is None:
            return self

        database_additional_headers: dict[str, str | None]
        admin_additional_headers: dict[str, str | None]
        redacted_header_names: set[str]

        timeout_options: FullTimeoutOptions
        serdes_options: FullSerdesOptions
        data_api_url_options: FullDataAPIURLOptions
        dev_ops_api_url_options: FullDevOpsAPIURLOptions

        if isinstance(other.database_additional_headers, UnsetType):
            database_additional_headers = self.database_additional_headers
        else:
            database_additional_headers = {
                **self.database_additional_headers,
                **other.database_additional_headers,
            }
        if isinstance(other.admin_additional_headers, UnsetType):
            admin_additional_headers = self.admin_additional_headers
        else:
            admin_additional_headers = {
                **self.admin_additional_headers,
                **other.admin_additional_headers,
            }
        if isinstance(other.redacted_header_names, UnsetType):
            redacted_header_names = self.redacted_header_names
        else:
            redacted_header_names = (
                self.redacted_header_names | other.redacted_header_names
            )

        if isinstance(other.timeout_options, TimeoutOptions):
            timeout_options = self.timeout_options.with_override(other.timeout_options)
        else:
            timeout_options = self.timeout_options
        if isinstance(other.serdes_options, SerdesOptions):
            serdes_options = self.serdes_options.with_override(other.serdes_options)
        else:
            serdes_options = self.serdes_options
        if isinstance(other.data_api_url_options, DataAPIURLOptions):
            data_api_url_options = self.data_api_url_options.with_override(
                other.data_api_url_options
            )
        else:
            data_api_url_options = self.data_api_url_options
        if isinstance(other.dev_ops_api_url_options, DevOpsAPIURLOptions):
            dev_ops_api_url_options = self.dev_ops_api_url_options.with_override(
                other.dev_ops_api_url_options
            )
        else:
            dev_ops_api_url_options = self.dev_ops_api_url_options

        return FullAPIOptions(
            environment=(
                other.environment
                if not isinstance(other.environment, UnsetType)
                else self.environment
            ),
            callers=(
                other.callers
                if not isinstance(other.callers, UnsetType)
                else self.callers
            ),
            database_additional_headers=database_additional_headers,
            admin_additional_headers=admin_additional_headers,
            redacted_header_names=redacted_header_names,
            token=other.token if not isinstance(other.token, UnsetType) else self.token,
            embedding_api_key=(
                other.embedding_api_key
                if not isinstance(other.embedding_api_key, UnsetType)
                else self.embedding_api_key
            ),
            reranking_api_key=(
                other.reranking_api_key
                if not isinstance(other.reranking_api_key, UnsetType)
                else self.reranking_api_key
            ),
            timeout_options=timeout_options,
            serdes_options=serdes_options,
            data_api_url_options=data_api_url_options,
            dev_ops_api_url_options=dev_ops_api_url_options,
        )


defaultTimeoutOptions = FullTimeoutOptions(
    request_timeout_ms=DEFAULT_REQUEST_TIMEOUT_MS,
    general_method_timeout_ms=DEFAULT_GENERAL_METHOD_TIMEOUT_MS,
    collection_admin_timeout_ms=DEFAULT_COLLECTION_ADMIN_TIMEOUT_MS,
    table_admin_timeout_ms=DEFAULT_TABLE_ADMIN_TIMEOUT_MS,
    database_admin_timeout_ms=DEFAULT_DATABASE_ADMIN_TIMEOUT_MS,
    keyspace_admin_timeout_ms=DEFAULT_KEYSPACE_ADMIN_TIMEOUT_MS,
)
defaultSerdesOptions = FullSerdesOptions(
    binary_encode_vectors=DEFAULT_BINARY_ENCODE_VECTORS,
    custom_datatypes_in_reading=DEFAULT_CUSTOM_DATATYPES_IN_READING,
    unroll_iterables_to_lists=DEFAULT_UNROLL_ITERABLES_TO_LISTS,
    use_decimals_in_collections=DEFAULT_USE_DECIMALS_IN_COLLECTIONS,
    encode_maps_as_lists_in_tables=DEFAULT_ENCODE_MAPS_AS_LISTS_IN_TABLES,
    accept_naive_datetimes=DEFAULT_ACCEPT_NAIVE_DATETIMES,
    datetime_tzinfo=DEFAULT_DATETIME_TZINFO,
)


def defaultAPIOptions(environment: str) -> FullAPIOptions:
    """
    Return the default APIOptions object for a given environment,
    based on 'grand defaults' hardcoded in astrapy.
    """

    defaultDataAPIURLOptions = FullDataAPIURLOptions(
        api_path=API_PATH_ENV_MAP[environment],
        api_version=API_VERSION_ENV_MAP[environment],
    )
    defaultDevOpsAPIURLOptions: FullDevOpsAPIURLOptions
    if environment in Environment.astra_db_values:
        defaultDevOpsAPIURLOptions = FullDevOpsAPIURLOptions(
            dev_ops_url=DEV_OPS_URL_ENV_MAP[environment],
            dev_ops_api_version=DEV_OPS_VERSION_ENV_MAP[environment],
        )
    else:
        defaultDevOpsAPIURLOptions = FullDevOpsAPIURLOptions(
            dev_ops_url="never used",
            dev_ops_api_version=None,
        )
    return FullAPIOptions(
        environment=environment,
        callers=[],
        database_additional_headers={},
        admin_additional_headers={},
        redacted_header_names=set(),
        token=StaticTokenProvider(None),
        embedding_api_key=EmbeddingAPIKeyHeaderProvider(None),
        reranking_api_key=RerankingAPIKeyHeaderProvider(None),
        timeout_options=defaultTimeoutOptions,
        serdes_options=defaultSerdesOptions,
        data_api_url_options=defaultDataAPIURLOptions,
        dev_ops_api_url_options=defaultDevOpsAPIURLOptions,
    )
