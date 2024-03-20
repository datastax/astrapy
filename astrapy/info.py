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

from typing import Any, Dict, Optional
from dataclasses import dataclass


@dataclass
class DatabaseInfo:
    """
    Represents the identifying information for a database,
    including the region the connection is established to.

    Attributes:
        id: the database ID.
        region: the ID of the region through which the connection to DB is done.
        namespace: the namespace this DB is set to work with.
        name: the database name. Not necessarily unique: there can be multiple
            databases with the same name.
        environment: a label such as "prod", "dev" or "test"
        raw_info: the full response from the DevOPS API call to get this info.

    Note:
        Most members of this object can be None. This happens when errors occur
        during DevOps API calls and usually signals that the Data API server
        is a deploy where concepts such as "region" or "database ID" do not apply.

    Note:
        The `raw_info` dictionary usually has a `region` key describing
        the default region as configured in the database, which does not
        necessarily (for multi-region databases) match the region through
        which the connection is established: the latter is the one specified
        by the "api endpoint" used for connecting. In other words, for multi-region
        databases it is possible that
            database_info.region != database_info.raw_info["region"]
    """

    id: str
    region: str
    namespace: str
    name: str
    environment: str
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
