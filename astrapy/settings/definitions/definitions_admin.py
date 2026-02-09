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

from astrapy.utils.str_enum import StrEnum


class Environment:
    """
    Admitted values for `environment` property,
    denoting the targeted API deployment type.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    PROD = "prod"
    DEV = "dev"
    TEST = "test"
    DSE = "dse"
    HCD = "hcd"
    CASSANDRA = "cassandra"
    OTHER = "other"

    values = {PROD, DEV, TEST, DSE, HCD, CASSANDRA, OTHER}
    astra_db_values = {PROD, DEV, TEST}


class ModelStatus(StrEnum):
    """
    Admitted values for the status of a (reranking/embedding) model,
    as returned by the corresponding Data API query.
    """

    ALL = ""
    SUPPORTED = "SUPPORTED"
    DEPRECATED = "DEPRECATED"
    END_OF_LIFE = "END_OF_LIFE"


class DatabaseStatus(StrEnum):
    """
    Admitted values for the status of a database as returned by Astra DB.

    To avoid the risk of a newly-introduced status (by the DevOps API)
    breaking the clients, this enum is used only for reference and no coercion
    from server-received status strings is enforced.
    """

    ACTIVE = "ACTIVE"
    ASSOCIATING = "ASSOCIATING"
    DECOMMISSIONING = "DECOMMISSIONING"
    DEGRADED = "DEGRADED"
    ERROR = "ERROR"
    HIBERNATED = "HIBERNATED"
    HIBERNATING = "HIBERNATING"
    INITIALIZING = "INITIALIZING"
    MAINTENANCE = "MAINTENANCE"
    PARKED = "PARKED"
    PARKING = "PARKING"
    PENDING = "PENDING"
    PREPARED = "PREPARED"
    PREPARING = "PREPARING"
    RESIZING = "RESIZING"
    RESUMING = "RESUMING"
    SYNCHRONIZING = "SYNCHRONIZING"
    TERMINATED = "TERMINATED"
    TERMINATING = "TERMINATING"
    UNKNOWN = "UNKNOWN"
    UNPARKING = "UNPARKING"
