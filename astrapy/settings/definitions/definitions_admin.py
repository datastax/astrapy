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

from astrapy.settings.defaults import (
    DATA_API_ENVIRONMENT_CASSANDRA,
    DATA_API_ENVIRONMENT_DEV,
    DATA_API_ENVIRONMENT_DSE,
    DATA_API_ENVIRONMENT_HCD,
    DATA_API_ENVIRONMENT_OTHER,
    DATA_API_ENVIRONMENT_PROD,
    DATA_API_ENVIRONMENT_TEST,
)
from astrapy.utils.str_enum import StrEnum


class Environment:
    """
    Admitted values for `environment` property,
    denoting the targeted API deployment type.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    PROD = DATA_API_ENVIRONMENT_PROD
    DEV = DATA_API_ENVIRONMENT_DEV
    TEST = DATA_API_ENVIRONMENT_TEST
    DSE = DATA_API_ENVIRONMENT_DSE
    HCD = DATA_API_ENVIRONMENT_HCD
    CASSANDRA = DATA_API_ENVIRONMENT_CASSANDRA
    OTHER = DATA_API_ENVIRONMENT_OTHER

    values = {PROD, DEV, TEST, DSE, HCD, CASSANDRA, OTHER}
    astra_db_values = {PROD, DEV, TEST}


class ModelStatus(StrEnum):
    """ """

    ALL = ""
    SUPPORTED = "SUPPORTED"
    DEPRECATED = "DEPRECATED"
    END_OF_LIFE = "END_OF_LIFE"
