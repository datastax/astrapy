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

"""
Bottleneck entrypoint for reading os.environ and exposing its contents as
(normalized) regular variables.
Except for the vectorize information, which for the time being passes as os.environ.
"""

import os
from typing import Optional

IS_ASTRA_DB: bool
DOCKER_COMPOSE_LOCAL_DATA_API: bool
SECONDARY_NAMESPACE: Optional[str] = None
ASTRA_DB_API_ENDPOINT: Optional[str] = None
ASTRA_DB_APPLICATION_TOKEN: Optional[str] = None
ASTRA_DB_KEYSPACE: Optional[str] = None
LOCAL_DATA_API_USERNAME: Optional[str] = None
LOCAL_DATA_API_PASSWORD: Optional[str] = None
LOCAL_DATA_API_APPLICATION_TOKEN: Optional[str] = None
LOCAL_DATA_API_ENDPOINT: Optional[str] = None
LOCAL_DATA_API_KEYSPACE: Optional[str] = None

# idiomatic-related settings
if "LOCAL_DATA_API_ENDPOINT" in os.environ:
    IS_ASTRA_DB = False
    DOCKER_COMPOSE_LOCAL_DATA_API = False
    LOCAL_DATA_API_USERNAME = os.environ.get("LOCAL_DATA_API_USERNAME")
    LOCAL_DATA_API_PASSWORD = os.environ.get("LOCAL_DATA_API_PASSWORD")
    LOCAL_DATA_API_APPLICATION_TOKEN = os.environ.get(
        "LOCAL_DATA_API_APPLICATION_TOKEN"
    )
    LOCAL_DATA_API_ENDPOINT = os.environ["LOCAL_DATA_API_ENDPOINT"]
    LOCAL_DATA_API_KEYSPACE = os.environ.get("LOCAL_DATA_API_KEYSPACE")
    # no reason not to use it
    SECONDARY_NAMESPACE = os.environ.get(
        "LOCAL_DATA_API_SECONDARY_KEYSPACE", "alternate_keyspace"
    )
elif "DOCKER_COMPOSE_LOCAL_DATA_API" in os.environ:
    IS_ASTRA_DB = False
    DOCKER_COMPOSE_LOCAL_DATA_API = True
    LOCAL_DATA_API_USERNAME = "cassandra"
    LOCAL_DATA_API_PASSWORD = "cassandra"
    LOCAL_DATA_API_ENDPOINT = "http://localhost:8181"
    LOCAL_DATA_API_KEYSPACE = os.environ.get("LOCAL_DATA_API_KEYSPACE")
    # no reason not to use it
    SECONDARY_NAMESPACE = os.environ.get(
        "LOCAL_DATA_API_SECONDARY_KEYSPACE", "alternate_keyspace"
    )
elif "ASTRA_DB_API_ENDPOINT" in os.environ:
    IS_ASTRA_DB = True
    DOCKER_COMPOSE_LOCAL_DATA_API = False
    SECONDARY_NAMESPACE = os.environ.get("ASTRA_DB_SECONDARY_KEYSPACE")
    ASTRA_DB_API_ENDPOINT = os.environ["ASTRA_DB_API_ENDPOINT"]
    ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
    ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE")
else:
    raise ValueError("No credentials.")

# Idomatic admin test flag
DO_IDIOMATIC_ADMIN_TESTS: bool
if "DO_IDIOMATIC_ADMIN_TESTS" in os.environ:
    _do_idiomatic_admin_tests = os.environ["DO_IDIOMATIC_ADMIN_TESTS"]
    if _do_idiomatic_admin_tests.strip():
        DO_IDIOMATIC_ADMIN_TESTS = int(_do_idiomatic_admin_tests) != 0
    else:
        DO_IDIOMATIC_ADMIN_TESTS = False
else:
    DO_IDIOMATIC_ADMIN_TESTS = False

ADMIN_ENV_LIST = ["prod", "dev"]
ADMIN_ENV_VARIABLE_MAP = {
    admin_env: {
        "token": os.environ.get(
            f"{admin_env.upper()}_ADMIN_TEST_ASTRA_DB_APPLICATION_TOKEN"
        ),
        "provider": os.environ.get(f"{admin_env.upper()}_ADMIN_TEST_ASTRA_DB_PROVIDER"),
        "region": os.environ.get(f"{admin_env.upper()}_ADMIN_TEST_ASTRA_DB_REGION"),
    }
    for admin_env in ADMIN_ENV_LIST
}

# core-specific (legacy) flags
TEST_SKIP_COLLECTION_DELETE: bool
if os.getenv("TEST_SKIP_COLLECTION_DELETE"):
    TEST_SKIP_COLLECTION_DELETE = int(os.environ["TEST_SKIP_COLLECTION_DELETE"]) != 0
else:
    TEST_SKIP_COLLECTION_DELETE = False

ASTRA_DB_OPS_APPLICATION_TOKEN = os.environ.get(
    "ASTRA_DB_OPS_APPLICATION_TOKEN",
    ASTRA_DB_APPLICATION_TOKEN or "no_token!",
)
ASTRA_DB_ID = os.environ.get("ASTRA_DB_ID", "")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE")
ASTRA_DB_REGION = os.environ.get("ASTRA_DB_REGION")
TEST_ASTRADBOPS = int(os.environ.get("TEST_ASTRADBOPS", "0")) != 0
