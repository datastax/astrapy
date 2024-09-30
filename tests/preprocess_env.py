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

from __future__ import annotations

import os
import time
from typing import List, Optional

from testcontainers.compose import DockerCompose

from astrapy.authentication import (
    StaticTokenProvider,
    TokenProvider,
    UsernamePasswordTokenProvider,
)

DOCKER_COMPOSE_SLEEP_TIME_SECONDS = 20

base_dir = os.path.abspath(os.path.dirname(__file__))
docker_compose_filepath = os.path.join(base_dir, "hcd_compose")


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

ASTRA_DB_TOKEN_PROVIDER: Optional[TokenProvider] = None
LOCAL_DATA_API_TOKEN_PROVIDER: Optional[TokenProvider] = None

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

# token provider setup
if IS_ASTRA_DB:
    ASTRA_DB_TOKEN_PROVIDER = StaticTokenProvider(ASTRA_DB_APPLICATION_TOKEN)
else:
    # either token or user/pwd pair (the latter having precedence)
    if LOCAL_DATA_API_USERNAME and LOCAL_DATA_API_PASSWORD:
        LOCAL_DATA_API_TOKEN_PROVIDER = UsernamePasswordTokenProvider(
            username=LOCAL_DATA_API_USERNAME,
            password=LOCAL_DATA_API_PASSWORD,
        )
    elif LOCAL_DATA_API_APPLICATION_TOKEN:
        LOCAL_DATA_API_TOKEN_PROVIDER = StaticTokenProvider(
            LOCAL_DATA_API_APPLICATION_TOKEN
        )
    else:
        raise ValueError("No full authentication data for local Data API")


# Ensure docker compose, if needed, is started and ready before anything else
# (especially querying the findEmbeddingProviders)
# if "DOCKER_COMPOSE_LOCAL_DATA_API", must spin the whole environment:
# (it is started and not cleaned up at the moment: manual cleanup if needed)
is_docker_compose_started = False
if DOCKER_COMPOSE_LOCAL_DATA_API:
    if not is_docker_compose_started:
        """
        Note: this is a trick to invoke `docker compose` as opposed to `docker-compose`
        while using testcontainers < 4.
        This trick is only reliable with testcontainers >= 3.1, < 4.

        The reason is that `docker-compose` is not included in the `ubuntu-latest`
        Github runner starting August 2024 (in favour of `docker compose`).

        - More modern testcontainers would require python>=3.9.
        - Aliasing 'docker-compose' in the CI container is (more) brittle.
        - Pip installing a modern testcontainers in the CI yaml is 'inelegant'.
            (and would require a try/except here with different inits to keep compat.)

        So we ended up with this trick, which redefines the executable to invoke
        for the DockerCompose class of testcontainers.
        """

        class RedefineCommandDockerCompose(DockerCompose):
            def docker_compose_command(self) -> List[str]:
                docker_compose_cmd = ["docker", "compose"]
                for file in self.compose_file_names:
                    docker_compose_cmd += ["-f", file]
                if self.env_file:
                    docker_compose_cmd += ["--env-file", self.env_file]
                return docker_compose_cmd

        compose = RedefineCommandDockerCompose(filepath=docker_compose_filepath)
        compose.start()
        time.sleep(DOCKER_COMPOSE_SLEEP_TIME_SECONDS)
        is_docker_compose_started = True


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

ADMIN_ENV_LIST = ["prod", "dev", "test"]
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
