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

from testcontainers.compose import DockerCompose

from astrapy.authentication import (
    StaticTokenProvider,
    TokenProvider,
    UsernamePasswordTokenProvider,
)
from astrapy.settings.defaults import DEFAULT_ASTRA_DB_KEYSPACE

DOCKER_COMPOSE_SLEEP_TIME_SECONDS = 20
DEFAULT_SECONDARY_KEYSPACE = "secondary_keyspace"

base_dir = os.path.abspath(os.path.dirname(__file__))
docker_compose_filepath = os.path.join(base_dir, "hcd_compose")


IS_ASTRA_DB: bool
DOCKER_COMPOSE_LOCAL_DATA_API: bool
SECONDARY_KEYSPACE: str
ASTRA_DB_API_ENDPOINT: str | None = None
ASTRA_DB_APPLICATION_TOKEN: str | None = None
ASTRA_DB_KEYSPACE: str | None = None
LOCAL_DATA_API_USERNAME: str | None = None
LOCAL_DATA_API_PASSWORD: str | None = None
LOCAL_DATA_API_ENDPOINT: str | None = None
LOCAL_CASSANDRA_CONTACT_POINT: str | None = None
LOCAL_CASSANDRA_PORT: str | None = None
LOCAL_DATA_API_KEYSPACE: str = DEFAULT_ASTRA_DB_KEYSPACE
RUN_SHARED_SECRET_VECTORIZE_TESTS: bool = True

ASTRA_DB_TOKEN_PROVIDER: TokenProvider | None = None
LOCAL_DATA_API_TOKEN_PROVIDER: TokenProvider | None = None


def extended_booleanize_env(env_var_name: str, default: bool = False) -> bool:
    """
    Extended booleanize for environment variables.
    Accepts also "1"/"0", "yes"/"no", "true"/"false" (case insensitive).
    If the variable is not set, returns the default value.
    """
    value = os.environ.get(env_var_name)
    if value is None or value == "":
        return default
    # test if any integer:
    try:
        int_value = int(value)
        return int_value != 0
    except ValueError:
        pass
    value_lower = value.lower()
    if value_lower in ("yes", "true", "y", "t"):
        return True
    elif value_lower in ("no", "false", "n", "f"):
        return False
    else:
        raise ValueError(
            f"Invalid value for {env_var_name}: {value}. Expected an integer "
            "or one of 'yes', 'no', 'true', 'false', 'y', 'n', 't', 'f' (case insensitive)."
        )


# basic settings about which DB/API to use
if "LOCAL_DATA_API_ENDPOINT" in os.environ:
    IS_ASTRA_DB = False
    DOCKER_COMPOSE_LOCAL_DATA_API = False
    LOCAL_DATA_API_USERNAME = os.environ.get("LOCAL_DATA_API_USERNAME")
    LOCAL_DATA_API_PASSWORD = os.environ.get("LOCAL_DATA_API_PASSWORD")
    LOCAL_DATA_API_ENDPOINT = os.environ["LOCAL_DATA_API_ENDPOINT"]
    LOCAL_CASSANDRA_PORT = os.environ.get("LOCAL_CASSANDRA_PORT")
    LOCAL_CASSANDRA_CONTACT_POINT = os.environ["LOCAL_CASSANDRA_CONTACT_POINT"]
    LOCAL_DATA_API_KEYSPACE = os.environ.get(
        "LOCAL_DATA_API_KEYSPACE", DEFAULT_ASTRA_DB_KEYSPACE
    )
    # no reason not to use it
    SECONDARY_KEYSPACE = os.environ.get(
        "LOCAL_DATA_API_SECONDARY_KEYSPACE", DEFAULT_SECONDARY_KEYSPACE
    )
elif "DOCKER_COMPOSE_LOCAL_DATA_API" in os.environ:
    IS_ASTRA_DB = False
    DOCKER_COMPOSE_LOCAL_DATA_API = True
    LOCAL_DATA_API_USERNAME = "cassandra"
    LOCAL_DATA_API_PASSWORD = "cassandra"
    LOCAL_DATA_API_ENDPOINT = "http://localhost:8181"
    LOCAL_CASSANDRA_CONTACT_POINT = "127.0.0.1"
    LOCAL_DATA_API_KEYSPACE = os.environ.get(
        "LOCAL_DATA_API_KEYSPACE", DEFAULT_ASTRA_DB_KEYSPACE
    )
    # no reason not to use it
    SECONDARY_KEYSPACE = os.environ.get(
        "LOCAL_DATA_API_SECONDARY_KEYSPACE", DEFAULT_SECONDARY_KEYSPACE
    )
elif "ASTRA_DB_API_ENDPOINT" in os.environ:
    IS_ASTRA_DB = True
    DOCKER_COMPOSE_LOCAL_DATA_API = False
    SECONDARY_KEYSPACE = os.environ.get(
        "ASTRA_DB_SECONDARY_KEYSPACE", DEFAULT_SECONDARY_KEYSPACE
    )
    ASTRA_DB_API_ENDPOINT = os.environ["ASTRA_DB_API_ENDPOINT"]
    ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
    ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_ASTRA_DB_KEYSPACE)
else:
    raise ValueError("No credentials.")

RUN_SHARED_SECRET_VECTORIZE_TESTS = extended_booleanize_env(
    "RUN_SHARED_SECRET_VECTORIZE_TESTS", default=True
)

# token provider setup
if IS_ASTRA_DB:
    ASTRA_DB_TOKEN_PROVIDER = StaticTokenProvider(ASTRA_DB_APPLICATION_TOKEN)
else:
    # there must be a user/pwd pair
    if LOCAL_DATA_API_USERNAME and LOCAL_DATA_API_PASSWORD:
        LOCAL_DATA_API_TOKEN_PROVIDER = UsernamePasswordTokenProvider(
            username=LOCAL_DATA_API_USERNAME,
            password=LOCAL_DATA_API_PASSWORD,
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
            def docker_compose_command(self) -> list[str]:
                docker_compose_cmd = [
                    os.environ.get("DOCKER_COMMAND_NAME", "docker"),
                    "compose",
                ]
                for file in self.compose_file_names:
                    docker_compose_cmd += ["-f", file]
                if self.env_file:
                    docker_compose_cmd += ["--env-file", self.env_file]
                return docker_compose_cmd

        compose = RedefineCommandDockerCompose(filepath=docker_compose_filepath)
        compose.start()
        time.sleep(DOCKER_COMPOSE_SLEEP_TIME_SECONDS)
        is_docker_compose_started = True


# admin-test values for Astra DB
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

# misc variables
HEADER_EMBEDDING_API_KEY_OPENAI = os.environ.get("HEADER_EMBEDDING_API_KEY_OPENAI")
HEADER_RERANKING_API_KEY_NVIDIA = os.environ.get("HEADER_RERANKING_API_KEY_NVIDIA")
