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

from typing import Any, Callable, List, Optional, Tuple

import pytest

import os
import time

from astrapy import DataAPIClient


NAMESPACE_POLL_SLEEP_TIME = 2
NAMESPACE_TIMEOUT = 20


def admin_test_envs_tokens() -> List[Any]:
    """
    This actually returns a List of `_pytest.mark.structures.ParameterSet` instances,
    each wrapping a Tuple[str, Optional[str]] = (env, token)
    """
    envs_tokens: List[Any] = []
    for env in ["prod", "dev"]:
        varname = f"{env.upper()}_ADMIN_TEST_ASTRA_DB_APPLICATION_TOKEN"
        markers = []
        pair: Tuple[str, Optional[str]]
        if varname in os.environ:
            pair = (env, os.environ[varname])
        else:
            pair = (env, None)
            markers.append(pytest.mark.skip(reason=f"{env} token not available"))
        envs_tokens.append(pytest.param(pair, marks=markers))

    return envs_tokens


def wait_until_true(
    poll_interval: int, max_seconds: int, condition: Callable[..., bool]
) -> None:
    ini_time = time.time()
    while not condition():
        if time.time() - ini_time > max_seconds:
            raise ValueError("Timed out on condition.")
        time.sleep(poll_interval)


class TestAdmin:
    @pytest.mark.parametrize("env_token", admin_test_envs_tokens())
    @pytest.mark.describe("test of the full tour with AstraDBDatabaseAdmin")
    def test_astra_db_database_admin(self, env_token: Tuple[str, str]) -> None:
        """
        Test plan (it has to be a single giant test to use one DB throughout):
        - create client -> get_admin
        - create a db (wait)
        - with the AstraDBDatabaseAdmin:
            - info
            - list namespaces, check
            - create 2 namespaces (wait, nonwait)
            - list namespaces, check
            - get_database -> create_collection/list_collection_names
            - get_async_database, check if == previous
            - drop namespaces (wait, nonwait)
            - list namespaces, check
            - drop database (wait)
        - check DB not existings
        """
        env, token = env_token
        db_name = f"test_database_{env}"
        db_provider = os.environ[f"{env.upper()}_ADMIN_TEST_ASTRA_DB_PROVIDER"]
        db_region = os.environ[f"{env.upper()}_ADMIN_TEST_ASTRA_DB_REGION"]

        client: DataAPIClient
        if env == "prod":
            client = DataAPIClient(token)
        else:
            client = DataAPIClient(token, environment=env)
        admin = client.get_admin()
        db_admin = admin.create_database(
            name=db_name,
            wait_until_active=True,
            cloud_provider=db_provider,
            region=db_region,
        )

        created_db_id = db_admin.id
        assert db_admin.info().id == created_db_id

        namespaces1 = set(db_admin.list_namespaces())
        assert namespaces1 == {"default_keyspace"}

        w_create_ns_response = db_admin.create_namespace(
            "waited_ns",
            wait_until_active=True,
        )
        assert w_create_ns_response == {"ok": 1}

        nw_create_ns_response = db_admin.create_namespace(
            "nonwaited_ns",
            wait_until_active=False,
        )
        assert nw_create_ns_response == {"ok": 1}
        wait_until_true(
            max_seconds=NAMESPACE_POLL_SLEEP_TIME,
            poll_interval=NAMESPACE_TIMEOUT,
            condition=lambda: "nonwaited_ns" in db_admin.list_namespaces(),
        )

        namespaces3 = set(db_admin.list_namespaces())
        assert namespaces3 - namespaces1 == {"waited_ns", "nonwaited_ns"}

        db = db_admin.get_database()
        db.create_collection("canary_coll")
        assert "canary_coll" in db.list_collection_names()
        assert db_admin.get_async_database().to_sync() == db

        w_drop_ns_response = db_admin.drop_namespace(
            "waited_ns",
            wait_until_active=True,
        )
        assert w_drop_ns_response == {"ok": 1}

        nw_drop_ns_response = db_admin.drop_namespace(
            "nonwaited_ns",
            wait_until_active=False,
        )
        assert nw_drop_ns_response == {"ok": 1}
        wait_until_true(
            max_seconds=NAMESPACE_POLL_SLEEP_TIME,
            poll_interval=NAMESPACE_TIMEOUT,
            condition=lambda: "nonwaited_ns" not in db_admin.list_namespaces(),
        )

        namespaces1b = set(db_admin.list_namespaces())
        assert namespaces1b == namespaces1

        db_drop_response = db_admin.drop()
        assert db_drop_response == {"ok": 1}

        db_ids = {db.id for db in admin.list_databases()}
        assert created_db_id not in db_ids
