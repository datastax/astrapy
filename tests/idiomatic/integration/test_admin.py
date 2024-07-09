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

import time
from typing import Any, Awaitable, Callable, List, Optional, Tuple

import pytest

from astrapy import AsyncDatabase, DataAPIClient, Database
from astrapy.admin import API_ENDPOINT_TEMPLATE_MAP

from ..conftest import (
    ADMIN_ENV_LIST,
    ADMIN_ENV_VARIABLE_MAP,
    DO_IDIOMATIC_ADMIN_TESTS,
    IS_ASTRA_DB,
)

NAMESPACE_POLL_SLEEP_TIME = 2
NAMESPACE_TIMEOUT = 30
DATABASE_POLL_SLEEP_TIME = 10
DATABASE_TIMEOUT = 480
PRE_DROP_SAFETY_POLL_INTERVAL = 5
PRE_DROP_SAFETY_TIMEOUT = 120


def admin_test_envs_tokens() -> List[Any]:
    """
    This actually returns a List of `_pytest.mark.structures.ParameterSet` instances,
    each wrapping a Tuple[str, Optional[str]] = (env, token)
    """
    envs_tokens: List[Any] = []
    for admin_env in ADMIN_ENV_LIST:
        markers = []
        pair: Tuple[str, Optional[str]]
        if ADMIN_ENV_VARIABLE_MAP[admin_env]["token"]:
            pair = (admin_env, ADMIN_ENV_VARIABLE_MAP[admin_env]["token"])
        else:
            pair = (admin_env, None)
            markers.append(pytest.mark.skip(reason=f"{admin_env} token not available"))
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


async def await_until_true(
    poll_interval: int, max_seconds: int, acondition: Callable[..., Awaitable[bool]]
) -> None:
    ini_time = time.time()
    while not (await acondition()):
        if time.time() - ini_time > max_seconds:
            raise ValueError("Timed out on condition.")
        time.sleep(poll_interval)


@pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
@pytest.mark.skipif(not DO_IDIOMATIC_ADMIN_TESTS, reason="Admin tests are suppressed")
class TestAdmin:
    @pytest.mark.parametrize(
        "admin_env_token", admin_test_envs_tokens(), ids=ADMIN_ENV_LIST
    )
    @pytest.mark.describe("test of the full tour with AstraDBDatabaseAdmin, sync")
    def test_astra_db_database_admin_sync(
        self, admin_env_token: Tuple[str, str]
    ) -> None:
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
        admin_env, token = admin_env_token
        db_name = f"test_database_{admin_env}"
        db_provider = ADMIN_ENV_VARIABLE_MAP[admin_env]["provider"]
        db_region = ADMIN_ENV_VARIABLE_MAP[admin_env]["region"]

        # create client, get admin
        client: DataAPIClient
        if admin_env == "prod":
            client = DataAPIClient(token)
        else:
            client = DataAPIClient(token, environment=admin_env)
        admin = client.get_admin()

        # create a db (wait)
        db_admin = admin.create_database(
            name=db_name,
            namespace="custom_namespace",
            wait_until_active=True,
            cloud_provider=db_provider,
            region=db_region,
        )

        # info with the AstraDBDatabaseAdmin
        created_db_id = db_admin.id
        assert db_admin.info().id == created_db_id

        # list nss
        namespaces1 = set(db_admin.list_namespaces())
        assert namespaces1 == {"custom_namespace"}

        # create two namespaces
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
            poll_interval=NAMESPACE_POLL_SLEEP_TIME,
            max_seconds=NAMESPACE_TIMEOUT,
            condition=lambda: "nonwaited_ns" in db_admin.list_namespaces(),
        )

        namespaces3 = set(db_admin.list_namespaces())
        assert namespaces3 - namespaces1 == {"waited_ns", "nonwaited_ns"}

        # get db and use it
        db = db_admin.get_database()
        db.create_collection("canary_coll")
        assert "canary_coll" in db.list_collection_names()

        # check async db is the same
        assert db_admin.get_async_database().to_sync() == db

        # drop nss, wait, nonwait
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
            poll_interval=NAMESPACE_POLL_SLEEP_TIME,
            max_seconds=NAMESPACE_TIMEOUT,
            condition=lambda: "nonwaited_ns" not in db_admin.list_namespaces(),
        )

        # check nss after dropping two of them
        namespaces1b = set(db_admin.list_namespaces())
        assert namespaces1b == namespaces1

        # drop db and check. We wait a little due to "nontransactional cluster md"
        wait_until_true(
            poll_interval=PRE_DROP_SAFETY_POLL_INTERVAL,
            max_seconds=PRE_DROP_SAFETY_TIMEOUT,
            condition=lambda: db_admin.info().status == "ACTIVE",
        )
        db_drop_response = db_admin.drop()
        assert db_drop_response == {"ok": 1}

        db_ids = {db.id for db in admin.list_databases()}
        assert created_db_id not in db_ids

    @pytest.mark.parametrize(
        "admin_env_token", admin_test_envs_tokens(), ids=ADMIN_ENV_LIST
    )
    @pytest.mark.describe(
        "test of the full tour with AstraDBAdmin and client methods, sync"
    )
    def test_astra_db_admin_sync(self, admin_env_token: Tuple[str, str]) -> None:
        """
        Test plan (it has to be a single giant test to use the two DBs throughout):
        - create client -> get_admin
        - create two dbs (wait, nonwait)
        - list the two database ids, check
        - get check info on one such db through admin
        - with the client:
            4 get_dbs (a/sync, by id+region/api_endpoint), check if equal
            and test their list_collections
        - get_db_admin from the admin for one of the dbs
            - create ns
            - get_database -> list_collection_names
            - get_async_database and check == with above
        - drop dbs, (wait, nonwait)
        """
        admin_env, token = admin_env_token
        db_name_w = f"test_database_w_{admin_env}"
        db_name_nw = f"test_database_nw_{admin_env}"
        db_provider = ADMIN_ENV_VARIABLE_MAP[admin_env]["provider"]
        db_region = ADMIN_ENV_VARIABLE_MAP[admin_env]["region"]

        # create client and get admin
        client: DataAPIClient
        if admin_env == "prod":
            client = DataAPIClient(token)
        else:
            client = DataAPIClient(token, environment=admin_env)
        admin = client.get_admin()

        # create the two dbs
        db_admin_nw = admin.create_database(
            name=db_name_nw,
            wait_until_active=False,
            cloud_provider=db_provider,
            region=db_region,
        )
        created_db_id_nw = db_admin_nw.id
        db_admin_w = admin.create_database(
            name=db_name_w,
            wait_until_active=True,
            cloud_provider=db_provider,
            region=db_region,
        )
        created_db_id_w = db_admin_w.id

        def _waiter1() -> bool:
            db_ids = {db.id for db in admin.list_databases()}
            return created_db_id_nw in db_ids

        wait_until_true(
            poll_interval=DATABASE_POLL_SLEEP_TIME,
            max_seconds=DATABASE_TIMEOUT,
            condition=_waiter1,
        )

        # list, check ids
        db_ids = {db.id for db in admin.list_databases()}
        assert {created_db_id_nw, created_db_id_w} - db_ids == set()

        # get info through admin
        db_w_info = admin.database_info(created_db_id_w)
        assert db_w_info.id == created_db_id_w

        # get and compare dbs obtained by the client
        synthetic_api_endpoint = API_ENDPOINT_TEMPLATE_MAP[admin_env].format(
            database_id=created_db_id_w,
            region=db_region,
        )
        db_w_d = client.get_database(created_db_id_w)
        db_w_r = client.get_database(created_db_id_w, region=db_region)
        db_w_e = client.get_database(synthetic_api_endpoint)
        adb_w_d = client.get_async_database(created_db_id_w)
        adb_w_r = client.get_async_database(created_db_id_w, region=db_region)
        adb_w_e = client.get_async_database(synthetic_api_endpoint)
        assert isinstance(db_w_d.list_collection_names(), list)
        assert db_w_r == db_w_d
        assert db_w_e == db_w_d
        assert adb_w_d.to_sync() == db_w_d
        assert adb_w_r.to_sync() == db_w_d
        assert adb_w_e.to_sync() == db_w_d

        # get db admin from the admin and use it
        db_w_admin = admin.get_database_admin(created_db_id_w)
        db_w_admin.create_namespace("additional_namespace")
        db_w_from_admin = db_w_admin.get_database()
        assert isinstance(db_w_from_admin.list_collection_names(), list)
        adb_w_from_admin = db_w_admin.get_async_database()
        assert adb_w_from_admin.to_sync() == db_w_from_admin

        # drop databases: the w one through the admin, the nw using its db-admin
        #   (this covers most cases if combined with the
        #   (w, using db-admin) of test_astra_db_database_admin)
        assert db_admin_nw == admin.get_database_admin(created_db_id_nw)
        # drop db and check. We wait a little due to "nontransactional cluster md"
        wait_until_true(
            poll_interval=PRE_DROP_SAFETY_POLL_INTERVAL,
            max_seconds=PRE_DROP_SAFETY_TIMEOUT,
            condition=lambda: db_admin_nw.info().status == "ACTIVE",
        )
        wait_until_true(
            poll_interval=PRE_DROP_SAFETY_POLL_INTERVAL,
            max_seconds=PRE_DROP_SAFETY_TIMEOUT,
            condition=lambda: db_admin_w.info().status == "ACTIVE",
        )
        drop_nw_response = db_admin_nw.drop(wait_until_active=False)
        assert drop_nw_response == {"ok": 1}
        drop_w_response = admin.drop_database(created_db_id_w)
        assert drop_w_response == {"ok": 1}

        def _waiter2() -> bool:
            db_ids = {db.id for db in admin.list_databases()}
            return created_db_id_nw not in db_ids

        wait_until_true(
            poll_interval=DATABASE_POLL_SLEEP_TIME,
            max_seconds=DATABASE_TIMEOUT,
            condition=_waiter2,
        )

    @pytest.mark.parametrize(
        "admin_env_token", admin_test_envs_tokens(), ids=ADMIN_ENV_LIST
    )
    @pytest.mark.describe("test of the full tour with AstraDBDatabaseAdmin, async")
    async def test_astra_db_database_admin_async(
        self, admin_env_token: Tuple[str, str]
    ) -> None:
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
        admin_env, token = admin_env_token
        db_name = f"test_database_{admin_env}"
        db_provider = ADMIN_ENV_VARIABLE_MAP[admin_env]["provider"]
        db_region = ADMIN_ENV_VARIABLE_MAP[admin_env]["region"]

        # create client, get admin
        client: DataAPIClient
        if admin_env == "prod":
            client = DataAPIClient(token)
        else:
            client = DataAPIClient(token, environment=admin_env)
        admin = client.get_admin()

        # create a db (wait)
        db_admin = await admin.async_create_database(
            name=db_name,
            namespace="custom_namespace",
            wait_until_active=True,
            cloud_provider=db_provider,
            region=db_region,
        )

        # info with the AstraDBDatabaseAdmin
        created_db_id = db_admin.id
        assert (await db_admin.async_info()).id == created_db_id

        # list nss
        namespaces1 = set(await db_admin.async_list_namespaces())
        assert namespaces1 == {"custom_namespace"}

        # create two namespaces
        w_create_ns_response = await db_admin.async_create_namespace(
            "waited_ns",
            wait_until_active=True,
        )
        assert w_create_ns_response == {"ok": 1}

        nw_create_ns_response = await db_admin.async_create_namespace(
            "nonwaited_ns",
            wait_until_active=False,
        )
        assert nw_create_ns_response == {"ok": 1}

        async def _awaiter1() -> bool:
            return "nonwaited_ns" in (await db_admin.async_list_namespaces())

        await await_until_true(
            poll_interval=NAMESPACE_POLL_SLEEP_TIME,
            max_seconds=NAMESPACE_TIMEOUT,
            acondition=_awaiter1,
        )

        namespaces3 = set(await db_admin.async_list_namespaces())
        assert namespaces3 - namespaces1 == {"waited_ns", "nonwaited_ns"}

        # get db and use it
        adb = db_admin.get_async_database()
        await adb.create_collection("canary_coll")
        assert "canary_coll" in (await adb.list_collection_names())

        # check sync db is the same
        assert db_admin.get_database().to_async() == adb

        # drop nss, wait, nonwait
        w_drop_ns_response = await db_admin.async_drop_namespace(
            "waited_ns",
            wait_until_active=True,
        )
        assert w_drop_ns_response == {"ok": 1}

        nw_drop_ns_response = await db_admin.async_drop_namespace(
            "nonwaited_ns",
            wait_until_active=False,
        )
        assert nw_drop_ns_response == {"ok": 1}

        async def _awaiter2() -> bool:
            ns_list = await db_admin.async_list_namespaces()
            return "nonwaited_ns" not in ns_list

        await await_until_true(
            poll_interval=NAMESPACE_POLL_SLEEP_TIME,
            max_seconds=NAMESPACE_TIMEOUT,
            acondition=_awaiter2,
        )

        # check nss after dropping two of them
        namespaces1b = set(await db_admin.async_list_namespaces())
        assert namespaces1b == namespaces1

        async def _awaiter3() -> bool:
            a_info = await db_admin.async_info()
            return a_info.status == "ACTIVE"  # type: ignore[no-any-return]

        # drop db and check. We wait a little due to "nontransactional cluster md"
        await await_until_true(
            poll_interval=PRE_DROP_SAFETY_POLL_INTERVAL,
            max_seconds=PRE_DROP_SAFETY_TIMEOUT,
            acondition=_awaiter3,
        )
        db_drop_response = await db_admin.async_drop()
        assert db_drop_response == {"ok": 1}

        db_ids = {db.id for db in (await admin.async_list_databases())}
        assert created_db_id not in db_ids

    @pytest.mark.parametrize(
        "admin_env_token", admin_test_envs_tokens(), ids=ADMIN_ENV_LIST
    )
    @pytest.mark.describe(
        "test of the full tour with AstraDBAdmin and client methods, async"
    )
    async def test_astra_db_admin_async(self, admin_env_token: Tuple[str, str]) -> None:
        """
        Test plan (it has to be a single giant test to use the two DBs throughout):
        - create client -> get_admin
        - create two dbs (wait, nonwait)
        - list the two database ids, check
        - get check info on one such db through admin
        - with the client:
            4 get_dbs (a/sync, by id+region/api_endpoint), check if equal
            and test their list_collections
        - get_db_admin from the admin for one of the dbs
            - create ns
            - get_database -> list_collection_names
            - get_async_database and check == with above
        - drop dbs, (wait, nonwait)
        """
        admin_env, token = admin_env_token
        db_name_w = f"test_database_w_{admin_env}"
        db_name_nw = f"test_database_nw_{admin_env}"
        db_provider = ADMIN_ENV_VARIABLE_MAP[admin_env]["provider"]
        db_region = ADMIN_ENV_VARIABLE_MAP[admin_env]["region"]

        # create client and get admin
        client: DataAPIClient
        if admin_env == "prod":
            client = DataAPIClient(token)
        else:
            client = DataAPIClient(token, environment=admin_env)
        admin = client.get_admin()

        # create the two dbs
        db_admin_nw = await admin.async_create_database(
            name=db_name_nw,
            wait_until_active=False,
            cloud_provider=db_provider,
            region=db_region,
        )
        created_db_id_nw = db_admin_nw.id
        db_admin_w = await admin.async_create_database(
            name=db_name_w,
            wait_until_active=True,
            cloud_provider=db_provider,
            region=db_region,
        )
        created_db_id_w = db_admin_w.id

        async def _awaiter1() -> bool:
            db_ids = {db.id for db in (await admin.async_list_databases())}
            return created_db_id_nw in db_ids

        await await_until_true(
            poll_interval=DATABASE_POLL_SLEEP_TIME,
            max_seconds=DATABASE_TIMEOUT,
            acondition=_awaiter1,
        )

        # list, check ids
        db_ids = {db.id for db in (await admin.async_list_databases())}
        assert {created_db_id_nw, created_db_id_w} - db_ids == set()

        # get info through admin
        db_w_info = await admin.async_database_info(created_db_id_w)
        assert db_w_info.id == created_db_id_w

        # get and compare dbs obtained by the client
        synthetic_api_endpoint = API_ENDPOINT_TEMPLATE_MAP[admin_env].format(
            database_id=created_db_id_w,
            region=db_region,
        )
        adb_w_d = client.get_async_database(created_db_id_w)
        adb_w_r = client.get_async_database(created_db_id_w, region=db_region)
        adb_w_e = client.get_async_database(synthetic_api_endpoint)
        db_w_d = client.get_database(created_db_id_w)
        db_w_r = client.get_database(created_db_id_w, region=db_region)
        db_w_e = client.get_database(synthetic_api_endpoint)
        assert isinstance(await adb_w_d.list_collection_names(), list)
        assert adb_w_r == adb_w_d
        assert adb_w_e == adb_w_d
        assert db_w_d.to_async() == adb_w_d
        assert db_w_r.to_async() == adb_w_d
        assert db_w_e.to_async() == adb_w_d

        # get db admin from the admin and use it
        db_w_admin = admin.get_database_admin(created_db_id_w)
        await db_w_admin.async_create_namespace("additional_namespace")
        adb_w_from_admin = db_w_admin.get_async_database()
        assert isinstance(await adb_w_from_admin.list_collection_names(), list)
        db_w_from_admin = db_w_admin.get_database()
        assert db_w_from_admin.to_async() == adb_w_from_admin

        # drop databases: the w one through the admin, the nw using its db-admin
        #   (this covers most cases if combined with the
        #   (w, using db-admin) of test_astra_db_database_admin)
        assert db_admin_nw == admin.get_database_admin(created_db_id_nw)
        # drop db and check. We wait a little due to "nontransactional cluster md"

        async def _awaiter2() -> bool:
            a_info = await db_admin_nw.async_info()
            return a_info.status == "ACTIVE"  # type: ignore[no-any-return]

        async def _awaiter3() -> bool:
            a_info = await db_admin_w.async_info()
            return a_info.status == "ACTIVE"  # type: ignore[no-any-return]

        await await_until_true(
            poll_interval=PRE_DROP_SAFETY_POLL_INTERVAL,
            max_seconds=PRE_DROP_SAFETY_TIMEOUT,
            acondition=_awaiter2,
        )
        await await_until_true(
            poll_interval=PRE_DROP_SAFETY_POLL_INTERVAL,
            max_seconds=PRE_DROP_SAFETY_TIMEOUT,
            acondition=_awaiter3,
        )
        drop_nw_response = await db_admin_nw.async_drop(wait_until_active=False)
        assert drop_nw_response == {"ok": 1}
        drop_w_response = await admin.async_drop_database(created_db_id_w)
        assert drop_w_response == {"ok": 1}

        async def _awaiter4() -> bool:
            db_ids = {db.id for db in (await admin.async_list_databases())}
            return created_db_id_nw not in db_ids

        await await_until_true(
            poll_interval=DATABASE_POLL_SLEEP_TIME,
            max_seconds=DATABASE_TIMEOUT,
            acondition=_awaiter4,
        )

    @pytest.mark.describe(
        "test of the update_db_namespace flag for AstraDBDatabaseAdmin, sync"
    )
    def test_astra_updatedbnamespace_sync(self, sync_database: Database) -> None:
        NEW_NS_NAME_NOT_UPDATED = "tnudn_notupd"
        NEW_NS_NAME_UPDATED = "tnudn_upd"

        namespace0 = sync_database.namespace
        database_admin = sync_database.get_database_admin()
        database_admin.create_namespace(NEW_NS_NAME_NOT_UPDATED)
        assert sync_database.namespace == namespace0

        database_admin.create_namespace(NEW_NS_NAME_UPDATED, update_db_namespace=True)
        assert sync_database.namespace == NEW_NS_NAME_UPDATED

        database_admin.drop_namespace(NEW_NS_NAME_NOT_UPDATED)
        database_admin.drop_namespace(NEW_NS_NAME_UPDATED)

    @pytest.mark.describe(
        "test of the update_db_namespace flag for AstraDBDatabaseAdmin, async"
    )
    async def test_astra_updatedbnamespace_async(
        self, async_database: AsyncDatabase
    ) -> None:
        NEW_NS_NAME_NOT_UPDATED = "tnudn_notupd"
        NEW_NS_NAME_UPDATED = "tnudn_upd"

        namespace0 = async_database.namespace
        database_admin = async_database.get_database_admin()
        await database_admin.async_create_namespace(NEW_NS_NAME_NOT_UPDATED)
        assert async_database.namespace == namespace0

        await database_admin.async_create_namespace(
            NEW_NS_NAME_UPDATED, update_db_namespace=True
        )
        assert async_database.namespace == NEW_NS_NAME_UPDATED

        await database_admin.async_drop_namespace(NEW_NS_NAME_NOT_UPDATED)
        await database_admin.async_drop_namespace(NEW_NS_NAME_UPDATED)
