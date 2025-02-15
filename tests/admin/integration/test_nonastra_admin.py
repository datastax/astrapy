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

import pytest

from astrapy import AsyncDatabase, Database

from ..conftest import (
    IS_ASTRA_DB,
)


@pytest.mark.skipif(IS_ASTRA_DB, reason="Not supported on Astra DB")
class TestNonAstraAdmin:
    @pytest.mark.describe(
        "test of the keyspace crud with non-Astra DataAPIDatabaseAdmin, sync"
    )
    def test_nonastra_database_admin_keyspaces_sync(
        self, sync_database: Database
    ) -> None:
        """
        Test plan
        - database -> get_database_admin
            - list keyspaces, check name not there
            - create a keyspace
            - list keyspaces, check
            - database -> create_collection/list_collection_names
            - drop keyspace
            - list keyspaces, check
            - ALSO: test the update_db_keyspace flag when creating from db->dbadmin
        This version uses v2.0-compliant 'keyspace' method names
        """
        NEW_KS_NAME = "tnaa_test_ks_s"
        NEW_COLL_NAME = "tnaa_test_coll"
        database_admin = sync_database.get_database_admin()

        keyspaces1 = set(database_admin.list_keyspaces())
        assert NEW_KS_NAME not in keyspaces1

        database_admin.create_keyspace(NEW_KS_NAME)

        keyspaces2 = set(database_admin.list_keyspaces())
        assert NEW_KS_NAME in keyspaces2

        sync_database_ks = sync_database.with_options(keyspace=NEW_KS_NAME)
        sync_database_ks.create_collection(NEW_COLL_NAME)
        assert NEW_COLL_NAME in sync_database_ks.list_collection_names()

        database_admin.drop_keyspace(NEW_KS_NAME)

        keyspaces3 = set(database_admin.list_keyspaces())
        assert keyspaces3 == keyspaces1

        # update_db_keyspace:
        NEW_KS_NAME_NOT_UPDATED = "tnudn_notupd"
        NEW_KS_NAME_UPDATED = "tnudn_upd"

        database = sync_database._copy()

        keyspace0 = database.keyspace
        database_admin = database.get_database_admin()
        database_admin.create_keyspace(NEW_KS_NAME_NOT_UPDATED)
        assert database.keyspace == keyspace0

        database_admin.create_keyspace(NEW_KS_NAME_UPDATED, update_db_keyspace=True)
        assert database.keyspace == NEW_KS_NAME_UPDATED

        database_admin.drop_keyspace(NEW_KS_NAME_NOT_UPDATED)
        database_admin.drop_keyspace(NEW_KS_NAME_UPDATED)

    @pytest.mark.describe(
        "test of the keyspace crud with non-Astra DataAPIDatabaseAdmin, async"
    )
    async def test_nonastra_database_admin_keyspaces_async(
        self, async_database: AsyncDatabase
    ) -> None:
        """
        Test plan
        - database -> get_database_admin
            - list keyspaces, check name not there
            - create a keyspace
            - list keyspaces, check
            - database -> create_collection/list_collection_names
            - drop keyspace
            - list keyspaces, check
            - ALSO: test the update_db_keyspace flag when creating from db->dbadmin
        This version uses v2.0-compliant 'keyspace' method names
        """
        NEW_KS_NAME = "tnaa_test_ks_a"
        NEW_COLL_NAME = "tnaa_test_coll"
        database_admin = async_database.get_database_admin()

        keyspaces1 = set(await database_admin.async_list_keyspaces())
        assert NEW_KS_NAME not in keyspaces1

        await database_admin.async_create_keyspace(NEW_KS_NAME)

        keyspaces2 = set(await database_admin.async_list_keyspaces())
        assert NEW_KS_NAME in keyspaces2

        async_database_ks = async_database.with_options(keyspace=NEW_KS_NAME)
        await async_database_ks.create_collection(NEW_COLL_NAME)
        assert NEW_COLL_NAME in (await async_database_ks.list_collection_names())

        await database_admin.async_drop_keyspace(NEW_KS_NAME)

        keyspaces3 = set(await database_admin.async_list_keyspaces())
        assert keyspaces3 == keyspaces1

        # update_db_keyspace:
        NEW_KS_NAME_NOT_UPDATED = "tnudn_notupd"
        NEW_KS_NAME_UPDATED = "tnudn_upd"

        adatabase = async_database._copy()

        keyspace0 = adatabase.keyspace
        database_admin = adatabase.get_database_admin()
        await database_admin.async_create_keyspace(NEW_KS_NAME_NOT_UPDATED)
        assert adatabase.keyspace == keyspace0

        await database_admin.async_create_keyspace(
            NEW_KS_NAME_UPDATED, update_db_keyspace=True
        )
        assert adatabase.keyspace == NEW_KS_NAME_UPDATED

        await database_admin.async_drop_keyspace(NEW_KS_NAME_NOT_UPDATED)
        await database_admin.async_drop_keyspace(NEW_KS_NAME_UPDATED)
