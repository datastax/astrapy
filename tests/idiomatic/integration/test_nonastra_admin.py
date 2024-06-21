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

import pytest

from astrapy import Database, AsyncDatabase

from ..conftest import IS_ASTRA_DB


@pytest.mark.skipif(IS_ASTRA_DB, reason="Not supported on Astra DB")
class TestNonAstraAdmin:
    @pytest.mark.describe(
        "test of the namespace crud with non-Astra DataAPIDatabaseAdmin, sync"
    )
    def test_nonastra_database_admin_sync(self, sync_database: Database) -> None:
        """
        Test plan
        - database -> get_database_admin
            - list namespaces, check name not there
            - create a namespace
            - list namespaces, check
            - database -> create_collection/list_collection_names
            - drop namespace
            - list namespaces, check
        """
        NEW_NS_NAME = "tnaa_test_ns_s"
        NEW_COLL_NAME = "tnaa_test_coll"
        database_admin = sync_database.get_database_admin()

        namespaces1 = set(database_admin.list_namespaces())
        assert NEW_NS_NAME not in namespaces1

        create_response = database_admin.create_namespace(NEW_NS_NAME)
        assert create_response == {"ok": 1}

        namespaces2 = set(database_admin.list_namespaces())
        assert NEW_NS_NAME in namespaces2

        sync_database_ns = sync_database.with_options(namespace=NEW_NS_NAME)
        sync_database_ns.create_collection(NEW_COLL_NAME)
        assert NEW_COLL_NAME in sync_database_ns.list_collection_names()

        drop_response = database_admin.drop_namespace(NEW_NS_NAME)
        assert drop_response == {"ok": 1}

        namespaces3 = set(database_admin.list_namespaces())
        assert namespaces3 == namespaces1

    @pytest.mark.describe(
        "test of the namespace crud with non-Astra DataAPIDatabaseAdmin, async"
    )
    async def test_nonastra_database_admin_async(
        self, async_database: AsyncDatabase
    ) -> None:
        """
        Test plan
        - database -> get_database_admin
            - list namespaces, check name not there
            - create a namespace
            - list namespaces, check
            - database -> create_collection/list_collection_names
            - drop namespace
            - list namespaces, check
        """
        NEW_NS_NAME = "tnaa_test_ns_a"
        NEW_COLL_NAME = "tnaa_test_coll"
        database_admin = async_database.get_database_admin()

        namespaces1 = set(await database_admin.async_list_namespaces())
        assert NEW_NS_NAME not in namespaces1

        create_response = await database_admin.async_create_namespace(NEW_NS_NAME)
        assert create_response == {"ok": 1}

        namespaces2 = set(await database_admin.async_list_namespaces())
        assert NEW_NS_NAME in namespaces2

        async_database_ns = async_database.with_options(namespace=NEW_NS_NAME)
        await async_database_ns.create_collection(NEW_COLL_NAME)
        assert NEW_COLL_NAME in (await async_database_ns.list_collection_names())

        drop_response = await database_admin.async_drop_namespace(NEW_NS_NAME)
        assert drop_response == {"ok": 1}

        namespaces3 = set(await database_admin.async_list_namespaces())
        assert namespaces3 == namespaces1
