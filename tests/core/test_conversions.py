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
Tests for the User-Agent customization logic
"""

import logging
import pytest

from astrapy.core.db import (
    AstraDB,
    AstraDBCollection,
    AsyncAstraDB,
    AsyncAstraDBCollection,
)
from astrapy.core.ops import AstraDBOps


logger = logging.getLogger(__name__)


@pytest.mark.describe("test basic equality between instances")
def test_instance_equality() -> None:
    astradb_a = AstraDB(token="t1", api_endpoint="a1")
    astradb_b = AstraDB(token="t1", api_endpoint="a1")
    astradb_c = AstraDB(token="t2", api_endpoint="a2")

    assert astradb_a == astradb_b
    assert astradb_a != astradb_c

    astradb_coll_a = AstraDBCollection("c1", token="t1", api_endpoint="a1")

    assert astradb_a != astradb_coll_a

    astradb_coll_b = AstraDBCollection("c1", astra_db=astradb_a)
    astradb_coll_c = AstraDBCollection("c3", token="t3", api_endpoint="a3")

    assert astradb_coll_a == astradb_coll_b
    assert astradb_coll_a != astradb_coll_c

    astradbops_o1 = AstraDBOps(token="t1")
    astradbops_o2 = AstraDBOps(token="t1")
    astradbops_o3 = AstraDBOps(token="t3")

    assert astradbops_o1 == astradbops_o2
    assert astradbops_o1 != astradbops_o3


@pytest.mark.describe("test basic equality between async instances")
def test_instance_equality_async() -> None:
    astradb_a = AsyncAstraDB(token="t1", api_endpoint="a1")
    astradb_b = AsyncAstraDB(token="t1", api_endpoint="a1")
    astradb_c = AsyncAstraDB(token="t2", api_endpoint="a2")

    assert astradb_a == astradb_b
    assert astradb_a != astradb_c

    astradb_coll_a = AsyncAstraDBCollection("c1", token="t1", api_endpoint="a1")

    assert astradb_a != astradb_coll_a

    astradb_coll_b = AsyncAstraDBCollection("c1", astra_db=astradb_a)
    astradb_coll_c = AsyncAstraDBCollection("c3", token="t3", api_endpoint="a3")

    assert astradb_coll_a == astradb_coll_b
    assert astradb_coll_a != astradb_coll_c


@pytest.mark.describe("test to_sync and to_async methods combine to identity")
def test_round_conversion_is_noop() -> None:
    sync_astradb = AstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    assert sync_astradb.to_async().to_sync() == sync_astradb

    async_astradb = AsyncAstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    assert async_astradb.to_sync().to_async() == async_astradb

    sync_adbcollection = AstraDBCollection(
        collection_name="collection_name",
        astra_db=sync_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    assert sync_adbcollection.to_async().to_sync() == sync_adbcollection

    async_adbcollection = AsyncAstraDBCollection(
        collection_name="collection_name",
        astra_db=async_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    assert async_adbcollection.to_sync().to_async() == async_adbcollection


@pytest.mark.describe("test copy methods create identical objects")
def test_copy_methods() -> None:
    sync_astradb = AstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    c_sync_astradb = sync_astradb.copy()
    assert c_sync_astradb == sync_astradb
    assert c_sync_astradb is not sync_astradb

    async_astradb = AsyncAstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    c_async_astradb = async_astradb.copy()
    assert c_async_astradb == async_astradb
    assert c_async_astradb is not async_astradb

    sync_adbcollection = AstraDBCollection(
        collection_name="collection_name",
        astra_db=sync_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    c_sync_adbcollection = sync_adbcollection.copy()
    assert c_sync_adbcollection == sync_adbcollection
    assert c_sync_adbcollection is not sync_adbcollection

    async_adbcollection = AsyncAstraDBCollection(
        collection_name="collection_name",
        astra_db=async_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    c_async_adbcollection = async_adbcollection.copy()
    assert c_async_adbcollection == async_adbcollection
    assert c_async_adbcollection is not async_adbcollection

    adb_ops = AstraDBOps(
        token="token",
        dev_ops_url="dev_ops_url",
        dev_ops_api_version="dev_ops_api_version",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    c_adb_ops = adb_ops.copy()
    assert c_adb_ops == adb_ops
    assert c_adb_ops is not adb_ops


@pytest.mark.describe("test copy methods respect mutable caller identity")
def test_copy_methods_mutable_caller() -> None:
    sync_astradb = AstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    sync_astradb.set_caller(
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    sync_astradb2 = AstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert sync_astradb.copy() == sync_astradb2

    async_astradb = AsyncAstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    async_astradb.set_caller(
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    async_astradb2 = AsyncAstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert async_astradb.copy() == async_astradb2

    sync_adbcollection = AstraDBCollection(
        collection_name="collection_name",
        astra_db=sync_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    sync_adbcollection.set_caller(
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    sync_adbcollection2 = AstraDBCollection(
        collection_name="collection_name",
        astra_db=sync_astradb,
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert sync_adbcollection.copy() == sync_adbcollection2

    async_adbcollection = AsyncAstraDBCollection(
        collection_name="collection_name",
        astra_db=async_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    async_adbcollection.set_caller(
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    async_adbcollection2 = AsyncAstraDBCollection(
        collection_name="collection_name",
        astra_db=async_astradb,
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert async_adbcollection.copy() == async_adbcollection2

    adb_ops = AstraDBOps(
        token="token",
        dev_ops_url="dev_ops_url",
        dev_ops_api_version="dev_ops_api_version",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    adb_ops.set_caller(
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    adb_ops2 = AstraDBOps(
        token="token",
        dev_ops_url="dev_ops_url",
        dev_ops_api_version="dev_ops_api_version",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert adb_ops.copy() == adb_ops2


@pytest.mark.describe("test parameter override in copy methods")
def test_parameter_override_copy_methods() -> None:
    sync_astradb = AstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    sync_astradb2 = AstraDB(
        token="token2",
        api_endpoint="api_endpoint2",
        api_path="api_path2",
        api_version="api_version2",
        namespace="namespace2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    c_sync_astradb = sync_astradb.copy(
        token="token2",
        api_endpoint="api_endpoint2",
        api_path="api_path2",
        api_version="api_version2",
        namespace="namespace2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert c_sync_astradb == sync_astradb2

    async_astradb = AsyncAstraDB(
        token="token",
        api_endpoint="api_endpoint",
        api_path="api_path",
        api_version="api_version",
        namespace="namespace",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    async_astradb2 = AsyncAstraDB(
        token="token2",
        api_endpoint="api_endpoint2",
        api_path="api_path2",
        api_version="api_version2",
        namespace="namespace2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    c_async_astradb = async_astradb.copy(
        token="token2",
        api_endpoint="api_endpoint2",
        api_path="api_path2",
        api_version="api_version2",
        namespace="namespace2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert c_async_astradb == async_astradb2

    sync_adbcollection = AstraDBCollection(
        collection_name="collection_name",
        astra_db=sync_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    sync_adbcollection2 = AstraDBCollection(
        collection_name="collection_name2",
        astra_db=sync_astradb2,
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    c_sync_adbcollection = sync_adbcollection.copy(
        collection_name="collection_name2",
        token="token2",
        api_endpoint="api_endpoint2",
        api_path="api_path2",
        api_version="api_version2",
        namespace="namespace2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert c_sync_adbcollection == sync_adbcollection2

    async_adbcollection = AsyncAstraDBCollection(
        collection_name="collection_name",
        astra_db=async_astradb,
        caller_name="caller_name",
        caller_version="caller_version",
    )
    async_adbcollection2 = AsyncAstraDBCollection(
        collection_name="collection_name2",
        astra_db=async_astradb2,
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    c_async_adbcollection = async_adbcollection.copy(
        collection_name="collection_name2",
        token="token2",
        api_endpoint="api_endpoint2",
        api_path="api_path2",
        api_version="api_version2",
        namespace="namespace2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert c_async_adbcollection == async_adbcollection2

    adb_ops = AstraDBOps(
        token="token",
        dev_ops_url="dev_ops_url",
        dev_ops_api_version="dev_ops_api_version",
        caller_name="caller_name",
        caller_version="caller_version",
    )
    adb_ops2 = AstraDBOps(
        token="token2",
        dev_ops_url="dev_ops_url2",
        dev_ops_api_version="dev_ops_api_version2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    c_adb_ops = adb_ops.copy(
        token="token2",
        dev_ops_url="dev_ops_url2",
        dev_ops_api_version="dev_ops_api_version2",
        caller_name="caller_name2",
        caller_version="caller_version2",
    )
    assert c_adb_ops == adb_ops2


@pytest.mark.describe("test parameter override when instantiating collections")
def test_parameter_override_collection_instances() -> None:
    astradb0 = AstraDB(token="t0", api_endpoint="a0")
    astradb1 = AstraDB(token="t1", api_endpoint="a1", namespace="n1")
    col0 = AstraDBCollection(
        collection_name="col0",
        astra_db=astradb0,
    )
    col1 = AstraDBCollection(
        collection_name="col0",
        astra_db=astradb0,
        token="t1",
        api_endpoint="a1",
        namespace="n1",
    )
    assert col0 != col1
    assert col1 == AstraDBCollection(collection_name="col0", astra_db=astradb1)

    a_astradb0 = AsyncAstraDB(token="t0", api_endpoint="a0")
    a_astradb1 = AsyncAstraDB(token="t1", api_endpoint="a1", namespace="n1")
    a_col0 = AsyncAstraDBCollection(
        collection_name="col0",
        astra_db=a_astradb0,
    )
    a_col1 = AsyncAstraDBCollection(
        collection_name="col0",
        astra_db=a_astradb0,
        token="t1",
        api_endpoint="a1",
        namespace="n1",
    )
    assert a_col0 != a_col1
    assert a_col1 == AsyncAstraDBCollection(collection_name="col0", astra_db=a_astradb1)


@pytest.mark.describe("test set_caller works in place for clients")
def test_set_caller_clients() -> None:
    astradb0 = AstraDB(token="t1", api_endpoint="a1")
    astradbops0 = AstraDBOps(token="t1")
    async_astradb0 = AsyncAstraDB(token="t1", api_endpoint="a1")
    #
    astradb0.set_caller(caller_name="CN", caller_version="CV")
    astradbops0.set_caller(caller_name="CN", caller_version="CV")
    async_astradb0.set_caller(caller_name="CN", caller_version="CV")
    #
    astradb = AstraDB(
        token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    astradbops = AstraDBOps(token="t1", caller_name="CN", caller_version="CV")
    async_astradb = AsyncAstraDB(
        token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    #
    assert astradb0 == astradb
    assert astradbops0 == astradbops
    assert async_astradb0 == async_astradb


@pytest.mark.describe("test set_caller works in place for collections")
def test_set_caller_collections() -> None:
    """
    This tests (1) the collection set_caller, (2) the fact that it is propagated
    to the client, and (3) the propagation of the caller info to the astra_db
    being created if not passed to the collection constructor.
    """
    adb_collection0 = AstraDBCollection("c1", token="t1", api_endpoint="a1")
    async_adb_collection0 = AsyncAstraDBCollection("c1", token="t1", api_endpoint="a1")
    #
    adb_collection0.set_caller(caller_name="CN", caller_version="CV")
    async_adb_collection0.set_caller(caller_name="CN", caller_version="CV")
    #
    adb_collection = AstraDBCollection(
        "c1", token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    async_adb_collection = AsyncAstraDBCollection(
        "c1", token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    #
    assert adb_collection0 == adb_collection
    assert async_adb_collection0 == async_adb_collection


@pytest.mark.describe("test caller inheritance from client to collection")
def test_caller_inheritance_from_clients() -> None:
    """
    This tests the fact that when passing a client in collection creation
    the caller is acquired by default from said client.
    """
    astradb = AstraDB(
        token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    async_astradb = AsyncAstraDB(
        token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )

    adb_collection = AstraDBCollection("c1", astra_db=astradb)
    async_adb_collection = AsyncAstraDBCollection("c1", astra_db=async_astradb)

    ref_adb_collection = AstraDBCollection(
        "c1", token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    async_ref_adb_collection = AsyncAstraDBCollection(
        "c1", token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )

    assert ref_adb_collection == adb_collection
    assert async_ref_adb_collection == async_adb_collection


@pytest.mark.describe("test caller inheritance when spawning a collection")
async def test_caller_inheritance_spawning() -> None:
    """
    This tests that the caller is retained with the clients' .collection()
    method.
    As this module is for lightweight tests, no actual API operations involved,
    this single test will be enough: create_collection and truncate_collection
    are not covered (they work identically to this one though).
    """
    astradb = AstraDB(
        token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    async_astradb = AsyncAstraDB(
        token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )

    spawned_collection = astradb.collection("c1")
    async_spawned_collection = await async_astradb.collection("c1")

    ref_spawned_collection = AstraDBCollection(
        "c1", token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )
    async_ref_spawned_collection = AsyncAstraDBCollection(
        "c1", token="t1", api_endpoint="a1", caller_name="CN", caller_version="CV"
    )

    assert spawned_collection == ref_spawned_collection
    assert async_spawned_collection == async_ref_spawned_collection
