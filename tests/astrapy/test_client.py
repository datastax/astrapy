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

from typing import List
from astrapy.client import create_astra_client, AstraClient
import pytest
import logging
import os
import uuid
import time
from faker import Faker


logger = logging.getLogger(__name__)
fake = Faker()

ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')
TABLE_NAME = fake.bothify(text="users_????")

STARGATE_BASE_URL = os.environ.get('STARGATE_BASE_URL')
STARGATE_AUTH_URL = os.environ.get('STARGATE_AUTH_URL')
STARGATE_USERNAME = os.environ.get('STARGATE_USERNAME')
STARGATE_PASSWORD = os.environ.get('STARGATE_PASSWORD')


@pytest.fixture
def astra_client():
    return create_astra_client(astra_database_id=ASTRA_DB_ID,
                               astra_database_region=ASTRA_DB_REGION,
                               astra_application_token=ASTRA_DB_APPLICATION_TOKEN)


@pytest.fixture
def stargate_client():
    return create_astra_client(base_url=STARGATE_BASE_URL,
                               auth_base_url=STARGATE_AUTH_URL,
                               username=STARGATE_USERNAME,
                               password=STARGATE_PASSWORD)


@pytest.fixture
def table_definition():
    return {
        "name": TABLE_NAME,
        "columnDefinitions": [
            {
                "name": "firstname",
                "typeDefinition": "text"
            },
            {
                "name": "lastname",
                "typeDefinition": "text"
            },
            {
                "name": "favorite_color",
                "typeDefinition": "text",
            }
        ],
        "primaryKey": {
            "partitionKey": [
                "firstname"
            ],
            "clusteringKey": [
                "lastname"
            ]
        }
    }


@pytest.fixture
def column_definition():
    return {
        "name": "favorite_food",
        "typeDefinition": "text"
    }


@pytest.fixture
def index_definition():
    return {
        "column": "favorite_color",
        "name": "favorite_color_idx",
        "ifNotExists": True
    }


@pytest.fixture
def udt_definition():
    return {
        "name": "custom",
        "ifNotExists": True,
        "fields": [
            {
                "name": "title",
                "typeDefinition": "text"
            }
        ]
    }


@pytest.mark.it('should initialize an AstraDB REST Client')
def test_connect(astra_client):
    assert type(astra_client) is AstraClient


@pytest.mark.it('should initialize a Stargate REST Client')
def test_stargate_connect(stargate_client):
    assert type(stargate_client) is AstraClient


@pytest.mark.it('should get databases')
def test_get_databases(astra_client):
    databases = astra_client.ops.get_databases()
    assert type(databases) is list


@pytest.mark.it('should get a database')
def test_get_databases(astra_client):
    databases = astra_client.ops.get_databases(options={"include": "active"})
    database = astra_client.ops.get_database(databases[0]["id"])
    assert databases[0]["id"] == database["id"]

# # TODO: when deleting a keyspace is available, round out the test
# @pytest.mark.it('should create a keyspace')
# def test_create_keyspace(astra_client):
#     res = astra_client.ops.create_keyspace(
#         database=ASTRA_DB_ID, keyspace="new_keyspace")
#     print(res)
#     assert res is not None

# # TODO: find a way to terminate pending databases
# @pytest.mark.it('should create and delete a database')
# def test_create_database(astra_client):
#     create_res = astra_client.ops.create_database({
#         "name": "astrapy-test-db",
#         "keyspace": "astrapy_test",
#         "cloudProvider": "AWS",
#         "tier": "serverless",
#         "capacityUnits": 1,
#         "region": "us-west-2"
#     })
#     assert type(create_res["id"]) is str
#     time.sleep(10)
#     delete_res = astra_client.ops.terminate_database(create_res["id"])
#     assert delete_res is not None


@pytest.mark.it('should get a secure bundle')
def test_get_secure_bundle(astra_client):
    bundle = astra_client.ops.get_secure_bundle(database=ASTRA_DB_ID)
    assert bundle["downloadURL"] is not None


@pytest.mark.it('should get datacenters')
def test_get_datacenters(astra_client):
    datacenters = astra_client.ops.get_datacenters(database=ASTRA_DB_ID)
    assert type(datacenters) is list


@pytest.mark.it('should get a private link')
def test_get_private_link(astra_client):
    private_link = astra_client.ops.get_private_link(database=ASTRA_DB_ID)
    assert private_link["clusterID"] == ASTRA_DB_ID


@pytest.mark.it('should get available classic regions')
def test_get_available_classic_regions(astra_client):
    regions = astra_client.ops.get_available_classic_regions()
    assert type(regions) is list


@pytest.mark.it('should get available regions')
def test_get_available_regions(astra_client):
    regions = astra_client.ops.get_available_regions()
    assert type(regions) is list


@pytest.mark.it('should get roles')
def test_get_roles(astra_client):
    roles = astra_client.ops.get_roles()
    assert type(roles) is list


@pytest.mark.it('should get users')
def test_get_users(astra_client):
    users = astra_client.ops.get_users()
    assert users["OrgID"] is not None


@pytest.mark.it('should get clients')
def test_get_clients(astra_client):
    clients = astra_client.ops.get_clients()
    assert clients["clients"] is not None


@pytest.mark.it('should get an organization')
def test_get_organization(astra_client):
    organization = astra_client.ops.get_organization()
    assert organization["id"] is not None


@pytest.mark.it('should get an access list template')
def test_get_access_list_template(astra_client):
    access_list_template = astra_client.ops.get_access_list_template()
    assert access_list_template["addresses"] is not None


@pytest.mark.it('should get all private links')
def test_get_private_links(astra_client):
    private_links = astra_client.ops.get_private_links()
    assert type(private_links) is list


@pytest.mark.it('should get all streaming providers')
def test_get_streaming_providers(astra_client):
    streaming_providers = astra_client.ops.get_streaming_providers()
    assert streaming_providers["aws"] is not None


@pytest.mark.it('should get all streaming tenants')
def test_get_streaming_tenants(astra_client):
    streaming_tenants = astra_client.ops.get_streaming_tenants()
    assert type(streaming_tenants) is list


@pytest.mark.it('should get all keyspaces')
def test_get_keyspaces(astra_client):
    keyspaces = astra_client.schemas.get_keyspaces()
    assert type(keyspaces) is list


@pytest.mark.it('should get a keyspace')
def test_get_keyspace(astra_client):
    keyspaces = astra_client.schemas.get_keyspaces()
    keyspace = astra_client.schemas.get_keyspace(keyspace=keyspaces[0]["name"])
    assert keyspace["name"] == keyspaces[0]["name"]


@pytest.mark.it('should create a table')
def test_create_table(astra_client, table_definition):
    table = astra_client.schemas.create_table(keyspace=ASTRA_DB_KEYSPACE,
                                              table_definition=table_definition)
    assert table["name"] == table_definition["name"]


@pytest.mark.it('should get all tables')
def test_get_tables(astra_client):
    tables = astra_client.schemas.get_tables(keyspace=ASTRA_DB_KEYSPACE)
    assert type(tables) is list


@pytest.mark.it('should get a table')
def test_get_table(astra_client, table_definition):
    table = astra_client.schemas.get_table(keyspace=ASTRA_DB_KEYSPACE,
                                           table=table_definition["name"])
    assert table["name"] == table_definition["name"]


@pytest.mark.it('should update a table')
def test_update_table(astra_client, table_definition):
    table_definition["tableOptions"] = {"defaultTimeToLive": 0}
    table = astra_client.schemas.update_table(keyspace=ASTRA_DB_KEYSPACE,
                                              table_definition=table_definition)
    assert table["name"] == table_definition["name"]


@pytest.mark.it('should create a column')
def test_create_column(astra_client, table_definition, column_definition):
    column = astra_client.schemas.create_column(keyspace=ASTRA_DB_KEYSPACE,
                                                table=table_definition["name"],
                                                column_definition=column_definition)
    assert column["name"] == column_definition["name"]


@pytest.mark.it('should get columns')
def test_get_columns(astra_client, table_definition):
    columns = astra_client.schemas.get_columns(keyspace=ASTRA_DB_KEYSPACE,
                                               table=table_definition["name"])
    assert type(columns) is list


@pytest.mark.it('should get a column')
def test_get_column(astra_client, table_definition, column_definition):
    column = astra_client.schemas.get_column(keyspace=ASTRA_DB_KEYSPACE,
                                             table=table_definition["name"],
                                             column=column_definition["name"])
    assert column["name"] == column_definition["name"]


@pytest.mark.it('should delete a column')
def test_delete_column(astra_client, table_definition, column_definition):
    res = astra_client.schemas.delete_column(keyspace=ASTRA_DB_KEYSPACE,
                                             table=table_definition["name"],
                                             column=column_definition["name"])
    assert res is None


@pytest.mark.it('should create an index')
def test_create_index(astra_client, table_definition, index_definition):
    res = astra_client.schemas.create_index(keyspace=ASTRA_DB_KEYSPACE,
                                            table=table_definition["name"],
                                            index_definition=index_definition)
    assert res["success"] == True


@pytest.mark.it('should get all indexes')
def test_get_indexes(astra_client, table_definition):
    indexes = astra_client.schemas.get_indexes(keyspace=ASTRA_DB_KEYSPACE,
                                               table=table_definition["name"])
    assert type(indexes) is list


@pytest.mark.it('should delete an index')
def test_delete_index(astra_client, table_definition, index_definition):
    res = astra_client.schemas.delete_index(keyspace=ASTRA_DB_KEYSPACE,
                                            table=table_definition["name"],
                                            index=index_definition["name"])
    assert res is None


@pytest.mark.it('should create a type')
def test_create_type(astra_client, udt_definition):
    udt = astra_client.schemas.create_type(keyspace=ASTRA_DB_KEYSPACE,
                                           udt_definition=udt_definition)
    assert udt["name"] == udt_definition["name"]


@pytest.mark.it('should get all types')
def test_get_types(astra_client):
    udts = astra_client.schemas.get_types(keyspace=ASTRA_DB_KEYSPACE)
    assert type(udts) is list


@pytest.mark.it('should get a type')
def test_get_type(astra_client, udt_definition):
    udt = astra_client.schemas.get_type(
        keyspace=ASTRA_DB_KEYSPACE, udt=udt_definition["name"])
    assert udt["name"] == udt_definition["name"]


@pytest.mark.it('should update a type')
def test_update_type(astra_client):
    udt_definition = {"name": "custom",
                      "addFields": [{
                          "name": "description",
                          "typeDefinition": "text",
                      }]}
    res = astra_client.schemas.update_type(keyspace=ASTRA_DB_KEYSPACE,
                                           udt_definition=udt_definition)
    assert res is None


@pytest.mark.it('should delete a type')
def test_delete_type(astra_client, udt_definition):
    res = astra_client.schemas.delete_type(keyspace=ASTRA_DB_KEYSPACE,
                                           udt=udt_definition["name"])
    assert res is None


@pytest.mark.it('should add rows')
def test_add_row(astra_client, table_definition):
    row_definition = {"firstname": "Cliff", "lastname": "Wicklow"}
    row = astra_client.rest.add_row(keyspace=ASTRA_DB_KEYSPACE,
                                    table=table_definition["name"],
                                    row=row_definition)
    assert row["firstname"] == row_definition["firstname"]


@pytest.mark.it('should get rows')
def test_get_rows(astra_client, table_definition):
    rows = astra_client.rest.get_rows(keyspace=ASTRA_DB_KEYSPACE,
                                      table=table_definition["name"],
                                      key_path="Cliff/Wicklow")
    assert rows["count"] is not None
    assert rows["data"][0]["firstname"] == "Cliff"


@pytest.mark.it('should search a table')
def test_search_table(astra_client, table_definition):
    query = {"firstname": {"$eq": "Cliff"}}
    res = astra_client.rest.search_table(keyspace=ASTRA_DB_KEYSPACE,
                                         table=table_definition["name"],
                                         query=query)
    assert res["count"] is not None
    assert res["data"][0]["firstname"] == "Cliff"


@pytest.mark.it('should query the gql schema')
def test_gql_schema(astra_client):
    query = """{
        keyspaces {
            name
        }
    }"""
    res = astra_client.gql.execute(query=query)
    assert res["keyspaces"] is not None


@pytest.mark.it('should use gql to create a table')
def test_gql_create_table(astra_client):
    query = """
        mutation createTable ($keyspaceName: String!) {
            book: createTable(
                keyspaceName: $keyspaceName,
                tableName: "book",
                partitionKeys: [
                    { name: "title", type: { basic: TEXT } }
                ]
                clusteringKeys: [
                    { name: "author", type: { basic: TEXT } }
                ]
            )
        }
    """
    res = astra_client.gql.execute(query=query,
                                   variables={"keyspaceName": ASTRA_DB_KEYSPACE})
    assert res["book"] is True


@pytest.mark.it('should use gql to insert into a table')
def test_gql_insert_table(astra_client):
    query = """
        mutation insert2Books {
            moby: insertbook(value: {title:"Moby Dick", author:"Herman Melville"}) {
                value {
                    title
                }
            }
            catch22: insertbook(value: {title:"Catch-22", author:"Joseph Heller"}) {
                value {
                    title
                }
            }
        }
    """
    res = astra_client.gql.execute(query=query, keyspace=ASTRA_DB_KEYSPACE)
    assert res["moby"] is not None


@pytest.mark.it('should use gql to delete a table')
def test_gql_delete_table(astra_client):
    query = """
       mutation dropTable ($keyspaceName: String!) {
            book: dropTable(
                keyspaceName: $keyspaceName,
                tableName: "book"
            )
        }
    """
    res = astra_client.gql.execute(query=query,
                                   variables={"keyspaceName": ASTRA_DB_KEYSPACE})
    assert res["book"] is True


@pytest.mark.it('should delete a table')
def test_delete_table(astra_client, table_definition):
    res = astra_client.schemas.delete_table(keyspace=ASTRA_DB_KEYSPACE,
                                            table=table_definition["name"])
    assert res == None
