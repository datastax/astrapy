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

import json
from decimal import Decimal

import pytest
from httpx import HTTPStatusError, Response
from pytest_httpserver import HTTPServer

from astrapy import DataAPIClient
from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import Environment
from astrapy.exceptions import (
    DataAPIErrorDescriptor,
    DataAPIHttpException,
    DataAPIResponseException,
    InvalidEnvironmentException,
)

ERROR_MESSAGE = "da_message"
ERROR_CODE = "DA_ERRORCODE"
ERROR_ATTRIBUTES = {"ekey": "evalue"}
ERROR_TITLE = "da_title"
ERROR_JSON = {
    "title": ERROR_TITLE,
    "errorCode": ERROR_CODE,
    "message": ERROR_MESSAGE,
    **ERROR_ATTRIBUTES,
}
ERROR_TITLE = f"{ERROR_TITLE}: {ERROR_MESSAGE} ({ERROR_CODE})"
FULL_RESPONSE_OF_500 = json.dumps({"errors": [ERROR_JSON]})


@pytest.mark.describe("test DataAPIHttpException")
def test_dataapihttpexception() -> None:
    """Test that regardless of how incorrect the input httpx error, nothing breaks."""
    se0 = HTTPStatusError(message="httpx_message", request="req", response=None)  # type: ignore[arg-type]
    se1 = HTTPStatusError(message="httpx_message", request="req", response="blah")  # type: ignore[arg-type]
    se2 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(status_code=500, text="blah"),
    )
    se3 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(status_code=500, text='{"blabla": 1}'),
    )
    se4 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(status_code=500, text='{"errors": []}'),
    )
    se5 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(
            status_code=500,
            text=FULL_RESPONSE_OF_500,
        ),
    )

    de0 = DataAPIHttpException.from_httpx_error(se0)
    de1 = DataAPIHttpException.from_httpx_error(se1)
    de2 = DataAPIHttpException.from_httpx_error(se2)
    de3 = DataAPIHttpException.from_httpx_error(se3)
    de4 = DataAPIHttpException.from_httpx_error(se4)
    de5 = DataAPIHttpException.from_httpx_error(se5)

    repr(de0)
    repr(de1)
    repr(de2)
    repr(de3)
    repr(de4)
    repr(de5)
    str(de0)
    str(de1)
    str(de2)
    str(de3)
    str(de4)
    assert ERROR_MESSAGE in str(de5)


@pytest.mark.describe("test DataAPIHttpException raising 500 from a mock server, sync")
def test_dataapihttpexception_raising_500_sync(httpserver: HTTPServer) -> None:
    """
    testing that:
        - the request gets sent
        - the correct exception is raised, with the expected members:
            - X its request and response fields are those of the httpx object
            - its DataAPIHttpException fields are set
        - it is caught with an httpx "except" clause all right
    """
    root_endpoint = httpserver.url_for("/")
    client = DataAPIClient(environment="other")
    database = client.get_database(root_endpoint, keyspace="xkeyspace")
    collection = database.get_collection("xcoll")
    expected_url = "/v1/xkeyspace/xcoll"
    httpserver.expect_oneshot_request(
        expected_url,
        method="POST",
    ).respond_with_data(
        FULL_RESPONSE_OF_500,
        status=500,
    )
    exc = None
    try:
        collection.find_one()
    except HTTPStatusError as e:
        exc = e

    assert isinstance(exc, DataAPIHttpException)
    httpx_payload = json.loads(exc.request.content.decode())
    assert "findOne" in httpx_payload
    assert ERROR_MESSAGE in exc.response.text
    assert exc.error_descriptors == [DataAPIErrorDescriptor(ERROR_JSON)]
    assert ERROR_MESSAGE in str(exc)


@pytest.mark.describe("test DataAPIHttpException raising 404 from a mock server, sync")
def test_dataapihttpexception_raising_404_sync(httpserver: HTTPServer) -> None:
    """
    testing that:
        - the request gets sent
        - the correct exception is raised, with the expected members:
            - X its request and response fields are those of the httpx object
            - its DataAPIHttpException fields are set
        - it is caught with an httpx "except" clause all right
    """
    root_endpoint = httpserver.url_for("/")
    client = DataAPIClient(environment="other")
    database = client.get_database(root_endpoint, keyspace="xkeyspace")
    collection = database.get_collection("xcoll")
    expected_url = "/v1/xkeyspace/xcoll"
    httpserver.expect_oneshot_request(
        expected_url,
        method="POST",
    ).respond_with_data(
        "blah",
        status=404,
    )
    exc = None
    try:
        collection.find_one()
    except HTTPStatusError as e:
        exc = e

    assert isinstance(exc, DataAPIHttpException)
    httpx_payload = json.loads(exc.request.content.decode())
    assert "findOne" in httpx_payload
    assert "blah" in exc.response.text
    assert exc.error_descriptors == []
    # not parsable into a DataAPIErrorDescriptor -> not in the error str repr
    assert "blah" not in str(exc)


@pytest.mark.describe("test DataAPIHttpException raising 500 from a mock server, async")
async def test_dataapihttpexception_raising_500_async(httpserver: HTTPServer) -> None:
    """
    testing that:
        - the request gets sent
        - the correct exception is raised, with the expected members:
            - X its request and response fields are those of the httpx object
            - its DataAPIHttpException fields are set
        - it is caught with an httpx "except" clause all right
    """
    root_endpoint = httpserver.url_for("/")
    client = DataAPIClient(environment="other")
    adatabase = client.get_async_database(root_endpoint, keyspace="xkeyspace")
    acollection = adatabase.get_collection("xcoll")
    expected_url = "/v1/xkeyspace/xcoll"
    httpserver.expect_oneshot_request(
        expected_url,
        method="POST",
    ).respond_with_data(
        FULL_RESPONSE_OF_500,
        status=500,
    )
    exc = None
    try:
        await acollection.find_one()
    except HTTPStatusError as e:
        exc = e

    assert isinstance(exc, DataAPIHttpException)
    httpx_payload = json.loads(exc.request.content.decode())
    assert "findOne" in httpx_payload
    assert ERROR_MESSAGE in exc.response.text
    assert exc.error_descriptors == [DataAPIErrorDescriptor(ERROR_JSON)]
    assert ERROR_MESSAGE in str(exc)


@pytest.mark.describe("test DataAPIHttpException raising 404 from a mock server, async")
async def test_dataapihttpexception_raising_404_async(httpserver: HTTPServer) -> None:
    """
    testing that:
        - the request gets sent
        - the correct exception is raised, with the expected members:
            - X its request and response fields are those of the httpx object
            - its DataAPIHttpException fields are set
        - it is caught with an httpx "except" clause all right
    """
    root_endpoint = httpserver.url_for("/")
    client = DataAPIClient(environment="other")
    adatabase = client.get_async_database(root_endpoint, keyspace="xkeyspace")
    acollection = adatabase.get_collection("xcoll")
    expected_url = "/v1/xkeyspace/xcoll"
    httpserver.expect_oneshot_request(
        expected_url,
        method="POST",
    ).respond_with_data(
        "blah",
        status=404,
    )
    exc = None
    try:
        await acollection.find_one()
    except HTTPStatusError as e:
        exc = e

    assert isinstance(exc, DataAPIHttpException)
    httpx_payload = json.loads(exc.request.content.decode())
    assert "findOne" in httpx_payload
    assert "blah" in exc.response.text
    assert exc.error_descriptors == []
    # not parsable into a DataAPIErrorDescriptor -> not in the error str repr
    assert "blah" not in str(exc)


@pytest.mark.describe("test DataAPIResponseException")
def test_dataapiresponseexception() -> None:
    da_e1 = DataAPIResponseException.from_response(
        command={"cmd": "C1"},
        raw_response=({"errors": [ERROR_JSON]}),
    )
    the_daed = DataAPIErrorDescriptor(ERROR_JSON)

    assert the_daed.error_code == ERROR_CODE
    assert the_daed.message == ERROR_MESSAGE
    assert the_daed.attributes == ERROR_ATTRIBUTES

    assert da_e1.text == ERROR_TITLE
    assert str(da_e1) == ERROR_TITLE
    assert da_e1.error_descriptors == [the_daed]


@pytest.mark.describe("test of database info failures, sync")
def test_get_database_info_failures_sync() -> None:
    client = DataAPIClient(environment=Environment.OTHER)
    db = client.get_database("http://not.a.real.thing", keyspace="ks")
    with pytest.raises(InvalidEnvironmentException):
        db.info()


@pytest.mark.describe("test of database info failures, async")
async def test_get_database_info_failures_async() -> None:
    client = DataAPIClient(environment=Environment.OTHER)
    adb = client.get_async_database("http://not.a.real.thing", keyspace="ks")
    with pytest.raises(InvalidEnvironmentException):
        await adb.info()


@pytest.mark.describe("test of collections not inserting Decimals, sync")
def test_collections_error_on_decimal_sync(httpserver: HTTPServer) -> None:
    root_endpoint = httpserver.url_for("/")
    client = DataAPIClient(environment="other")
    database = client.get_database(root_endpoint, keyspace="xkeyspace")
    collection = database.get_collection("xcoll")
    expected_url = "/v1/xkeyspace/xcoll"
    httpserver.expect_request(
        expected_url,
        method="POST",
    ).respond_with_json({"status": {"insertedIds": ["x"]}})
    collection.insert_one({"a": 1.23})
    with pytest.raises(TypeError):
        collection.insert_one({"a": Decimal("1.23")})
    hd_collection = collection.with_options(
        api_options=APIOptions(
            serdes_options=SerdesOptions(use_decimals_in_collections=True)
        )
    )
    hd_collection.insert_one({"a": 1.23})
    hd_collection.insert_one({"a": Decimal("1.23")})


@pytest.mark.describe("test of collections not inserting Decimals, async")
async def test_collections_error_on_decimal_async(httpserver: HTTPServer) -> None:
    root_endpoint = httpserver.url_for("/")
    client = DataAPIClient(environment="other")
    adatabase = client.get_async_database(root_endpoint, keyspace="xkeyspace")
    acollection = adatabase.get_collection("xcoll")
    expected_url = "/v1/xkeyspace/xcoll"
    httpserver.expect_request(
        expected_url,
        method="POST",
    ).respond_with_json({"status": {"insertedIds": ["x"]}})
    await acollection.insert_one({"a": 1.23})
    with pytest.raises(TypeError):
        await acollection.insert_one({"a": Decimal("1.23")})
    hd_acollection = acollection.with_options(
        api_options=APIOptions(
            serdes_options=SerdesOptions(use_decimals_in_collections=True)
        )
    )
    await hd_acollection.insert_one({"a": 1.23})
    await hd_acollection.insert_one({"a": Decimal("1.23")})
