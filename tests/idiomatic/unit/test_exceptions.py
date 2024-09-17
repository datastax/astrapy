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

import pytest
from httpx import HTTPStatusError, Response
from pytest_httpserver import HTTPServer

from astrapy import DataAPIClient
from astrapy.exceptions import (
    DataAPIDetailedErrorDescriptor,
    DataAPIErrorDescriptor,
    DataAPIHttpException,
    DataAPIResponseException,
    DeleteManyException,
    InsertManyException,
)
from astrapy.results import DeleteResult, InsertManyResult

SAMPLE_API_MESSAGE = "da_message"
SAMPLE_API_ERROR_OBJECT = {
    "title": "da_title",
    "errorCode": "DA_ERRORCODE",
    "message": SAMPLE_API_MESSAGE,
}
FULL_RESPONSE_OF_500 = json.dumps({"errors": [SAMPLE_API_ERROR_OBJECT]})


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
    assert SAMPLE_API_MESSAGE in str(de5)


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
    database = client.get_database(root_endpoint, namespace="xnamespace")
    collection = database.get_collection("xcoll")
    expected_url = "/v1/xnamespace/xcoll"
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
    assert "find" in httpx_payload
    assert SAMPLE_API_MESSAGE in exc.response.text
    assert exc.error_descriptors == [DataAPIErrorDescriptor(SAMPLE_API_ERROR_OBJECT)]
    assert SAMPLE_API_MESSAGE in str(exc)


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
    database = client.get_database(root_endpoint, namespace="xnamespace")
    collection = database.get_collection("xcoll")
    expected_url = "/v1/xnamespace/xcoll"
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
    assert "find" in httpx_payload
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
    adatabase = client.get_async_database(root_endpoint, namespace="xnamespace")
    acollection = await adatabase.get_collection("xcoll")
    expected_url = "/v1/xnamespace/xcoll"
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
    assert "find" in httpx_payload
    assert SAMPLE_API_MESSAGE in exc.response.text
    assert exc.error_descriptors == [DataAPIErrorDescriptor(SAMPLE_API_ERROR_OBJECT)]
    assert SAMPLE_API_MESSAGE in str(exc)


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
    adatabase = client.get_async_database(root_endpoint, namespace="xnamespace")
    acollection = await adatabase.get_collection("xcoll")
    expected_url = "/v1/xnamespace/xcoll"
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
    assert "find" in httpx_payload
    assert "blah" in exc.response.text
    assert exc.error_descriptors == []
    # not parsable into a DataAPIErrorDescriptor -> not in the error str repr
    assert "blah" not in str(exc)


@pytest.mark.describe("test DataAPIResponseException")
def test_dataapiresponseexception() -> None:
    da_e1 = DataAPIResponseException.from_responses(
        commands=[{"cmd": "C1"}],
        raw_responses=[
            {"errors": [{"errorCode": "C", "message": "Aaa", "field": "value"}]}
        ],
    )
    the_daed = DataAPIErrorDescriptor(
        {"errorCode": "C", "message": "Aaa", "field": "value"}
    )

    assert the_daed.error_code == "C"
    assert the_daed.message == "Aaa"
    assert the_daed.attributes == {"field": "value"}

    assert da_e1.text == "Aaa"
    assert str(da_e1) == "Aaa"
    assert da_e1.error_descriptors == [the_daed]
    assert da_e1.detailed_error_descriptors == [
        DataAPIDetailedErrorDescriptor(
            error_descriptors=[the_daed],
            command={"cmd": "C1"},
            raw_response={
                "errors": [{"errorCode": "C", "message": "Aaa", "field": "value"}]
            },
        )
    ]


@pytest.mark.describe("test InsertManyException")
def test_insertmanyexception() -> None:
    im_result = InsertManyResult(raw_results=[{"a": 1}], inserted_ids=["a", "b"])
    # mypy thinks im_e1 is a DataAPIException for some reason...
    im_e1: InsertManyException = InsertManyException.from_responses(  # type: ignore[assignment]
        commands=[{"cmd": "C1"}],
        raw_responses=[{"errors": [{"errorCode": "C", "message": "Aaa"}]}],
        partial_result=im_result,
    )
    the_daed = DataAPIErrorDescriptor({"errorCode": "C", "message": "Aaa"})

    assert im_e1.partial_result == im_result
    assert im_e1.text == "Aaa"
    assert im_e1.error_descriptors == [the_daed]
    assert im_e1.detailed_error_descriptors == [
        DataAPIDetailedErrorDescriptor(
            error_descriptors=[the_daed],
            command={"cmd": "C1"},
            raw_response={"errors": [{"errorCode": "C", "message": "Aaa"}]},
        )
    ]


@pytest.mark.describe("test DeleteManyException")
def test_deletemanyexception() -> None:
    dm_result = DeleteResult(deleted_count=123, raw_results=[{"a": 1}])
    # mypy thinks dm_e1 is a DataAPIException for some reason...
    dm_e1: DeleteManyException = DeleteManyException.from_responses(  # type: ignore[assignment]
        commands=[{"cmd": "C1"}],
        raw_responses=[{"errors": [{"errorCode": "C", "message": "Aaa"}]}],
        partial_result=dm_result,
    )
    the_daed = DataAPIErrorDescriptor({"errorCode": "C", "message": "Aaa"})

    assert dm_e1.partial_result == dm_result
    assert dm_e1.text == "Aaa"
    assert dm_e1.error_descriptors == [the_daed]
    assert dm_e1.detailed_error_descriptors == [
        DataAPIDetailedErrorDescriptor(
            error_descriptors=[the_daed],
            command={"cmd": "C1"},
            raw_response={"errors": [{"errorCode": "C", "message": "Aaa"}]},
        )
    ]
